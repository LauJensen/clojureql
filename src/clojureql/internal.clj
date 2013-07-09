(ns clojureql.internal
  (:import [java.sql Statement])
  (:require
    clojure.set
    [clojure.java.jdbc :as jdbc])
  (:use [clojure.string :only [join split upper-case replace] :rename {join join-str replace replace-str}]
        [clojure.core.incubator :only [-?> -?>>]]))

(defn upper-name [kw]
  (-> kw name .toUpperCase))

(defn apply-aliases
  "Takes a statement and a hashmap of aliases and returns
   a statement with all aliases applied"
  [stmt aliases]
  [(reduce (fn [acc [old new]]
            (.replaceAll acc old (-> (.split new "\\.") first)))
          stmt aliases)])

(defn clean-sql [coll]
  "For internal use only. Concats a collection of strings interposing spaces
   between the items. Removes any garbage whitespace."
  (loop [s (-> (join-str " " coll) .trim)]
    (if-not (.contains s "  ")
      s
      (recur (-> (.replace s "  " " ")
                 (.replaceAll "\\( " "\\(")
                 (.replaceAll " \\)" "\\)"))))))

(defn assemble-sql [s & args]
  "For internal use only. Works like format but cleans up afterwards"
  (loop [s (-> (apply format s args) .trim)]
    (if-not (.contains s "  ")
      s
      (recur (.replace s "  " " ")))))

(defn nskeyword
  "Converts a namespace qualified keyword to a string"
  [k]
  (if (string? k)
    k
    (let [[kns nm] ((juxt namespace name) k)]
      (if kns
        (apply str (interpose "/" [kns nm]))
        nm))))

(defn qualified? [c]
  (.contains (nskeyword c) "."))

(defn add-tname
  [tname colname]
  (let [tname   (if (map? tname)
                  (-> tname vals first)
                  tname)
        colname (if (vector? colname)
                  (str (-> colname first nskeyword) " AS " (-> colname last nskeyword))
                  colname)]
    (if (qualified? colname)
      (nskeyword colname)
      (-> (str (if (or (keyword? tname)
                       (not (empty? tname)))
                 (str (nskeyword tname) \.) "")
               (nskeyword colname))
          (.replaceAll "\\.\\." "\\.")))))

(defn aggregate? [c]
  (let [c (if (vector? c) (first c) c)]
    (or (and (string? c) (.contains c "(")) ; Best guess
        (and (keyword? c)
             (not-any? nil? ((juxt namespace name) (keyword c)))))))

(defn split-fields [t a]
  (->> (.split a ":")
       (map #(if (= % "*")
               "*"
               (if (.contains % ".")
                              (str %)
                              (str t %))))
       (interpose ",")
       (apply str)))

(defn rename-subselects [tname tcols]
  (let [tcols (mapcat #(.split % ",") tcols)
        tname (if (map? tname)
                (str (nskeyword (-> tname vals last)) ".")
                (str (nskeyword tname) "_subselect."))]
    (map #(let [col (nskeyword %)]
            (if (.contains col ".")
              (let [[name col] (.split col "\\.")]
                (str tname col))
              (str tname col)))
         (remove aggregate? tcols))))

;TODO: This function is too complex. First step to simplyfing is getting rid
;      of the support for a non-collection argument.
(defn to-orderlist
  "Converts a list like [:id#asc :name#desc] to \"id asc, name desc\"

   Also takes a single keyword argument. Does not qualify fields found
   in aggregates. aggregates can also be a keyword in which case all
   fields are left unqualified."
  [tname aggregates fields]
  (->> (if (coll? fields) fields [fields])
       (map #(if (.contains (name %) "#")
               (->> (.split (name %) "#")
                    ((juxt first last))
                    (map (fn [f] (if (or (= f "asc") (= f "desc"))
                                   (upper-case f)
                                   (if (or (keyword? aggregates)
                                           (some (fn [i] (= (nskeyword i) (nskeyword f))) aggregates))
                                     f
                                     (add-tname tname f)))))
                    (interpose " ")
                    (apply str))
               (if (or (keyword? aggregates)
                       (some (fn [i] (= (nskeyword i) (nskeyword %))) aggregates))
                 (str (nskeyword %) " ASC")
                 (str (if (keyword? %) (add-tname tname %) %) " ASC"))))
       (interpose ",")
       (apply str)))

(declare to-name)

(defn to-tablename
  [c]
  (cond
   (nil? c)         nil
   (keyword? c)     (to-name c)
   (string? c)      c
   :else            ; Map denotes rename
   (let [[orig new] (map name (first c))]
     (str orig \space new))))

(defn to-name
  " Converts a keyword to a string, checking for aggregates

   (to-name :avg/y) => 'avg(y)'
   (to-name :y) => 'y'
   (to-name :parent :fn/field => 'fn(master.field)' "
  ([c]   (to-name :none c))
  ([p c]
     (let [p  (if (= :none p) "" (str (to-tablename p) \.))]
       (if (string? c)
         (str "'" c "'")
         (if (aggregate? c)
           (let [[aggr col] (-> (nskeyword c) (.split "/"))]
             (str aggr "(" (split-fields p col) ")"))
           (add-tname p c))))))

(defn to-tablealias
  "Returns the alias of a :tname. If :tname is not aliased it
  returns the same as to-tablename."
  [c]
  (if (map? c)
    (name (first (vals c)))
    (to-tablename c)))

(defn emit-case
  [{:keys [alias clauses returns else]}]
  (format "CASE %s %s END AS %s"
          (reduce #(str %1 (format " WHEN %s THEN ?" %2))
                  "" clauses)
          (if else " ELSE ?" "")
          (nskeyword alias)))

(defn to-fieldlist
  "Converts a column specification to SQL notation field list

   :tble [:sum/k1 :k2 [:avg/k3 :as :c]] => 'sum(tble.k1),tble.k2,avg(tble.k3) AS c'
   :tble :t1 [:avg/a:b] => 'avg(t1.a, t1.b)' "
  ([tcols] (to-fieldlist nil tcols))
  ([tname tcols]
     (let [tname (if-let [tname (to-tablename tname)]
                   (str (-> tname (.split " ") last) \.) "")]
       (letfn [(split-aggregate [item]
                 (let [item (nskeyword item)]
                   (if (.contains item "/")
                     (re-find #"(.*)\/(.*)" (nskeyword item))
                     (re-find #"(.*)\((.*)\)" item))))
               (item->string [i]
                 (cond
                  (string? i) i       ;User passed a string
                  (map? i)            ;Used case
                  (emit-case i)
                  (vector? i)         ;Either aliased or aggregated
                  (if (aggregate? (first i))
                    (let [[col _ alias] (map nskeyword i)
                          [_ fn aggr] (split-aggregate col)]
                      (if (string? (first i))
                        (str _ " AS " alias)
                        (str fn "(" (split-fields tname aggr) ")" " AS " alias)))
                    (let [[col _ alias] (map nskeyword i)]
                      (str (add-tname tname col) " AS " alias)))
                   (and (aggregate? i) (not (string? i)))
                   (let [[_ fn aggr :as x] (split-aggregate i)]
                     (str fn "(" (split-fields tname aggr) ")"))
                   (string? i)
                   i
                   :else
                   (add-tname tname i)))]
         (cond
          (not (coll? tcols))
          tcols
          (every? string? tcols)
          (join-str "," tcols)
          (= 1 (count tcols))
          (-> tcols first item->string)
          (or (keyword? tcols)
              (and (>= 3 (count tcols))
                   (= :as (nth tcols 1))))
          (item->string tcols)
          :else (->> tcols (map item->string) (join-str \,)))))))

(defn has-aggregate?
  [tble]
  (some aggregate? (:tcols tble)))

(defn find-aliases
  "Scans a column spec to find the first alias, if none is found the
   first column is used instead"
  [tcols]
  (let [alias (-?>> (filter #(and (vector? %) (= 3 (count %))) tcols)
                    (map (comp name last)))]
    (if (seq alias)
      alias
      [(-> tcols first nskeyword)])))

(defn find-aggregates
  "Goes through tcols looking for vectors with 3 items and returns
   the alias for those found."
  [tcols]
  ; TODO: Dangerous, vector? should be table? and arguments reversed
  (-?>> (if (vector? tcols) tcols (:tcols tcols))
        (filter #(and (vector? %) (= 3 (count %))))
        (map (comp name last))))

(defn requires-subselect?
  [table]
  (if (keyword? table)
    false
    (or (has-aggregate? table)
        (number? (:limit table)) ; TODO: Check offset as well?
        (seq (:restriction table))))) ; TODO: Sorting as well?

(defn extract-aliases
  " Internal: Looks through the tables in 'joins' and finds tables
              which requires subselects. It returns a vector of the
              original name and the new name for each table "
  [joins]
  (for [[tbl-or-kwd pred] (map :data joins)
        :when (requires-subselect? tbl-or-kwd)
        :let [{:keys [tname tcols]} tbl-or-kwd
              aliases (find-aliases tcols)]]
    (into [(to-tablename tname)]
          (map #(if (map? tname)
                  (str (-> tname vals last nskeyword) \. %)
                  (str (name tname) "_subselect." %)) aliases))))

(defn with-rename
  "Renames fields that have had their parent aliased.
   (name is horribly misleading, it protects against errors when
    tables have been renamed)

   :one [:one.a :one.b] {:one :two} => 'one AS one(a,b)' "
  [tname tcols renames]
  (let [oname (to-tablename tname)
        alias (to-tablealias tname)
        unqualify (fn [s] (let [s (nskeyword s)]
                            (subs s (inc (.indexOf s ".")))))]
    (if (map? renames)
      (format "%s AS %s(%s)" oname alias
                    (reduce #(let [[orig new] %2]
                         (.replaceAll %1 (unqualify orig) (unqualify new)))
                      (join-str "," (->> (map nskeyword tcols)
                                         (filter #(.contains % alias))
                                         (map #(subs % (inc (.indexOf % "."))))))
                      renames))
      (str oname "(" (to-fieldlist tcols) ")"))))

(defn get-foreignfield
  "Extracts the first foreign field in a column spec

   :t1 [:t1.x :t2x]  => :t2.x "
  [tname s]
  (-> (remove #(.contains (-> % name (.split "\\.") first) (name tname)) s)
      first))

(defn sql-clause [pred & args]
  " Allows you to generate an sql clause by identifying params as %1, %2, %n.

    (sql-clause '%1 > %2 < %1' 'one' 'two') => 'one > two < one' "
  (letfn [(rep [s i] (.replaceAll s (str "%" (inc i))
                                  (let [item (nth args i)]
                                    (cond
                                     (keyword? item) (name item)
                                     (string? item) (str "'" item "'")
                                     :else
                                     (str item)))))]
    (if (empty? args)
      pred
      (loop [i 0 retr pred]
        (if (= i (-> args count dec))
          (rep retr i)
          (recur (inc i) (rep retr i)))))))

                                        ; SQL Specifics

(defn result-seq
  "Creates and returns a lazy sequence of structmaps corresponding to
  the rows in the java.sql.ResultSet rs. Accepts duplicate keys"
  [^java.sql.ResultSet rs]
  (let [rsmeta (.getMetaData rs)
        idxs (range (.getColumnCount rsmeta))
        keys (map (comp keyword
                        #(.toLowerCase ^String %)
                        #(.getColumnLabel rsmeta %)
                        inc)
                  idxs)
        assoc-value (fn [mapping i]
                      (let [key (nth keys i)
                            val (.getObject rs ^Integer (inc i))]
                                        ; due to http://dev.clojure.org/jira/browse/CLJ-700
                                        ; we can't use contains? so use ::missing as a marker
                        (let [old (mapping key)]
                          (when-not (or (identical? ::missing old)
                                        (= val old))
                            (throw (Exception.
                                    (format "ResultSet has same label %s for different values, please rename one" key)))))
                        (assoc! mapping key val)))
        empty (apply hash-map
                     (interleave (set keys)
                                 (repeat ::missing)))
        rows (fn rows []
               (lazy-seq
                (when (.next rs)
                  (cons
                   (persistent!
                    (reduce assoc-value
                            (transient empty)
                            idxs))
                   (rows)))))]
    (rows)))


(defn with-results*
  "Executes a query, then evaluates func passing in a seq of the results as
  an argument. The first argument is a vector containing the (optionally
  parameterized) sql query string followed by values for any parameters."
  [[sql & params :as sql-params] func]
  (when-not (vector? sql-params)
    (throw (Exception. "sql-params must be a vector")))
  (with-open [stmt (jdbc/prepare-statement (jdbc/find-connection) sql)]
    (doseq [[idx v] (map vector (iterate inc 1) params)]
      (.setObject stmt idx v))
    (if-let [fetch-size (-> @#'jdbc/*db* :opts :fetch-size)]
      (do
        (.setFetchSize stmt fetch-size)
        (jdbc/transaction
         (with-open [rset (.executeQuery stmt)]
           (func (result-seq rset)))))
      (with-open [rset (.executeQuery stmt)]
        (func (result-seq rset))))))

(defn supports-generated-keys? [conn]
  "Checks if the JDBC Driver supports generated keys"
  (try (.supportsGetGeneratedKeys (.getMetaData conn))
       (catch AbstractMethodError _ false)))

(defn prepare-statement [conn sql]
  "When supported by the JDBC Driver, creates a new prepared statement which will return the generated keys - else returns a 'normal' prepared statement"
  (if (supports-generated-keys? conn)
    (jdbc/prepare-statement conn sql :return-keys true)
    (jdbc/prepare-statement conn sql)))

(defn generated-keys [stmt]
  "When supported by the JDBC driver, returns the generated keys of the latest executed statement"
  (when (supports-generated-keys? (.getConnection stmt))
    (let [ks (.getGeneratedKeys stmt)]
      {:last-index
       (and
        (.next ks)
        (.getObject ks 1))})))

(defn exec-prepared
  "Executes an (optionally parameterized) SQL prepared statement on the
  open database connection. Each param-group is a seq of values for all of
  the parameters."
  ([sql param-group]
     (with-open [stmt (prepare-statement (jdbc/find-connection) sql)]
       (doseq [[idx v] (map vector (iterate inc 1) param-group)]
         (.setObject stmt idx v))
       (jdbc/transaction
        (let [retr (.execute stmt)]
          (with-meta [(.getUpdateCount stmt)]
            (generated-keys stmt))))))
  ([sql param-group & param-groups]
     (with-open [stmt (jdbc/prepare-statement (jdbc/find-connection) sql)]
       (doseq [param-group (cons param-group param-groups)]
         (doseq [[idx v] (map vector (iterate inc 1) param-group)]
           (.setObject stmt idx v))
         (.addBatch stmt))
       (jdbc/transaction
        (seq (.executeBatch stmt))))))

(defn conj-rows
  "Inserts rows into a table with values for specified columns only.
  column-names is a vector of strings or keywords identifying columns. Each
  value-group is a vector containing a values for each column in
  order. When inserting complete rows (all columns), consider using
  insert-rows instead."
  [table column-names & value-groups]
  (let [column-strs (map to-tablename column-names)
        n (count (first value-groups))
        template (apply str (interpose "," (replicate n "?")))
        columns (if (seq column-names)
                  (format "(%s)" (apply str (interpose "," column-strs)))
                  "")]
    (apply exec-prepared
           (format "INSERT INTO %s %s VALUES (%s)"
                   (to-tablename table) columns template)
           value-groups)))

(defn update-vals
  "Updates values on selected rows in a table. where-params is a vector
  containing a string providing the (optionally parameterized) selection
  criteria followed by values for any parameters. record is a map from
  strings or keywords (identifying columns) to updated values."
  [table where-params record]
  (let [[where & params] where-params
        column-strs (map to-tablename (keys record))
        columns (apply str (concat (interpose "=?, " column-strs) "=?"))]
    (exec-prepared
     (format "UPDATE %s SET %s WHERE %s"
             (to-tablename table) columns where)
     (concat (vals record) params))))

(defn update-or-insert-vals
  "Updates values on selected rows in a table, or inserts a new row when no
  existing row matches the selection criteria. where-params is a vector
  containing a string providing the (optionally parameterized) selection
  criteria followed by values for any parameters. record is a map from
  strings or keywords (identifying columns) to updated values."
  [table where-params record]
  (jdbc/transaction
   (let [result (update-vals table where-params record)]
     (if (zero? (first result))
       (conj-rows table (keys record) (vals record))
       result))))

(defn table-alias
  "given an RTable tname, which is either
    - a simple table-name string
    - a {:table-name :alias} map
   return
     in the case of the simple string, the string itself
     in the case of the alias map, the value of the explicit alias"
  [tname]
  (if (map? tname)
    (str (nskeyword (-> tname vals last)))
    (nskeyword tname)))

(defn subselect-table-alias
  "given an RTable tname, which is either
    - a simple table-name string
    - a {:table-name :alias} map
   return
     in the case of the simple string, the string with _subselect appended
     in the case of the alias map, the value of the explicit alias"
  [tname]
  (if (map? tname)
    (str (nskeyword (-> tname vals last)))
    (str (nskeyword tname) "_subselect")))

(defn join-table-alias
  "given tna from the first position of the :data value of a join definition,
   which is one of
     - an RTable (for subselects)
     - a simple table-name
     - a space separated \"table-name alias\" pair
   return
     - if the object was an RTable, return the subselect table
       alias, which is either an explicit alias, or
        (str (nskeyword (:tname tna)) \"_subselect\")
     - if the object was a simple table-name, return it
     - if the object was an aliased table-name, return
       just the alias"
  [tna]
  (if (map? tna)
    (subselect-table-alias (:tname tna))
    (let [[table-name alias] (split tna #"\s+")]
      (or alias table-name))))

(defn joins-by-table-alias
  "given a list of joins return a map of joins
   keyed by the join table-alias or,
   in the case of subselects, the subselect table alias"
  [joins]
  (reduce #(let [{[table-name-alias join-on] :data :as join} %2
                 k (join-table-alias table-name-alias)]
             (assoc %1 k join))
          {}
          joins))

(defn join-column-names
  "given table-name-alias from the first position of the :data value of a join def,
   if table-name-alias is an RTable, representing a subselect, then return the renamed
   join cols according the the subselect alias. if table-name-alias is not an
   RTable then return cols unmodified"
  [table-name-alias cols]
  (if (map? table-name-alias)
    (let [otn (table-alias (:tname table-name-alias))
          sstn (subselect-table-alias (:tname table-name-alias))
          otn-patt (re-pattern (str otn "\\..*"))
          col-strs (map nskeyword cols)
          subselect-col-strs (filter #(re-matches otn-patt %) col-strs)
          other-col-strs (filter #(not (re-matches otn-patt %)) col-strs)
          renamed-subselect-col-strs (map #(replace-str % (re-pattern (str otn "\\.")) (str sstn "."))
                                          subselect-col-strs)]
      (concat renamed-subselect-col-strs other-col-strs))
    cols))

(defn to-tbl-name
  "given a join definition, return a table alias
   that the join depends on"
  [{[table-name-alias join-on] :data :as join}]
  (->> join-on :cols
       (join-column-names table-name-alias)
       (map #(-> % name (.replaceAll "\\..*" "")))
       (filter #(not= % (join-table-alias table-name-alias)))
       first))

(defn to-graph-el
  "given a join definition return an edge in the table
   alias dependency graph"
  [m {[table-name-alias join-on] :data :as join}]
  (let [required-table (to-tbl-name join)]
    (assoc m (join-table-alias table-name-alias) required-table)))

(defn add-deps
  "recursively add dependencies of tbl into a list"
  [map-of-joins edges tbl]
  (into [(map-of-joins tbl)] (map #(add-deps map-of-joins edges %)
                                  (filter #(= tbl (edges %))
                                          (keys edges)))))

(defn flatten-deps
  [map-of-joins edges set-of-root-nodes]
  (filter #(not (nil? %)) (flatten (map #(add-deps map-of-joins edges %) set-of-root-nodes))))

(defn sort-joins
  "sort a list of joins into dependency order"
  [joins]
  (let [map-of-joins (joins-by-table-alias joins)
        edges (reduce to-graph-el {} joins)
        set-of-root-nodes (clojure.set/difference (into #{} (vals edges)) (into #{} (keys edges)))]
    (flatten-deps map-of-joins edges set-of-root-nodes)))
