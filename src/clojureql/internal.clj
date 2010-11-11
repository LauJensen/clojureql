(ns clojureql.internal
  (:require clojure.contrib.sql.internal)
  (:use [clojure.string :only [join] :rename {join join-str}]
        [clojure.contrib.core :only [-?>]]))

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

(defn aggregate? [c]
  (or (and (string? c) (.contains c "(")) ; Best guess
      (not-any? nil? ((juxt namespace name) c))))

(defn to-tablename
  [c]
  (cond
   (nil? c)         nil
   (keyword? c)     (name c)
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
             (str aggr "(" p col ")"))
           (str p (nskeyword c)))))))

(defn to-fieldlist
  "Converts a column specification to SQL notation field list

   :tble [:sum/k1 :k2 [:avg/k3 :as :c]] => 'sum(tble.k1),tble.k2,avg(tble.k3) AS c'
   :tble :t1 [:avg/a:b] => 'avg(t1.a, t1.b)' "
  ([tcols] (to-fieldlist nil tcols))
  ([tname tcols]
     (let [tname (if-let [tname (to-tablename tname)]
                   (str (-> tname (.split " ") last) \.) "")]
       (letfn [(split-fields [t a] (->> (.split a ":")
                                        (map #(str t %))
                                        (interpose ",")
                                        (apply str)))
               (split-aggregate [item]  (re-find #"(.*)\/(.*)" (nskeyword item)))
               (item->string [i]
                 (cond
                  (string? i) i
                  (vector? i)
                  (if (aggregate? (first i))
                    (let [[col _ alias] (map nskeyword i)
                          [_ fn aggr] (split-aggregate col)]
                      (str fn "(" (split-fields tname aggr) ")" " AS " alias))
                    (let [[col _ alias] (map nskeyword i)]
                      (str tname col " AS " alias)))
                   (and (aggregate? i) (not (string? i)))
                   (let [[_ fn aggr :as x] (split-aggregate i)]
                     (str fn "(" (split-fields tname aggr) ")"))
                   (string? i)
                   i
                   :else (str tname (nskeyword i))))]
         (cond
          (every? string? tcols)
          (join-str "," tcols)
          (= 1 (count tcols))
          (-> tcols first item->string)
          (or (keyword? tcols)
              (and (>= 3 (count tcols))
                   (= :as (nth tcols 1))))
          (item->string tcols)
          :else (->> tcols (map item->string) (join-str \,)))))))

(defn qualify
  "Will fully qualify the names of the child(ren) to the parent.

   :parent :child        => parent.child
   :parent :other.child  => other.child
   :parent :avg/sales    => avg(parent.sales)
   :parent :avg/a:b      => avg(parent.a, parent.b)
   :parent [:a :fn/b [:c :as :d]] => ('parent.a', 'fn(parent.b)'
                                      'parent.c as parent.d') "
  [parent children]
  (let [parent (cond
                (string? parent) ; has this already been treated as an alias?
                (-> parent (.split " ") last)
                (map? parent) ; is an alias
                (-> parent vals first name)
                :else
                parent)]
    (if (nil? children)
      ""
      (letfn [(singular [c]
                (cond
                 (vector? c)
                 (let [[nm _ alias] c]
                   (str (to-name parent nm) " AS " (to-name parent alias)))
                 (string? c)
                 c ; TODO: We might want to check for a period
                 :else
                  (let [childname (name c)]
                    (if (or (qualified? c) (aggregate? c))
                      (if (aggregate? c)
                        (to-fieldlist parent [c])
                        (name c))
                      (str (name parent) \. (name c))))))]
        (if (keyword? children)
          (singular children)
          (map singular children))))))

(defn has-aggregate?
  [tble]
  (some #(or (vector? %) (aggregate? %)) (:tcols tble)))

(defn derrived-fields
  "Computes the resulting fields from its input

   :one [:a :b :c] :two :cnt => 'one.a,one.b,one.c,two.cnt

   Note: Used internally when compiling a join with an aggregate "
  [tname cols table-alias col-alias]
  (str (->> cols (qualify tname) to-fieldlist)
       (when (and table-alias col-alias)
         (str "," (name table-alias) \. (nskeyword col-alias) ))))

(defn find-first-alias
  "Scans a column spec to find the first alias"
  [tcols]
  (-?> (filter #(and (vector? %) (= 3 (count %))) tcols)
       first last name))

(defn with-rename
  "Renames fields that have had their parent aliased.
   (name is horribly misleading, it protects against errors when
    tables have been renamed)

   :one [:one.a :one.b] {:one :two} => 'one AS one(a,b)' "
  [tname tcols renames]
  (let [oname (to-tablename tname)]
    (if (map? renames)
      (format "%s AS %s(%s)" oname oname
              (reduce #(let [[orig new] %2]
                         (.replaceAll %1 (name orig) (name new)))
                      (join-str "," (->> tcols (map name)
                                         (filter #(.contains % oname))
                                         (map #(subs % (inc (.indexOf % "."))))))
                      renames))
      (str oname "(" (to-fieldlist tcols) ")"))))

(defn build-join
  "Generates a JOIN statement from the joins field of a table

   [:t2 (= {:a 5})] => 'JOIN t2 ON (a = 5)'

   [:t2 :id]        => 'JOIN t2 USING(id)'                   "
  [[tname pred]]
  (str "JOIN "
       (if (keyword? pred)
         (format "%s USING(%s) " tname (name pred))
         (format "%s ON %s" tname pred))))

(defn get-foreignfield
  "Extracts the first foreign field in a column spec

   :t1 [:t1.x :t2x]  => :t2.x "
  [tname s]
  (-> (remove #(.contains (-> % name (.split "\\.") first) (name tname)) s)
      first))

(defn non-unique-map
  " Reduces a collection of key/val pairs to a single hashmap.

   [[:a 5] [:b 10]] => {:a 5 :b 10}

   [[:a 5] [:b 10] [:a 15]] => {:a [5 15] :b 10} "
  [ks]
  (reduce (fn [acc [k v]]
            (assoc acc k (if-let [vl (acc k)]
                           (if (not= vl (acc k))
                             (conj [vl] v)
                             vl)
                           v))) {} ks))

(defn sql-clause [pred & args]
  " Allows you to generate an sql clause by identifying params as %1, %2, %n.

    (sql-clause '%1 > %2 < %1' 'one' 'two') => 'one > two < one' "
  (letfn [(rep [s i] (.replaceAll s (str "%" (inc i))
                                  (let [item (nth args i)]
                                    (if (keyword? item)
                                      (name item)
                                      (str item)))))]
    (if (empty? args)
      pred
      (loop [i 0 retr pred]
        (if (= i (-> args count dec))
          (rep retr i)
          (recur (inc i) (rep retr i)))))))

                                        ; SQL Specifics

(def global-connections (atom {}))

(defn open-global [id specs]
  (swap! global-connections assoc
         id (clojure.contrib.sql.internal/get-connection specs)))


(defn close-global
  "Supplied with a keyword identifying a global connection, that connection
  is closed and the reference dropped."
  [conn-name]
  (if-let [conn (conn-name @global-connections)]
    (do
      (.close conn)
      (swap! global-connections dissoc conn-name))
    (throw
     (Exception. (format "No global connection by that name is open (%s)" conn-name)))))

(defn with-cnx*
  "Evaluates func in the context of a new connection to a database then
  closes the connection."
  [con-info func]
  (io!
   (if (keyword? con-info)
     (if-let [con (@global-connections con-info)]
       (binding [clojure.contrib.sql.internal/*db*
                 (assoc clojure.contrib.sql.internal/*db* :connection con
                        :level 0 :rollback (atom false))]
         (func))
       (throw
        (Exception. "No such global connection currently open!")))
     (with-open [con (clojure.contrib.sql.internal/get-connection con-info)]
       (binding [clojure.contrib.sql.internal/*db*
                 (assoc clojure.contrib.sql.internal/*db* :connection con
                        :level 0 :rollback (atom false))]
         (func))))))

(defmacro with-cnx
  [db-spec & body]
  `(with-cnx* ~db-spec (fn [] ~@body)))

(defn result-seq
  "Creates and returns a lazy sequence of structmaps corresponding to
  the rows in the java.sql.ResultSet rs. Accepts duplicate keys"
  [^java.sql.ResultSet rs]
  (let [rsmeta (. rs (getMetaData))
        idxs (range 1 (inc (. rsmeta (getColumnCount))))
        keys (map (comp keyword #(.toLowerCase ^String %))
                  (map (fn [i] (. rsmeta (getColumnLabel i))) idxs))
        row-values (fn [] (map (fn [^Integer i] (. rs (getObject i))) idxs))
        rows (fn thisfn []
               (when (. rs (next))
                 (cons
                  (->> (for [i idxs :let [vals (row-values)]]
                         [(nth keys (dec i)) (nth vals (dec i))])
                       non-unique-map)
                  (lazy-seq (thisfn)))))]
    (rows)))

(defn with-results*
  "Executes a query, then evaluates func passing in a seq of the results as
  an argument. The first argument is a vector containing the (optionally
  parameterized) sql query string followed by values for any parameters."
  [[sql & params :as sql-params] func]
  (when-not (vector? sql-params)
    (throw (Exception. "sql-params must be a vector")))
  (with-open [stmt (.prepareStatement (:connection clojure.contrib.sql.internal/*db*) sql)]
    (doseq [[index value] (map vector (iterate inc 1) params)]
      (.setObject stmt index value))

    (with-open [rset (.executeQuery stmt)]
      (func (result-seq rset)))))

(defmacro with-results
  "Executes a query, then evaluates body with results bound to a seq of the
  results. sql-params is a vector containing a string providing
  the (optionally parameterized) SQL query followed by values for any
  parameters."
  [results sql-params & body]
  `(with-results* ~sql-params (fn [~results] ~@body)))
