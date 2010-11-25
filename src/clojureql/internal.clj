(ns clojureql.internal
  (:require
   [clojure.contrib.sql.internal :as sqlint]
   [clojure.contrib.sql :as csql])
  (:use [clojure.string :only [join] :rename {join join-str}]
        [clojure.contrib.core :only [-?>]]))

(defn clean-sql [coll]
  "For internal use only. Concats a collection of strings interposing spaces
   between the items. Removes any garbage whitespace."
  (loop [s (-> (join-str " " coll) .trim)]
    (if-not (.contains s "  ")
      s
      (recur (.replace s "  " " ")))))

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

(defn aggregate? [c]
  (or (and (string? c) (.contains c "(")) ; Best guess
      (not-any? nil? ((juxt namespace name) (keyword c)))))

(defn split-fields [t a]
  (->> (.split a ":")
       (map #(if (= % "*") "*" (str t %)))
       (interpose ",")
       (apply str)))

(defn to-orderlist
  "Converts a list like [:id#asc :name#desc] to \"id asc, name desc\"

   Also takes a single keyword argument"
  [tname fields]
  (->> (if (coll? fields) fields [fields])
       (map #(if (.contains (name %) "#")
               (->> (.split (name %) "#")
                    ((juxt first last))
                    (map (fn [f] (if (or (= f "asc") (= f "desc"))
                                   f
                                   (str (name tname) \. f ))))
                    (interpose " ")
                    (apply str))
               (str (name %) " asc")))
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
           (str p (nskeyword c)))))))

(defn to-fieldlist
  "Converts a column specification to SQL notation field list

   :tble [:sum/k1 :k2 [:avg/k3 :as :c]] => 'sum(tble.k1),tble.k2,avg(tble.k3) AS c'
   :tble :t1 [:avg/a:b] => 'avg(t1.a, t1.b)' "
  ([tcols] (to-fieldlist nil tcols))
  ([tname tcols]
     (let [tname (if-let [tname (to-tablename tname)]
                   (str (-> tname (.split " ") last) \.) "")]
       (letfn [(split-aggregate [item]  (re-find #"(.*)\/(.*)" (nskeyword item)))
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
                   :else
                   (if (.contains (nskeyword i) ".")
                     (nskeyword i)
                     (str tname (nskeyword i)))))]
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

(defn derived-fields
  "Computes the resulting fields from its input

   :one [:a :b :c] :two :cnt => 'one.a,one.b,one.c,two.cnt

   Note: Used internally when compiling a join with an aggregate "
  [tname cols table-alias col-alias]
  (str (->> cols (qualify tname) to-fieldlist)
       (when (and table-alias col-alias)
         (str "," (name table-alias) \. (nskeyword col-alias) ))))

(defn find-first-alias
  "Scans a column spec to find the first alias, if none is found the
   first column is used instead"
  [tcols]
  (let [alias (-?> (filter #(and (vector? %) (= 3 (count %))) tcols)
                   first last name)]
    (if (seq alias)
      alias
      (-> tcols first nskeyword))))

(defn with-rename
  "Renames fields that have had their parent aliased.
   (name is horribly misleading, it protects against errors when
    tables have been renamed)

   :one [:one.a :one.b] {:one :two} => 'one AS one(a,b)' "
  [tname tcols renames]
  (let [oname (to-tablename tname)
        unqualify (fn [s] (let [s (name s)] (subs s (inc (.indexOf s ".")))))]
    (if (map? renames)
      (format "%s AS %s(%s)" oname oname
              (reduce #(let [[orig new] %2]
                         (.replaceAll %1 (unqualify orig) (unqualify new)))
                      (join-str "," (->> tcols (map name)
                                         (filter #(.contains % oname))
                                         (map #(subs % (inc (.indexOf % "."))))))
                      renames))
      (str oname "(" (to-fieldlist tcols) ")"))))

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
  (with-open [stmt (.prepareStatement (:connection sqlint/*db*) sql)]
    (doseq [[idx v] (map vector (iterate inc 1) params)]
         (.setObject stmt idx v))
    (if-let [fetch-size (-> sqlint/*db* :opts :fetch-size)]
      (do
        (.setFetchSize stmt fetch-size)
        (csql/transaction
         (with-open [rset (.executeQuery stmt)]
           (func (result-seq rset)))))
      (with-open [rset (.executeQuery stmt)]
        (func (result-seq rset))))))
