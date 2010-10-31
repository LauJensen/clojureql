(ns clojureql.internal
  (:use [clojure.string :only [join] :rename {join join-str}]))

(def *db* {:connection nil :level 0})

(defn with-cnx*
  "Evaluates func in the context of a new connection to a database then
  closes the connection."
  [db-spec func]
  (with-open [con (clojure.contrib.sql.internal/get-connection db-spec)]
    (binding [*db* (assoc *db*
                     :connection con :level 0 :rollback (atom false))]
      (func))))

(defmacro with-cnx
  [db-spec & body]
  `(with-cnx* ~db-spec (fn [] ~@body)))

(defn colkeys->string
  " [:k1 :k2] becomes \"k1,k2\" "
  [tcols]
  (->> tcols (map name) (join-str \,)))

(defn map->predicate
  " {:x 5 :y 10} becomes \"x=5 AND y=10\"
    {'x 5 'y 10} becomes \"x=5 OR y=10\"
    Strings are automatically quoted. "
  [pred]
  (reduce (fn [acc [k v]]
            (str acc  (if-not (empty? acc)
                        (if (symbol? k) " OR " " AND "))
                 (name k)
                 \=
                 (if (string? v)
                   (format "'%s'" v)
                   v)
                 )) "" pred))

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
    (loop [i 0 retr pred]
      (if (= i (-> args count dec))
        (rep retr i)
        (recur (inc i) (rep retr i))))))

                                        ; SQL Specifics

(defn result-seq
  "Creates and returns a lazy sequence of structmaps corresponding to
  the rows in the java.sql.ResultSet rs"
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
    (throw "sql-params must be a vector"))
  (with-open [stmt (.prepareStatement (:connection *db*) sql)]
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
