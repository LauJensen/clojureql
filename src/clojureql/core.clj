(ns
  ^{:author "Lau B. Jensen    <lau.jensen@bestinclass.dk>"
    :doc    "ClojureQL is superior SQL integration for Clojure, which allows
             you to access tables and rows as objects that have uniform interfaces
             for queries, inserts and deletions."
    :url    "http://github.com/LauJensen/clojureql"}
  clojureql.core
  (:use
   clojureql.internal
   [clojure.string :only [join] :rename {join join-str}]
   [clojure.contrib sql])
  (refer-clojure :exclude [take sort conj! disj!]
                 :rename {take take-coll}))


(def db
     {:classname   "com.mysql.jdbc.Driver"
      :subprotocol "mysql"
      :user        "cql"
      :password    "cql"
      :subname     "//localhost:3306/cql"})

(defprotocol DBPredicate
  (to-string [expr] "To-Strings an expression to a String.

                   []  = 'this AND that'
                   {}  = 'key=val'
                   #{} = 'this OR that'"))

(extend-protocol DBPredicate
   String
   (to-string [expr] (str "\"" expr "\""))
   Integer
   (to-string [expr] expr)
   Character
   (to-string [expr] (str expr))
   clojure.lang.Keyword
   (to-string [expr] (str (name expr) "="))
   clojure.lang.MapEntry
   (to-string [expr]
            (str (-> expr key name) \= (-> expr val to-string)))
   clojure.lang.PersistentVector
   (to-string [expr]
              (join-str " AND " (map to-string expr)))
   clojure.lang.ArraySeq
   (to-string [expr]
            (join-str " AND " (map to-string expr)))
   clojure.lang.IPersistentSet
   (to-string [expr]
            (str "(" (->> expr (map to-string) (join-str " OR ")) ")"))
   clojure.lang.PersistentArrayMap
   (to-string [expr]
            (->> expr (map to-string) (join-str " AND "))))

(defprotocol Relation
  (select [_    predicate]            "Queries the table using a predicate")
  (conj!  [this records]              "Inserts record(s) into the table")
  (disj!  [this predicate]            "Deletes record(s) from the table")
  (take   [_    n]                    "Queries the table with LIMIT n")
  (sort   [_    col type]             "Sorts the query either :asc or :desc")
  (join   [_    table2 join_on]       "Joins two table")
  )

(defrecord Table [cnx tname tcols]
  clojure.lang.IDeref
  (deref [_]
         (with-connection cnx
           (with-query-results rs
             [(format "SELECT %s FROM %s" (colkeys->string tcols) (name tname))]
             (doall rs))))
  Relation
  (select [_ predicate]
         (with-connection cnx
           (with-query-results rs
             [(format "SELECT %s FROM %s %s" (colkeys->string tcols) (name tname) predicate)]
             (doall rs))))
  (conj! [this records]
         (with-connection cnx
           (if (map? records)
             (insert-records tname records)
             (apply insert-records tname records)))
         this)
  (disj! [this predicate]
         (with-connection cnx
           (delete-rows tname [(to-string predicate)]))
         this)
  (take  [_ n]
         (with-connection cnx
           (with-query-results rs
             [(format "SELECT %s FROM %s LIMIT %d" (colkeys->string tcols) (name tname) n)]
             (doall rs))))
  (sort  [_ col type]
         (with-connection cnx
           (with-query-results rs
             [(format "SELECT %s FROM %s ORDER BY %s %s"
                      (colkeys->string tcols)
                      (name tname)
                      (name col)
                      (if (= :asc type) "ASC" "DESC"))]
             (doall rs))))
  (join  [_ table2 join_on]
         (with-cnx cnx
           (with-results rs
             [(format "SELECT %s,%s FROM %s JOIN %s ON %s"
                      (colkeys->string tname tcols)
                      (colkeys->string (:tname table2) (:tcols table2))
                      (name tname)
                      (-> table2 :tname name)
                      (->> join_on (map name) (join-str \=))
                      )]
             (doall rs))))
  )

(defn table
  " Returns a reference to a table, which will be accessed via the connection-info
    (contrib.sql spec) and query the table-name (keyword) for the colums defined in
    table-colums.

    (table *conn-info* :table1 [:name :id]) "
  [connection-info table-name table-colums]
  (Table. connection-info table-name table-colums))

(defn table? [tinstance]
  (instance? clojureql.core.Table tinstance))

(defn where
  "Returns a query string. If final argument is :invert the boolean value
   of the predicate is inverted.

   (where 'id=%1 OR id < %2' 15 10) => 'WHERE id=15 OR id < 10'

   (where 'id=%1 OR id < %2' 15 10 :invert) => 'WHERE not(id=15 OR id < 10')"
  ([ast]         (str "WHERE " (to-string ast)))
  ([pred & args] (str "WHERE "  (apply sql-clause pred args))))


(defn order-by
  "Returns a query string.

   (order-by :name) => ' ORDER BY name'

   (-> (where 'id=%1' 5) (order-by :name)) => 'WHERE id=5 ORDER BY name"
  ([col]      (str " ORDER BY "  (name col)))
  ([stmt col] (str stmt (order-by col))))

(defn group-by
  "Returns a query string.

   (group-by :name) => ' GROUP BY name'

   (-> (where 'id=%1' 5) (group-by :name)) => 'WHERE id=5 GROUP BY name"
  ([col]      (str " GROUP BY " (name col)))
  ([stmt col] (str stmt (group-by col))))

(defn having
  "Returns a query string.

   (-> (where 'id=%1' 5) (having '%1 < id < %2' 1 2)) =>
    'WHERE id=5 HAVING 1 < id < 2' "
  [stmt pred & args]
  (str stmt " HAVING " (apply sql-clause pred args)))






                                        ; DEMO

(defmacro tst [expr]
  `(do (println "Code:   " (quote ~expr))
       (println "Return: " ~expr)))

(defn test-suite []
  (letfn [(drop-if [t] (try
                        (drop-table t)
                        (catch Exception e nil)))]
    (with-connection db
      (drop-if :users)
      (create-table :users
                    [:id    :integer "PRIMARY KEY" "AUTO_INCREMENT"]
                    [:name  "varchar(255)"]
                    [:title "varchar(255)"])
      (drop-if :salary)
      (create-table :salary
                    [:id    :integer "PRIMARY KEY" "AUTO_INCREMENT"]
                    [:wage  :integer])))
  (let [users  (table db :users [:id :name :title])
        salary (table db :salary [:id :wage])
        roster [{:name "Lau Jensen" :title "Dev"}
                {:name "Christophe" :title "Design Guru"}
                {:name "sthuebner"  :title "Mr. Macros"}
                {:name "Frank"      :title "Engineer"}]
        wages  (map #(hash-map :wage %) [100 200 300 400])]
    (tst @(conj! users roster))
    (tst @(conj! salary wages))
    (tst (join users salary #{:users.id :salary.id}))
    (tst (-> (disj! users #{{:id 3} {:id 4}})
             (sort :id :desc)))
    (tst (take users 1))
    (tst (select users (where "id=%1 OR id=%2" 1 10)))
    (tst (select users (where #{{:id 1} {:id 10}})))))
