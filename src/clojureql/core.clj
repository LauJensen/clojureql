(ns clojureql.core
  ^{:author "Lau B. Jensen    <lau.jensen@bestinclass.dk>"
    :doc    "ClojureQL is superior SQL integration for Clojure, which allows
             you to access tables and rows as objects that have uniform interfaces
             for queries, inserts and deletions."
    :url    "http://github.com/LauJensen/clojureql"}
  (:use
   clojureql.internal
   [clojure.string :only [join] :rename {join join-str}]
   [clojure.contrib sql])
  (refer-clojure :exclude [take sort conj! disj! < <= > >= =]
                 :rename {take take-coll}))


(def db
     {:classname   "com.mysql.jdbc.Driver"
      :subprotocol "mysql"
      :user        "cql"
      :password    "cql"
      :subname     "//localhost:3306/cql"})

(defn compile-expr
  [expr]
  (case (first expr)
        :or  (str "(" (join-str " OR "  (map compile-expr (rest expr))) ")")
        :and (str "(" (join-str " AND " (map compile-expr (rest expr))) ")")
        :eq  (str (-> expr last keys first to-name) " = " (-> expr last vals first))
        :gt  (str (-> expr last keys first to-name) " > " (-> expr last vals first))
        :lt  (str (-> expr last keys first to-name) " < " (-> expr last vals first))
        :gt= (str (-> expr last keys first to-name) ">=" (-> expr last vals first))
        :lt= (str (-> expr last keys first to-name) "<=" (-> expr last vals first))
        (str expr)))

(defn either
  " CQL version of OR.

    (either (= {:a 5)) (>= {:a 10})) means either a = 5 or a >= 10 "
  [& conds]
  (compile-expr (apply vector :or conds)))

(defn both
  " CQL version of AND.

    (both (= {:a 5}) (>= {:b 10})) means a=5 AND b >= 10 "
  [& conds]
  (compile-expr (apply vector :and conds)))

(defn =
  " Alpha - Subject to sanity.

    The idea is, that if this doesn't get passed a map, it assumes
    you want Clojures regular = operator "
  [& args]
  (if (map? (first args))
    (compile-expr (apply vector :eq args))
    (apply clojure.core/= args)))

(defn >
  " Alpha - Subject to sanity.

    The idea is, that if this doesn't get passed a map, it assumes
    you want Clojures regular > operator "
  [& args]
  (if (map? (first args))
    (compile-expr (apply vector :gt args))
    (apply clojure.core/> args)))

(defn <
  " Alpha - Subject to sanity.

    The idea is, that if this doesn't get passed a map, it assumes
    you want Clojures regular < operator "
  [& args]
  (if (map? (first args))
    (compile-expr (apply vector :lt args))
    (apply clojure.core/< args)))

(defn <=
  " Alpha - Subject to sanity.

    The idea is, that if this doesn't get passed a map, it assumes
    you want Clojures regular <= operator "
  [& args]
  (if (map? (first args))
    (compile-expr (apply vector :lt= args))
    (apply clojure.core/<= args)))

(defn >=
  " Alpha - Subject to sanity.

    The idea is, that if this doesn't get passed a map, it assumes
    you want Clojures regular >= operator "
  [& args]
  (if (map? (first args))
    (compile-expr (apply vector :gt= args))
    (apply clojure.core/>= args)))

(defn where
  "Returns a query string. Can take a raw string with params as %1 %2 %n
   or an AST which compiles using compile-expr.

   (where 'id=%1 OR id < %2' 15 10) => 'WHERE id=15 OR id < 10'

   (where (either (= {:id 5}) (>= {:id 10})))
      'WHERE (id=5 OR id>=10)' "
  ([ast]         (str "WHERE " (compile-expr ast)))
  ([pred & args] (str "WHERE "  (apply sql-clause pred args))))

(defn where-not
  "The inverse of the where fn"
  ([ast]         (str "WHERE not(" (compile-expr ast) ")"))
  ([pred & args] (str "WHERE not("  (apply sql-clause pred args) ")")))

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
    'WHERE id=5 HAVING 1 < id < 2'

   (-> (where (= {:id 5})) (having (either (> {:id 5}) (<= {:id 2})))) =>
    'WHERE id=5 HAVING (id > 5 OR id <= 2)'"
  ([stmt ast]
     (str stmt " HAVING " (compile-expr ast)))
  ([stmt pred & args]
     (str stmt " HAVING " (apply sql-clause pred args))))


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
           (delete-rows tname [(compile-expr predicate)]))
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







                                        ; DEMO

(defmacro tst [expr]
  `(do (println "Code:   " (quote ~expr))
       (println "Return: " ~expr "\n")))

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
    (tst (-> (disj! users (either (= {:id 3}) (= {:id 4})))
             (sort :id :desc)))
    (tst (take users 1))
    (tst (select users (where "id=%1 OR id=%2" 1 10)))
    (tst (select users (where (either (= {:id 1}) (>= {:id 10})))))))
