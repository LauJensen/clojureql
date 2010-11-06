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

                                        ; GLOBALS

(def *debug* true) ; If true: Shows all SQL expressions before executing

(def db
     {:classname   "com.mysql.jdbc.Driver"
      :subprotocol "mysql"
      :user        "cql"
      :password    "cql"
      :subname     "//localhost:3306/cql"})

                                        ; PREDICATE COMPILER

(defn compile-expr
  [expr]
  (case (first expr)
        :or  (str "(" (join-str " OR "  (map compile-expr (rest expr))) ")")
        :and (str "(" (join-str " AND " (map compile-expr (rest expr))) ")")
        :eq  (str (-> expr last keys first to-name) " = " (-> expr last vals first))
        :gt  (str (-> expr last keys first to-name) " > " (-> expr last vals first))
        :lt  (str (-> expr last keys first to-name) " < " (-> expr last vals first))
        :gt= (str (-> expr last keys first to-name) " >= " (-> expr last vals first))
        :lt= (str (-> expr last keys first to-name) " <= " (-> expr last vals first))
        :!=  (str (-> expr last keys first to-name) " != " (-> expr last vals first))
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

(defn !=
  " Same as not= "
  [& args]
  (if (map? (first args))
    (compile-expr (apply vector :!= args))
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


                                        ; RELATIONAL ALGEBRA

(defprotocol Relation
  (select     [this predicate]            "Queries the table using a predicate")
  (project    [this fields]               "Projects fields onto the query")
  (join       [_    table2 join_on]       "Joins two table")
  (rename     [this newnames]             "Renames colums")

  (conj!      [this records]              "Inserts record(s) into the table")
  (disj!      [this predicate]            "Deletes record(s) from the table")

  (limit      [_    n]                    "Queries the table with LIMIT n")
  (group-by   [this col]                  "Groups the Query by the column")
  (order-by   [this col]                  "Orders the Query by the column")

  (sort       [_    col type]             "Sorts the query either :asc or :desc")
  (options    [this opts]                 "Appends opt(ion)s to the query")
  )


(defrecord RTable [cnx tname tcols restriction renames joins options]
  clojure.lang.IDeref
  (deref [_]
         (let [sql-string (format "SELECT %s FROM %s %s %s %s"
                                  (colkeys->string tcols)
                                  (if renames
                                    (with-rename tname renames)
                                    (name tname))
                                  (if joins
                                    (with-joins joins)
                                    "")
                                  (if restriction
                                    (where (join-str " AND " restriction))
                                    "")
                                  (or options ""))]
           (when *debug* (prn sql-string))
           (with-cnx cnx
             (with-results rs
               [sql-string]
               (doall rs)))))
  Relation
  (select   [this predicate]
            (RTable. cnx tname tcols
                     (conj (or restriction []) predicate)
                     renames joins options))
  (project  [this fields]
            (RTable. cnx tname
                     (apply conj (or tcols []) (qualify tname fields)) ; TODO: If this is an aggregate, dont qualify
                     restriction renames joins options))
  (join     [this table2 join-on]
            (RTable. cnx tname
                     (apply conj (or tcols [])
                            (qualify (:tname table2) (:tcols table2)))
                     restriction renames
                     (assoc (or joins {}) (:tname table2) join-on)
                     options))
  (rename   [this newnames]
            (RTable. cnx tname tcols restriction
                     (merge (or renames {}) newnames)
                     joins options))

  (conj!   [this records]
           (with-connection cnx
             (if (map? records)
               (insert-records tname records)
               (apply insert-records tname records)))
           this)
  (disj!   [this predicate]
           (with-connection cnx
             (delete-rows tname [(compile-expr predicate)]))
           this)

  (options  [this opts]
            (RTable. cnx tname tcols restriction renames joins
                     (str options \space opts)))
  (limit    [this n]       (.options this (str "LIMIT " n)))
  (group-by [this col]     (.options this (str "GROUP BY " (name col))))
  (order-by [this col]     (.options this (str "ORDER BY " (name col))))
  (sort     [this col dir] (.options this (str "ORDER BY " (name col) \space
                                               (if (:asc dir) "ASC" "DESC"))))

  )

(defn table
  ([connection-info table-name]
     (table connection-info table-name nil nil))
  ([connection-info table-name colums]
     (table connection-info table-name colums nil))
  ([connection-info table-name colums restrictions]
     (RTable. connection-info table-name (qualify table-name colums) restrictions nil nil nil)))

(defn table? [tinstance]
  (instance? clojureql.core.RTable tinstance))

                                        ; DEMO

(defmacro tst [expr]
  `(do (print "Code:   " (quote ~expr) "\nSQL:     ")
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
  (binding [*debug* true]
    (let [users  (table db :users [:id :name :title])
          salary (table db :salary [:id :wage])
          roster [{:name "Lau Jensen" :title "Dev"}
                  {:name "Christophe" :title "Design Guru"}
                  {:name "sthuebner"  :title "Mr. Macros"}
                  {:name "Frank"      :title "Engineer"}]
          wages  (map #(hash-map :wage %) [100 200 300 400])]
      (tst @(conj! users roster))
      (tst @(conj! salary wages))
      (tst @(join users salary #{:users.id :salary.id}))
      (tst @(join users salary :id))
      (tst @(-> users
                (conj! {:name "Jack"})
                (disj! (= {:id 1}))
                (sort :id :desc)
                (project #{:id :title})
                (select (<= {:id 10}))
                (limit 10)))
      (tst @(-> (disj! users (either (= {:id 3}) (= {:id 4})))
                (sort :id :desc)))
      (tst @(limit users 1))
      #_(tst (select users (where "id=%1 OR id=%2" 1 10)))
      (tst @(select users (either (= {:id 1}) (>= {:id 10})))))))