(ns clojureql.core
  ^{:author "Lau B. Jensen    <lau.jensen@bestinclass.dk>"
    :doc    "ClojureQL is superior SQL integration for Clojure, which allows
             you to access tables and rows as objects that have uniform interfaces
             for queries, inserts and deletions."
    :url    "http://github.com/LauJensen/clojureql"}
  (:refer-clojure
   :exclude [take sort conj! disj! < <= > >= =]
   :rename {take take-coll})
  (:use
   [clojureql internal predicates]
   [clojure.string :only [join] :rename {join join-str}]
   [clojure.contrib sql]))

                                        ; GLOBALS

(def *debug* true) ; If true: Shows all SQL expressions before executing

(def db
     {:classname   "com.mysql.jdbc.Driver"
      :subprotocol "mysql"
      :user        "cql"
      :password    "cql"
      :subname     "//localhost:3306/cql"})

                                        ; RELATIONAL ALGEBRA

(defprotocol Relation
  (select     [this predicate]            "Queries the table using a predicate")
  (project    [this fields]               "Projects fields onto the query")
  (join       [_    table2 join_on]       "Joins two table")
  (rename     [this newnames]             "Renames colums in a join")

  (conj!      [this records]              "Inserts record(s) into the table")
  (disj!      [this predicate]            "Deletes record(s) from the table")

  (limit      [_    n]                    "Queries the table with LIMIT n")
  (group-by   [this col]                  "Groups the Query by the column")
  (order-by   [this col]                  "Orders the Query by the column")

  (sort       [_    col type]             "Sorts the query either :asc or :desc")
  (options    [this opts]                 "Appends opt(ion)s to the query")

  (compile    [this]                      "Returns an SQL statement")
  )

(defn has-aggregate?
  [tble]
  (loop [[v & vs] (:tcols tble)]
    (when v
      (if (or (vector? v)
              (.contains (name v) ":"))
        true
        (recur vs)))))

(defn derrived-fields [tname cols table-alias col-alias]
  (str (->> cols (qualify tname) colkeys->string)
       ","
       (str table-alias \. col-alias )))

(defn find-first-alias [tble]
  (loop [[x & xs] tble]
    (when x
      (if (and (vector? x) (= 3 (count x)))
        (-> x last name)
        (recur xs)))))

(defrecord RTable [cnx tname tcols restriction renames joins options]
  clojure.lang.IDeref
  (deref [this] (with-cnx cnx
                  (with-results rs
                    [(compile this)]
                    (doall rs))))
  Relation
  (compile  [this]
            (let [sql-string
                  (if (vector? joins) ; Are we joining on a table containing aggregates?
                    (let [[[t2 pred]]  joins
                          t2name       (-> t2 :tname name)
                          colalias     (find-first-alias (:tcols t2))
                          t2alias      (str t2name "_aggregation")]
                      (-> (format "SELECT %s FROM %s LEFT OUTER JOIN (%s) AS %s ON %s %s"
                                  (derrived-fields tname tcols t2alias colalias)
                                  (name tname)
                                  (-> (.group-by t2 (-> t2 :tcols first)) compile)
                                  t2alias
                                  (.replaceAll pred t2name t2alias)
                                  (or options ""))
                          .trim))
                    (-> (format "SELECT %s FROM %s %s %s %s"
                                (->> tcols (qualify tname) colkeys->string)
                                (if renames
                                  (with-rename tname (qualify tname tcols) renames)
                                  (name tname))
                                (if joins (with-joins joins) "")
                                (if restriction (where (join-str " AND " restriction)) "")
                                (or options ""))
                        .trim))]
              (when *debug* (prn sql-string))
              sql-string))
  (select   [this predicate]
            (RTable. cnx tname tcols
                     (conj (or restriction []) predicate)
                     renames joins options))
  (project  [this fields]
            (RTable. cnx tname
                     (apply conj (or tcols []) fields)
                     restriction renames joins options))
  (join     [this table2 join-on]
            (if (has-aggregate? table2)
              (RTable. cnx tname tcols restriction renames
                       (conj (or joins []) [table2 join-on])
                       options)
              (RTable. cnx tname
                       (apply conj (or tcols [])
                              (qualify (:tname table2) (:tcols table2)))
                       restriction renames
                       (assoc (or joins {}) (:tname table2) join-on)
                       options)))
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
  (order-by [this col]     (.options this (str "ORDER BY " (qualify tname col))))
  (sort     [this col dir] (.options this (str "ORDER BY " (qualify tname col) \space
                                               (if (:asc dir) "ASC" "DESC"))))

  )

(defn table
  ([connection-info table-name]
     (table connection-info table-name nil nil))
  ([connection-info table-name colums]
     (table connection-info table-name colums nil))
  ([connection-info table-name colums restrictions]
     (RTable. connection-info table-name colums restrictions nil nil nil)))

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
  (binding [*debug* true]                                      ; Causes all SQL statements to be printed
    (let [users  (table db :users [:id :name :title])
          salary (table db :salary [:id :wage])
          roster [{:name "Lau Jensen" :title "Dev"}
                  {:name "Christophe" :title "Design Guru"}
                  {:name "sthuebner"  :title "Mr. Macros"}
                  {:name "Frank"      :title "Engineer"}]
          wages  (map #(hash-map :wage %) [100 200 300 400])]
      (tst @(conj! users roster))                              ; Add multiple rows
      (tst @(conj! salary wages))                              ; Same
      (tst @(join users salary (= {:users.id :salary.id})))    ; Join two tables explicitly
      (tst @(join users salary :id))                           ; Join two tables with USING
      (tst @(-> users
                (conj! {:name "Jack"})                         ; Add a single row
                (disj! (= {:id 1}))                            ; Remove anothern
                (sort :id :desc)                               ; Prepare to sort
                (project #{:id :title})                        ; Returns colums id and title
                (select (<= {:id 10}))                         ; Where ID is <= 10
                (join salary :id)                              ; Join with table salary
                (limit 10)))                                   ; Limit return to 10 rows
      (tst @(-> (disj! users (either (= {:id 3}) (= {:id 4})))
                (sort :id :desc)))
      (tst @(limit users 1))
      (tst @(-> (table db :salary) (project [:avg:wage])))
      #_(tst (select users (where "id=%1 OR id=%2" 1 10)))
      (tst @(select users (either (= {:id 1}) (>= {:id 10})))))))



