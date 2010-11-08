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
   [clojure.contrib sql [core :only [-?>]]]))

                                        ; GLOBALS

(def *debug* false) ; If true: Shows all SQL expressions before executing

                                        ; RELATIONAL ALGEBRA

(defprotocol Relation
  (select     [this predicate]            "Queries the table using a predicate")
  (project    [this fields]               "Projects fields onto the query")
  (join       [this table2 join_on]       "Joins two table")
  (rename     [this newnames]             "Renames colums in a join")

  (conj!      [this records]              "Inserts record(s) into the table")
  (disj!      [this predicate]            "Deletes record(s) from the table")

  (limit      [this n]                    "Queries the table with LIMIT n")
  (group-by   [this col]                  "Groups the Query by the column")
  (order-by   [this col]                  "Orders the Query by the column")

  (sort       [this col type]             "Sorts the query either :asc or :desc")
  (options    [this opts]                 "Appends opt(ion)s to the query")

  (compile    [this]                      "Returns an SQL statement"))

(defrecord RTable [cnx tname tcols restriction renames joins options]
  clojure.lang.IDeref
  (deref [this]
    (with-cnx cnx
      (with-results rs
        [(compile this)]
        (doall rs))))

  Relation
  (compile [this]
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

  (select [this predicate]
    (assoc this :restriction (conj (or restriction []) predicate)))

  (project [this fields]
    (assoc this :tcols (apply conj (or tcols []) fields)))

  (join [this table2 join-on]
    (if (has-aggregate? table2)
      (assoc this :joins (conj (or joins []) [table2 join-on]))
      (assoc this
        :tcols (apply conj (or tcols [])
                      (qualify (:tname table2) (:tcols table2)))
        :joins (assoc (or joins {}) (:tname table2) join-on))))

  (rename [this newnames]
    (assoc this :renames (merge (or renames {}) newnames)))

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

  (options [this opts]
    (assoc this :options (str options \space opts)))

  (limit    [this n]       (.options this (str "LIMIT " n)))
  (group-by [this col]     (.options this (str "GROUP BY " (name col))))
  (order-by [this col]     (.options this (str "ORDER BY " (qualify tname col))))
  (sort     [this col dir] (.options this (str "ORDER BY " (qualify tname col) \space
                                              (if (:asc dir) "ASC" "DESC")))))

(defn table
  ([connection-info table-name]
     (table connection-info table-name nil nil))
  ([connection-info table-name colums]
     (table connection-info table-name colums nil))
  ([connection-info table-name colums restrictions]
     (RTable. connection-info table-name colums restrictions nil nil nil)))

(defn table? [tinstance]
  (instance? clojureql.core.RTable tinstance))