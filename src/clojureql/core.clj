(ns clojureql.core
  ^{:author "Lau B. Jensen    <lau.jensen@bestinclass.dk>"
    :doc    "ClojureQL is superior SQL integration for Clojure, which allows
             you to access tables and rows as objects that have uniform interfaces
             for queries, inserts and deletions."
    :url    "http://github.com/LauJensen/clojureql"}
  (:refer-clojure
   :exclude [compile take sort conj! disj! < <= > >= =]
   :rename {take take-coll})
  (:use
   [clojureql internal predicates]
   [clojure.string :only [join] :rename {join join-str}]
   [clojure.contrib sql [core :only [-?>]]]))

                                        ; GLOBALS

(def *debug* false) ; If true: Shows all SQL expressions before executing
(declare table?)
                                        ; RELATIONAL ALGEBRA

(defprotocol Relation
  (select     [this predicate]            "Queries the table using a predicate")
  (project    [this fields]               "Projects fields onto the query")
  (join       [this table2 join_on]       "Joins two table")
  (outer-join [this table2 type join_on]  "Makes an outer join of type :left|:right|:full")
  (rename     [this newnames]             "Renames colums in a join")
  (aggregate  [this aggregates]
              [this aggregates group-by]  "Computes aggregates grouped by the specified fields")

  (conj!      [this records]              "Inserts record(s) into the table")
  (disj!      [this predicate]            "Deletes record(s) from the table")
  (update-in! [this pred records]         "Inserts or updates record(s) where pred is true")

  (limit      [this n]                    "Queries the table with LIMIT n")
  (order-by   [this col]                  "Orders the Query by the column")

  (sort       [this col type]             "Sorts the query either :asc or :desc")
  (options    [this opts]                 "Appends opt(ion)s to the query")

  (compile    [this]                      "Returns an SQL statement"))

(defrecord RTable [cnx tname tcols restriction renames joins options]
  clojure.lang.IDeref
  (deref [this]
    (if cnx
      (with-cnx cnx (with-results rs [(compile this)] (doall rs)))
      (with-results rs [(compile this)] (doall rs))))

  Relation
  (compile [this]
    (let [sql-string
      (if (seq joins)
        (cond
                                        ;joining with a table containing aggregates
         (table? (-> joins :data first))
         (let [[t2 pred]    (:data joins)
               t2name       (-> t2 :tname to-tablename)
               colalias     (find-first-alias (:tcols t2))
               t2alias      (str t2name "_aggregation")]
           (-> (format "SELECT %s FROM %s %s %s JOIN (%s) AS %s ON %s %s"
                       (derrived-fields tname tcols t2alias colalias)
                       (to-tablename tname)
                       (-> (:position joins) name .toUpperCase)
                       (-> (:type joins) name .toUpperCase)
                       (-> (.options t2 (str "GROUP BY " (-> t2 :tcols first name)))
                           compile)
                       t2alias
                       (.replaceAll pred t2name t2alias)
                       (or options ""))
               .trim))
         :else
                                        ;joining with non-aggregate table
         (-> (format "SELECT %s FROM %s %s %s %s"
                     (->> tcols (to-fieldlist tname))
                     (if renames
                       (with-rename tname (qualify tname tcols) renames)
                       (to-tablename tname))
                     (if joins (build-join (:data joins)) "")
                     (if restriction (restrict (join-str " AND " restriction)) "")
                     (or options ""))
             .trim))
                                        ; Non Join compile
         (-> (format "SELECT %s FROM %s %s %s"
                     (->> tcols (to-fieldlist tname))
                     (if renames
                       (with-rename tname (qualify tname tcols) renames)
                       (to-tablename tname))
                     (if restriction (restrict (join-str " AND " restriction)) "")
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
      (assoc this
        :joins (assoc (or joins {})
                 :data     [table2 join-on]
                 :type     :join
                 :position ""))
      (assoc this
        :tcols (if-let [t2cols (seq (:tcols table2))]
                 (apply conj (or tcols [])
                        (qualify (to-tablename (:tname table2)) t2cols))
                 tcols)
        :joins (assoc (or joins {})
                 :data     [(to-tablename (:tname table2)) join-on]
                 :type     :join
                 :position ""))))

  (outer-join [this table2 type join-on]
    (if (has-aggregate? table2)
      (assoc this
        :joins (assoc (or joins {})
                 :data     [table2 join-on]
                 :type     :outer
                 :position type))
      (assoc this
        :tcols (if-let [t2cols (seq (:tcols table2))]
                 (apply conj (or tcols [])
                        (qualify (to-tablename (:tname table2)) t2cols))
                 tcols)
        :joins (assoc (or joins {})
                 :data     [(to-tablename (:tname table2)) join-on]
                 :type     :outer
                 :position type))))

  (rename [this newnames]
    (assoc this :renames (merge (or renames {}) newnames)))

  (aggregate [this aggregates]
    (aggregate this aggregates []))

  (aggregate [this aggregates group-by]
    (let [table (project this (into group-by aggregates))]
      (if (seq group-by)
        (.options table (str "GROUP BY " (to-fieldlist tname group-by)))
        table)))

  (conj! [this records]
    (letfn [(exec [] (if (map? records)
                       (insert-records tname records)
                       (apply insert-records tname records)))]
      (if cnx
        (with-cnx cnx (exec))
        (exec)))
    this)

  (disj! [this predicate]
     (if cnx
       (with-cnx cnx (delete-rows tname [(compile-expr predicate)]))
       (delete-rows tname [(compile-expr predicate)]))
    this)

  (update-in! [this pred records]
     (letfn [(exec [] (if (map? records)
                        (update-or-insert-values tname [pred] records)
                        (apply update-or-insert-values tname [pred] records)))]
       (if cnx
         (with-cnx cnx (exec))
         (exec)))
     this)

  (options [this opts]
    (assoc this :options (str options \space opts)))

  (limit    [this n]       (.options this (str "LIMIT " n)))
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

(defmacro where [clause]
  `(where* '~clause))

