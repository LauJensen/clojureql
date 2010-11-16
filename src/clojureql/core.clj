(ns clojureql.core
  ^{:author "Lau B. Jensen    <lau.jensen@bestinclass.dk>"
    :doc    "ClojureQL is superior SQL integration for Clojure, which allows
             you to access tables and rows as objects that have uniform interfaces
             for queries, inserts and deletions.

             Short Tutorial:

             First you may define a connection object through which database access
             will be provided.

             (def db
                  {:classname   \"com.mysql.jdbc.Driver\"
                   :subprotocol \"mysql\"
                   :user        \"cql\"
                   :password    \"cql\"
                   :subname     \"//localhost:3306/cql\"})

             This object can be used in 2 ways:
               1) As a connection which opens and closes on every query.
                  To achieve this, pass the db object as the first argument
                  when you construct a table:

                          (table db :users)

               2) As a persistent connection, which does not automatically close:

                          (open-global :mysql db)

                  This call opens the connection, which can then be supplied to the
                  table constructor as well:

                          (table :mysql :users)

             With a connection established/defined you can begin interacting with your
             database. Everything revolves around table objects, which can be queried (@)
             added to (conj!), deleted from (disj!), joined to others tables (join or
             outer-join) and much more. All actions on tables are composable and this
             is the contract:

                 No action is executed unless the table object is deferenced (@) or
                 if you call a function which is suffixed with a bang (!).

             How do I query all fields of a table named 'users'?

                 @(table :mysql :users [:*])

             How do I query 2 specific columns?

                 @(table :mysql :users [:name :title])

             How do I compose actions on queries?

                 @(-> (table :mysql :users [:name :title])
                      (select (where (= :name \"Lau\")))
                      (sort :asc))

             How do I inspect the compiled SQL statement before execution?

                 (-> (table :mysql :users [:name :title])
                     (select (where (= :name \"Lau\")))
                     (sort :asc)
                     assemble) ; TODO: Final name?

             For more advanced examples please review README.md, demo.clj and core_test.clj."
    :url    "http://github.com/LauJensen/clojureql"}
  (:refer-clojure
   :exclude [take drop sort conj! disj!])
  (:use
   [clojureql internal predicates]
   [clojure.string :only [join] :rename {join join-str}]
   [clojure.contrib sql [core :only [-?>]]]
   [clojure.contrib.sql.internal :as sqlint]))

                                        ; GLOBALS

(def *debug* false) ; If true: Shows all SQL expressions before executing
(declare table?)
(def global-connections (atom {}))
                                        ; CONNECTIVITY

(defn open-global [id specs]
  (let [con (sqlint/get-connection specs)]
    (when-let [ac (-> specs :auto-commit)]
      (.setAutoCommit con ac))
    (swap! global-connections assoc id {:connection con :opts specs})))

(defn close-global
  "Supplied with a keyword identifying a global connection, that connection
  is closed and the reference dropped."
  [conn-name]
  (if-let [conn (conn-name @global-connections)]
    (do
      (.close (:connection conn))
      (swap! global-connections dissoc conn-name)
      true)
    (throw
     (Exception. (format "No global connection by that name is open (%s)" conn-name)))))

(defn with-cnx*
  "Evaluates func in the context of a new connection to a database then
  closes the connection."
  [con-info func]
  (io!
   (if (keyword? con-info)
     (if-let [con (@clojureql.core/global-connections con-info)]
       (binding [sqlint/*db*
                 (assoc sqlint/*db*
                   :connection (:connection con)
                   :level 0
                   :rollback (atom false)
                   :opts     (:opts con))]
         (func))
       (throw
        (Exception. "No such global connection currently open!")))
     (with-open [con (sqlint/get-connection con-info)]
       (binding [sqlint/*db*
                 (assoc sqlint/*db*
                   :connection con
                   :level 0
                   :rollback (atom false)
                   :opts     con-info)]
         (.setAutoCommit con (or (-> con-info :auto-commit) true))
         (func))))))

(defmacro with-cnx
  "For internal use only. If you want to wrap a query in a connection, use
   with-connection"
  [db-spec & body]
  `(with-cnx* ~db-spec (fn [] ~@body)))

                                       ;HELPERS


(defmacro with-results
  "Executes the body, wherein the results of the query can be accessed
   via the name supplies as results.

  Example:
   (with-results table res
     (println res))            "
  [tble results & body]
  `(apply-on ~tble (fn [~results] ~@body)))

(defmacro in-connection*
  "For internal use only!

   This lets users supply a nil argument as the connection when
   constructing a table, and instead wrapping their calls in
   with-connection"
  [& body]
  `(if ~'cnx
     (with-cnx ~'cnx (do ~@body))
     (do ~@body)))

(defmacro where [clause]
  "Constructs a where-clause for queries.

   (where (or (< :a 2) (>= :b 4))) => \"((a < 2) OR (b >= 4))\"

   Strings are auto quoted. Typically you will use this in conjunction with
   select, ie.

   (select tble (where ...))"
  `(where* '~clause))

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

  (take       [this n]                    "Queries the table with LIMIT n")
  (drop       [this n]                    "Queries the table with OFFSET n")
  (sort       [this fields]               "Sorts the query using fields.")

  (apply-on   [this f]                    "Applies f on a resultset, use via with-results")

  (to-sql     [this]                      "Returns an SQL statement"))

(defrecord RTable [cnx tname tcols restriction renames joins group-by limit offset order-by]
  clojure.lang.IDeref
  (deref [this]
     (in-connection*
      (with-results* [(to-sql this)] (fn [rs] (doall rs)))))

  Relation
  (apply-on [this f]
     (in-connection*
       (with-cnx cnx
         (with-open [stmt (.prepareStatement (:connection sqlint/*db*) (to-sql this))]
           (with-open [rset (.executeQuery stmt)]
             (f (resultset-seq rset)))))))

  (to-sql [this]
    (let [sql-string
          (if (seq joins)
            (if (table? (-> joins :data first))
              ;; joining with a table containing aggregates
              (let [[t2 pred] (:data joins)
                    t2name    (-> t2 :tname to-tablename)
                    colalias  (find-first-alias (:tcols t2))
                    t2alias   (str t2name "_aggregation")]
                (-> (format "SELECT %s FROM %s %s JOIN (%s) AS %s ON %s %s"
                            (derived-fields tname tcols t2alias colalias)
                            (to-tablename tname)
                            (-> (:position joins) name .toUpperCase)
                            (-> (:type joins) name .toUpperCase)
                            t2alias
                            (.replaceAll pred t2name t2alias)
                            (if group-by (str "GROUP BY " (to-fieldlist tname group-by)) ""))
                    .trim))
              ;; joining with non-aggregate table
              (-> (format "SELECT %s FROM %s %s %s %s"
                          (->> tcols (to-fieldlist tname))
                          (if renames
                            (with-rename tname (qualify tname tcols) renames)
                            (to-tablename tname))
                          (if joins (build-join (:data joins)) "")
                          (if restriction (restrict (join-str " AND " restriction)) "")
                          (if group-by (str "GROUP BY " (to-fieldlist tname group-by)) ""))
                  .trim))
            ;; no joins
            (-> (format "SELECT %s FROM %s %s %s"
                        (->> tcols (to-fieldlist tname))
                        (if renames
                          (with-rename tname (qualify tname tcols) renames)
                          (to-tablename tname))
                        (if restriction (restrict (join-str " AND " restriction)) "")
                        (if group-by (str "GROUP BY " (to-fieldlist tname group-by)) ""))
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
        (assoc table :group-by group-by)
        table)))

  (conj! [this records]
     (in-connection*
      (if (map? records)
        (insert-records tname records)
        (apply insert-records tname records)))
     this)

  (disj! [this predicate]
     (in-connection*
       (delete-rows tname [(compile-expr predicate)]))
    this)

  (update-in! [this pred records]
     (in-connection*
      (if (map? records)
        (update-or-insert-values tname [pred] records)
        (apply update-or-insert-values tname [pred] records)))
     this)

  (take [this n]
    (if limit
      (assoc this :limit (min limit n))
      (assoc this :limit n)))

  (drop [this n]
    (let [limit  (if limit  (- limit  n))
          offset (if offset (+ offset n) n)]
      (assoc this
        :limit  limit
        :offset offset)))

  (sort [this fields]
    (assoc this
      :order-by fields)))

(defn table
  "Constructs a relational object."
  ([connection-info table-name]
     (table connection-info table-name nil nil))
  ([connection-info table-name colums]
     (table connection-info table-name colums nil))
  ([connection-info table-name colums restrictions]
     (RTable. connection-info table-name colums restrictions nil nil nil nil nil nil)))

(defn table?
  "Returns true if tinstance is an instnce of RTable"
  [tinstance]
  (instance? clojureql.core.RTable tinstance))