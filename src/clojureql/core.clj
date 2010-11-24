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

                 @(table :mysql :users)

             How do I query 2 specific columns?

                 @(-> (table :mysql :users)
                      (project [:name :title]))

             How do I compose actions on queries?

                 @(-> (table :mysql :users)
                      (select (where (= :name \"Lau\")))
                      (project [:name :title]))
                      (sort :name :asc))

             How do I inspect the compiled SQL statement before execution?

                 (-> (table :mysql :users)
                     (select (where (= :name \"Lau\")))
                     (project [:name :title]))
                     (sort :asc)
                     to-sql)

             For more advanced examples please review README.md, demo.clj and core_test.clj."
    :url    "http://github.com/LauJensen/clojureql"}
  (:refer-clojure
   :exclude [take drop sort conj! disj!])
  (:use
   [clojureql internal predicates]
   [clojure.string :only [join] :rename {join join-str}]
   [clojure.contrib sql [core :only [-?> -?>>]]]
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

                                       ; INTERFACES


(defmacro with-results
  "Executes the body, wherein the results of the query can be accessed
   via the name supplies as results.

  Example:
   (with-results table res
     (println res))            "
  [[results tble] & body]
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
  `(where* ~(into {} (for [[local] &env] [(list 'quote local) local]))
           '~clause))

(defn requires-subselect?
  [table]
  (if (keyword? table)
    false
    (or (has-aggregate? table)
        (number? (:limit table)) ; TODO: Check offset as well?
        (seq (:restriction table)))))

(defn extract-aliases
  [joins]
  (for [[tbl-or-kwd pred] (map :data joins)
             :when (requires-subselect? tbl-or-kwd)
             :let [{:keys [tname tcols]} tbl-or-kwd
                   alias (find-first-alias tcols)]]
         [(to-tablename tname) (str (name tname) "_subselect." alias)]))

(declare table?)
(declare to-sql)

(defn apply-aliases
  [stmt aliases]
  [(reduce (fn [acc [old new]]
            (.replaceAll acc old (-> (.split new "\\.") first)))
          stmt aliases)])

(defn build-join
  "Generates a JOIN statement from the joins field of a table"
  [{[tname pred] :data type :type pos :position} aliases]
  (let [pred (if (and (seq aliases) (string? pred))
               (reduce (fn [acc a]
                         (let [t1name (if (or (keyword? tname) (string? tname))
                                        (to-tablename tname)
                                        (to-tablename (:tname tname)))
                               alias  (-> (.split a "\\.") first)]
                           (if (.contains alias t1name)
                             (replace-in acc t1name alias)
                             acc)))
                       pred (map last aliases))
               pred)
        [subselect env] (when (requires-subselect? tname)
                          (to-sql tname))]
    [(assemble-sql "%s %s JOIN %s %s %s"
       (if (keyword? pos)  (-> pos name .toUpperCase) "")
       (if (not= :join type) (-> type name .toUpperCase) "")
       (if (requires-subselect? tname)
         (assemble-sql "(%s) AS %s_subselect" subselect
                       (to-tablename (:tname tname)))
         (to-tablename tname))
       (if-not (keyword? pred) " ON " "")
       (if (keyword? pred)
         (format " USING(%s) " (name pred))
         (-> (str pred) (apply-aliases aliases) first)))
     (if (and subselect (map? pred))
       (assoc pred :env (into (:env pred) env))
       pred)]))

(defn to-sql [tble]
  (let [{:keys [cnx tname tcols restriction renames joins
                grouped-by limit offset order-by]} tble
        aliases   (when joins (extract-aliases joins))
        fields    (str (if tcols (to-fieldlist tname tcols) "*")
                       (when (seq aliases)
                         (str "," (join-str "," (map last aliases)))))
        jdata     (when joins
                    (for [join-data joins] (build-join join-data aliases)))
        tables    (if joins
                    (str (if renames
                           (with-rename tname (qualify tname tcols) renames)
                           (to-tablename tname)) \space
                           (join-str " " (map first jdata)))
                    (if renames
                      (with-rename tname (qualify tname tcols) renames)
                      (to-tablename tname)))
        preds     (if (and aliases restriction)
                    (reduce (fn [acc [orig-name alias]]
                              (replace-in acc orig-name (-> (.split alias "\\.") first)))
                            restriction aliases)
                    (when restriction
                      restriction))
        statement (clean-sql ["SELECT" fields
                       (when tables "FROM") tables
                       (when preds "WHERE") (str preds)
                       (when (seq order-by) (str "ORDER BY " (to-orderlist tname order-by)))
                       (when grouped-by     (str "GROUP BY " (to-fieldlist tname grouped-by)))
                       (when limit          (str "LIMIT " limit))
                       (when offset         (str "OFFSET " offset))])
        env       (->> [(map (comp :env last) jdata) (when preds [(:env preds)])]
                       flatten vec
                       (remove nil?)
                       vec)
        sql-vec   (into [statement] env)]
    (when *debug* (prn sql-vec))
    sql-vec))

(defn interpolate-sql [[stmt & args]]
  "For compilation test purposes only"
  (reduce #(.replaceFirst %1 "\\?" (str %2)) stmt args))


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

  (limit      [this n]                    "Queries the table with LIMIT n, call via take")
  (offset     [this n]                    "Queries the table with OFFSET n, call via drop")
  (sorted     [this fields]               "Sorts the query using fields, call via sort")
  (grouped    [this field]                "Groups the expression by field")

  (apply-on   [this f]                    "Applies f on a resultset, call via with-results"))

(defrecord RTable [cnx tname tcols restriction renames joins grouped-by limit offset order-by]
  clojure.lang.IDeref
  (deref [this]
     (in-connection*
      (with-results* (to-sql this)
        (fn [rs] (doall rs)))))

  Relation
  (apply-on [this f]
    (let [[sql-string & env] (to-sql this)]
     (in-connection*
       (with-open [stmt (.prepareStatement (:connection sqlint/*db*) sql-string)]
         (with-open [rset (.executeQuery stmt)]
           (doseq [[idx v] (map vector (iterate inc 1) env)]
             (.setObject stmt idx v))
           (f (resultset-seq rset)))))))

  (select [this predicate]
    (assoc this :restriction predicate)) ;(conj (or restriction []) predicate)))

  (project [this fields]
    (assoc this :tcols fields))

  (join [this table2 join-on]
    (if (requires-subselect? table2)
      (assoc this
        :joins (conj (or joins [])
                 {:data     [table2 join-on]
                 :type     :join
                 :position ""}))
      (assoc this
        :tcols (if-let [t2cols (seq (:tcols table2))]
                 (apply conj (or tcols [])
                        (qualify (to-tablename (:tname table2)) t2cols))
                 tcols)
        :joins (conj (or joins [])
                 {:data     [(to-tablename (:tname table2)) join-on]
                 :type     :join
                 :position ""}))))

  (outer-join [this table2 type join-on]
    (if (requires-subselect? table2)
      (assoc this
        :joins (conj (or joins [])
                 {:data     [table2 join-on]
                 :type     :outer
                 :position type}))
      (assoc this
        :tcols (if-let [t2cols (seq (:tcols table2))]
                 (apply conj (or tcols [])
                        (qualify (to-tablename (:tname table2)) t2cols))
                 tcols)
        :joins (conj (or joins [])
                 {:data     [(to-tablename (:tname table2)) join-on]
                  :type     :outer
                  :position type}))))

  (rename [this newnames]
    (assoc this :renames (merge (or renames {}) newnames)))

  (aggregate [this aggregates]
    (aggregate this aggregates []))

  (aggregate [this aggregates group-by]
     (let [grps (reduce conj group-by grouped-by)
           table (project this (into grps aggregates))]
      (if (seq grps)
        (assoc table :grouped-by grps)
        table)))

  (conj! [this records]
     (in-connection*
      (if (map? records)
        (insert-records tname records)
        (apply insert-records tname records)))
     this)

  (disj! [this predicate]
     (in-connection*
       (delete-rows tname (into [(str predicate)] (:env predicate))))
    this)

  (update-in! [this pred records]
    (let [predicate (into [(str pred)] (:env pred))]
      (in-connection*
       (if (map? records)
         (update-or-insert-values tname predicate records)
         (apply update-or-insert-values tname predicate records)))
      this))

  (grouped [this field]
    (assoc this :grouped-by (to-name tname field)))

  (limit [this n]
    (if limit
      (assoc this :limit (min limit n))
      (assoc this :limit n)))

  (offset [this n]
    (let [limit  (if limit  (- limit  n))
          offset (if offset (+ offset n) n)]
      (assoc this
        :limit  limit
        :offset offset)))

  (sorted [this fields]
    (assoc this
      :order-by fields)))

                                        ; INTERFACES

(defn table
  "Constructs a relational object."
  ([table-name]
     (table nil table-name))
  ([connection-info table-name]
     (RTable. connection-info table-name [:*] nil nil nil nil nil nil nil)))

(defn table?
  "Returns true if tinstance is an instnce of RTable"
  [tinstance]
  (instance? clojureql.core.RTable tinstance))

(defn take
  "A take which works on both tables and collections"
  [obj & args]
  (if (table? obj)
    (apply limit obj args)
    (apply clojure.core/take obj args)))

(defn sort
  "A sort which works on both tables and collections"
  [obj & args]
  (if (table? obj)
    (apply sorted obj args)
    (apply clojure.core/sort obj args)))

(defn drop
  "A drop which works on both tables and collections"
  [obj & args]
  (if (table? obj)
    (apply offset obj args)
    (apply clojure.core/drop obj args)))