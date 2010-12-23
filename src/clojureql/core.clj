(ns clojureql.core
  ^{:author "Lau B. Jensen    <lau.jensen@bestinclass.dk>"
    :doc    "ClojureQL is superior SQL integration for Clojure, which allows
             you to access tables and rows as objects that have uniform interfaces
             for queries, inserts and deletions.

             Please see the README.md for documentation"
    :url    "http://github.com/LauJensen/clojureql"}
  (:refer-clojure
   :exclude [take drop sort distinct conj! disj! compile])
  (:use
   [clojureql internal predicates]
   [clojure.string :only [join upper-case] :rename {join join-str}]
   [clojure.contrib sql [core :only [-?> -?>>]]]
   [clojure.contrib.sql.internal :as sqlint]
   [clojure.walk :only (postwalk-replace)]))

                                        ; GLOBALS

(def *debug* false)
(def global-connections (atom {}))

(declare table?)
(declare compile)
(declare table)

                                        ; CONNECTIVITY

(defn open-global
  "Opens a global connection with the supplied specs. If given a
  conn-name, use it as a key to access that connection, else set the
  default global connection."
  [specs & [conn-name]]
  (let [con (sqlint/get-connection specs)
        conn-name (or conn-name ::default-connection)]
    (when-let [ac (-> specs :auto-commit)]
      (.setAutoCommit con ac))
    (swap! global-connections assoc conn-name {:connection con :opts specs})))

(defn close-global
  "Supplied with a keyword identifying a global connection, that connection
  is closed and the reference dropped. "
  [& [conn-name]]
  (let [conn-name (or conn-name ::default-connection)]
    (if-let [conn (conn-name @global-connections)]
      (do
        (.close (:connection conn))
        (swap! global-connections dissoc conn-name)
        true)
      (throw
       (Exception.
        (format "No global connection by that name is open (%s)" conn-name))))))

(defmacro with-cnx
  "For internal use only. If you want to wrap a query in a connection, use
   with-connection"
  [db-spec & body]
  `(with-cnx* ~db-spec (fn [] ~@body)))

(defn with-cnx*
  "Evaluates func in the context of a new connection to a database then
  closes the connection."
  [con-info func]
  (io!
  (let [con-info (or con-info ::default-connection)]
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
          (func)))))))

                                       ; INTERFACES


(defmacro with-results
  "Executes the body, wherein the results of the query can be accessed
   via the name supplies as results.

  Example:
   (with-results [res table]
     (println res))"
  [[results tble] & body]
  `(apply-on ~tble (fn [~results] ~@body)))

(defmacro where [clause]
  "Constructs a where-clause for queries.

   (where (or (< :a 2) (>= :b 4))) => \"((a < ?) OR (b >= ?))\"

   If you call str on the result, you'll get the above. If you call
   (:env) you will see the captured environment

   Use as: (select tble (where ...))"
  `~(postwalk-replace
     '{=   clojureql.predicates/=*
       !=  clojureql.predicates/!=*
       <   clojureql.predicates/<*
       >   clojureql.predicates/>*
       <=  clojureql.predicates/<=*
       >=  clojureql.predicates/>=*
       and clojureql.predicates/and*
       or  clojureql.predicates/or*
       not clojureql.predicates/not*}
     clause))

(defmulti compile
  (fn [table db] (:dialect db)))

(defn- combination-op [combination]
  (->> [(:type combination) (:mode combination)]
       (remove nil?)
       (map name)
       (join-str " ")
       upper-case))

(defn- append-combination [type relation-1 relation-2 & [mode]]
  (assoc relation-1
    :combination
    (if-let [combination (:combination relation-1)]
      {:relation (append-combination type (:relation combination) relation-2 mode)
       :type (:type combination)
       :mode (:mode combination)}
      {:relation relation-2 :type type :mode mode}) ))

(defn- append-combinations [type relation relations & [mode]]
  (reduce #(append-combination type %1 %2 mode)
          relation (if (vector? relations) relations [relations])))

(defn build-join
  "Generates a JOIN statement from the joins field of a table"
  [{[tname pred] :data type :type pos :position} aliases]
  (let [pred (if (and (seq aliases) (string? pred))
               (reduce (fn [acc a]
                         (let [t1name (if (or (keyword? tname) (string? tname))
                                        (to-tablename tname)
                                        (to-tablename (:tname tname)))
                               alias  (-> (.split a "\\.") first)]
                             acc))
                       pred (map last aliases))
               pred)
        [subselect env] (when (requires-subselect? tname)
                          (compile tname :default))]
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

(defmethod compile :default [tble db]
  (let [{:keys [cnx tname tcols restriction renames joins
                grouped-by limit offset order-by modifiers]} tble
        aliases   (when joins (extract-aliases joins))
        mods      (join-str \space (map upper-name modifiers))
        combination (if (:combination tble) (compile (:relation (:combination tble)) :default))
        fields    (when-not (table? tcols)
                    (str (if tcols (to-fieldlist tname tcols) "*")
                         (when (seq aliases)
                           (str ","
                                (->> (map rest aliases)
                                     (map #(join-str "," %))
                                     (apply str))))))
        jdata     (when joins
                    (for [join-data joins] (build-join join-data aliases)))
        tables    (cond
                   joins
                    (str (if renames
                           (with-rename tname (qualify tname tcols) renames)
                           (to-tablename tname))
                         \space
                         (join-str " " (map first jdata)))
                    (table? tcols)
                    (compile tcols nil)
                    :else
                    (if renames
                      (with-rename tname (qualify tname tcols) renames)
                      (to-tablename tname)))
        preds     (when restriction restriction)
        statement (clean-sql ["SELECT" mods (or fields "*")
                       (when tables "FROM") (if (string? tables)
                                              tables
                                              (format "(%s)" (first tables)))
                       (when preds "WHERE") (str preds)
                       (when (seq order-by) (str "ORDER BY " (to-orderlist tname order-by)))
                       (when grouped-by     (str "GROUP BY " (to-fieldlist tname grouped-by)))
                       (when limit          (str "LIMIT " limit))
                       (when offset         (str "OFFSET " offset))
                       (when combination    (str (combination-op (:combination tble))
                                                 \space
                                                 (first combination)))])
        env       (concat
                   (->> [(map (comp :env last) jdata)
                         (if (table? tcols) (rest tables))
                         (if preds [(:env preds)])]
                        flatten (remove nil?) vec)
                   (rest combination))
        sql-vec   (into [statement] env)]
    (when *debug* (prn sql-vec))
    sql-vec))

                                        ; RELATIONAL ALGEBRA

(defprotocol Relation
  (select     [this predicate]
    "Confines the query to rows for which the predicate is true

     Ex. (select (table :users) (where (= :id 5)))")
  (project    [this fields]
    "Confines the query to the fieldlist supplied in fields

     Ex. (project (table :users) [:email])")
  (join       [this table2 join_on]
    "Joins two tables on join_on

     Ex. (join (table :one) (table :two) :id)
         (join (table :one) (table :two)
               (where (= :one.col :two.col)))")
  (outer-join [this table2 type join_on]
    "Joins two tables on join_on and sets the direction of the join. type
     can be :right, :left, :full etc. Backend support may vary.

     Ex. (outer-join (table :one) (table :two) :left :id)
         (outer-join (table :one) (table :two) :left
                     (where (= :one.id :two.id)))")

  (rename     [this newnames]
    "Renames colums when joining. Newnames is a map of replacement pairs

     Ex. (-> (join (table :one) (table :two) :id)
             (project [:id])
             (rename {:one.id :idx}))")
  (aggregate  [this aggregates]
              [this aggregates group-by]
    "Selects aggregates from a table. Aggregates are denoted with the
     :function/field syntax. They can be aliased by supplying a vector
     [:function/field :as :myfn]. Optionally accepts a group-by argument

     Ex. (-> (table :one)
             (aggregate [[:count/* :as :cnt]] [:id]))")

  (modify     [this modifiers]
    "Allows for arbitrary modifiers to be applied on the result. Can either
     be called directly or via helper interfaces like 'distinct'.

     Ex. (-> (table :one)
             (modify \"TOP 5\")) ; MSSqls special LIMIT syntax
         (-> (table :one) distinct)")

  (pick       [this kw]
    "For queries where you know only a single result will be returned,
     pick calls the keyword on that result. You can supply multiple keywords
     in a collection.  Returns nil for no-hits, throws
     an exception on multiple hits.

     Ex. (-> (table :users)
             (select (where (= :id 5))) ; We know this will only match 1 row
             (pick :email))")

  (conj!      [this records]
    "Inserts record(s) into the table

     Ex. (conj! (table :one) {:age 22})
         (conj! (table :one) [{:age 22} {:age 23}]")
  (disj!      [this predicate]
    "Deletes record(s) from the table

     Ex. (disj! (table :one) (where (= :age 22)))")

  (update-in! [this pred records]
    "Inserts or updates record(s) where pred is true. Accepts records
     as both maps and collections.

     Ex. (update-in! (table :one) (where (= :id 5))
            {:age 22})")

  (difference   [this relations]
                [this relations mode]
    "Selects the difference between tables. Mode can take a keyword
     which can be anything which your backend supports. Commonly :all is
     used to allow duplicate rows.

     Ex. (-> (table :one)
             (difference (table :two) :all))")
  (intersection [this relations]
                [this relations mode]
    "Selects the intersection between tables. Mode can take a keyword
     which can be anything which your backend supports. Commonly :all is
     used to allow duplicate rows.

     Ex. (-> (table :one)
             (intersection (table :two) :all))")
  (union        [this relations]
                [this relations mode]
    "Selects the union between tables. Mode can take a keyword
     which can be anything which your backend supports. Commonly :all is
     used to allow duplicate rows.

     Ex. (-> (table :one)
             (union (table :two) :all))")
  (limit      [this n]                    "Internal: Queries the table with LIMIT n, call via take")
  (offset     [this n]                    "Internal: Queries the table with OFFSET n, call via drop")
  (order-by   [this fields]               "Internal: Orders the query by fields, call via sort")
  (apply-on   [this f]                    "Internal: Applies f on a resultset, call via with-results")
  (grouped    [this field]                "Internal: Groups the expression by field"))

(defrecord RTable [cnx tname tcols restriction renames joins
                   grouped-by limit offset order-by modifiers]
  clojure.lang.IDeref
  (deref [this]
     (in-connection*
      (with-results* (compile this cnx)
        (fn [rs] (doall rs)))))

  Relation
  (apply-on [this f]
    (let [[sql-string & env] (compile this cnx)]
     (in-connection*
       (with-open [stmt (.prepareStatement (:connection sqlint/*db*) sql-string)]
	 (doseq [[idx v] (map vector (iterate inc 1) env)]
	   (.setObject stmt idx v))
         (with-open [rset (.executeQuery stmt)]
           (f (resultset-seq rset)))))))

  (pick [this kw]
    (let [results @this]
      (if (or (= 1 (count results)) (empty? results))
        (if (coll? kw)
          (map (first results) kw)
          (kw (first results)))
        (throw (Exception. "Multiple items in resultsetseq, keyword lookup not possible")))))

  (select [this clause]
    (assoc this :restriction
           (fuse-predicates (or restriction (predicate nil nil))
                            clause)))

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

  (modify [this new-modifiers]
    (assoc this :modifiers
           (into (or modifiers []) (if (coll? new-modifiers)
                                     new-modifiers
                                     [new-modifiers]))))

  (difference [this relations]
    (difference this relations nil))

  (difference [this relations mode]
    (append-combinations :except this relations mode))

  (intersection [this relations]
    (intersection this relations nil))

  (intersection [this relations mode]
    (append-combinations :intersect this relations mode))

  (union [this relations]
    (union this relations nil))

  (union [this relations mode]
    (append-combinations :union this relations mode))

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
      (when *debug* (prn predicate))
      (in-connection*
       (if (map? records)
         (update-or-insert-vals tname predicate records)
         (apply update-or-insert-vals tname predicate records)))
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

  (order-by [this fields]
    (if (seq order-by)
      (assoc (table cnx tname)
        :tcols (assoc this :order-by order-by)
        :order-by fields)
      (assoc this
        :order-by fields))))

                                        ; INTERFACES

(defn interpolate-sql [[stmt & args]]
  "For compilation test purposes only"
  (reduce #(.replaceFirst %1 "\\?" (if (nil? %2) "NULL" (str %2))) stmt args))

(defmethod print-method RTable [tble ^String out]
  "RTables print as SQL92 compliant SQL"
  (.write out (-> tble (compile nil) interpolate-sql)))

(defn table
  "Constructs a relational object."
  ([table-name]
     (table nil table-name))
  ([connection-info table-name]
     (let [connection-info (if (fn? connection-info)
			     (connection-info)
			     connection-info)]
       (RTable. connection-info table-name [:*] nil nil nil nil nil nil nil nil))))

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
    (apply order-by obj args)
    (apply clojure.core/sort obj args)))

(defn drop
  "A drop which works on both tables and collections"
  [obj & args]
  (if (table? obj)
    (apply offset obj args)
    (apply clojure.core/drop obj args)))

(defn distinct
  "A distinct which works on both tables and collections"
  [obj & args]
  (if (table? obj)
    (modify obj :distinct)
    (apply clojure.core/distinct obj args)))