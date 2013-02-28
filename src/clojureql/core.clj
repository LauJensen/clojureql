(ns clojureql.core
  ^{:author "Lau B. Jensen    <lau.jensen@bestinclass.dk>"
    :doc    "ClojureQL is superior SQL integration for Clojure, which allows
             you to access tables and rows as objects that have uniform interfaces
             for queries, inserts and deletions.

             Please see the http://www.clojureql.org for documentation"
    :url    "http://github.com/LauJensen/clojureql"}
  (:refer-clojure
   :exclude [take drop sort distinct conj! disj! compile case])
  (:use
    [clojureql internal predicates]
    [clojure.string :only [join upper-case] :rename {join join-str}]
    [clojure.java.jdbc :only [delete-rows]]
    [clojure.core.incubator :only [-?> -?>>]]
    [clojure.walk :only (postwalk-replace)]))

                                        ; GLOBALS
(def ^{:dynamic true} *debug* false)

(declare table?)
(declare table)

(defmulti compile
  (fn [table db] (:dialect db)))

(load "connectivity")
(load "sql92compiler")

                                        ; INTERFACES

(defmacro with-results
  "Executes the body, wherein the results of the query can be accessed
   via the name supplies as results.

  Example:
   (with-results [res table]
     (println res))"
  [[results tble] & body]
  `(apply-on ~tble (fn [~results] ~@body)))

(def predicate-symbols
  '{=    clojureql.predicates/=*
    !=   clojureql.predicates/!=*
    <    clojureql.predicates/<*
    >    clojureql.predicates/>*
    <=   clojureql.predicates/<=*
    >=   clojureql.predicates/>=*
    and  clojureql.predicates/and*
    or   clojureql.predicates/or*
    not  clojureql.predicates/not*
    like clojureql.predicates/like
    nil? clojureql.predicates/nil?*
    in   clojureql.predicates/in})

(defn- where* [clause]
  (postwalk-replace predicate-symbols clause))

(defmacro where [clause]
  "Constructs a where-clause for queries.

   (where (or (< :a 2) (>= :b 4))) => \"((a < ?) OR (b >= ?))\"

   If you call str on the result, you'll get the above. If you call
   (:env) you will see the captured environment

   Use as: (select tble (where ...))"
  (where* clause))

(defmacro case
  "Lets you specify a column using the SQL CASE operator.

   The first argument is your alias for the return of CASE, the remaining
   arguments are a series of conditions and their returns similar to condp.
   The final two arguments can optionally be ':else value'.

   Example:
     (project (table :t1)
           [:id (case :wages
                  (<= :wage 5)  \"low\"
                  (>= :wage 10) \"high\"
                  :else         \"average\")])"
  [alias & clauses]
  (let [pairs (->> (if (= :else (-> clauses vec rseq second))
                     (drop-last 2 clauses)
                     clauses)
                   where*
                   (partition 2))]
    {:alias   alias
     :clauses (vec (map first pairs))
     :else    (when (= :else (-> clauses vec rseq second))
                (last clauses))
     :returns (vec (map last pairs))}))

(defprotocol Relation
  (select     [this predicate]
    "Confines the query to rows for which the predicate is true

     Ex. (select (table :users) (where (= :id 5)))")

  (select-if  [this test predicate]
              [this test predicate else]
    "Evaluates test. If logical true, confines the query to rows for which
     the predicate is true. Optionally accepts a predicate to confine the
     query if the test is logical false.

     Ex. (select-if (table :users)
                    (nil? s)
                      (where (= :email \"default@website.com\"))
                      (where (= :email s))")

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

  (transform  [this fn]
    "Transforms results using fn when deref or with-results is called.
     The pick helper function is implemented using this.
     Ex. (-> (table :users)
             (select (where (= :id 5)))
             (transform #(map :email %))")

  (conj!      [this records]
    "Inserts record(s) into the table

     Ex. (conj! (table :one) {:age 22})
         (conj! (table :one) [{:age 22} {:age 23}]")
  (disj!      [this predicate]
    "Deletes record(s) from the table

     Ex. (disj! (table :one) (where (= :age 22)))")

  (update! [this pred record]
    "Updates a record where pred is true. Record
     is a map from strings or keywords (identifying columns)
     to updated values.

     Ex. (update! (table :one) (where (= :id 5))
            {:age 22})")

  (update-in! [this pred record]
    "Inserts or updates a record where pred is true. Record
     is a map from strings or keywords (identifying columns)
     to updated values.

     Ex. (update-in! (table :one) (where (= :id 5))
            {:age 22})")

  (difference   [this relations]
                [this relations opts]
    "Selects the difference between tables. Mode can take a keyword
     which can be anything which your backend supports. Commonly :all is
     used to allow duplicate rows.

     Ex. (-> (table :one)
             (difference (table :two) :all))")
  (intersection [this relations]
                [this relations opts]
    "Selects the intersection between tables. Mode can take a keyword
     which can be anything which your backend supports. Commonly :all is
     used to allow duplicate rows.

     Ex. (-> (table :one)
             (intersection (table :two) :all))")
  (union        [this relations]
                [this relations opts]
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
                   grouped-by pre-scope scope order-by modifiers
                   combinations having transform]
  clojure.lang.IDeref
  (deref [this]
    ;; we might not get a sequence, if transform is set
    (apply-on this #(if (seq? %) (doall %) %)))

  Relation
  (apply-on [this f]
    (with-cnx cnx
      (with-results* (compile this cnx)
        (fn [results]
          (f (if transform
               (transform results)
               results))))))

  (transform [this fn]
    (if transform
      (assoc this :transform (comp fn transform))
      (assoc this :transform fn)))

  (select [this clause]
    (if (and (has-aggregate? this) (seq grouped-by))
      (assoc this :having ; TODO: Throw exception if clause contains column not in grouped-by
             (->> (qualify-predicate this clause)
                  (fuse-predicates (or having (predicate nil nil)))))
      (assoc this :restriction
             (->> (qualify-predicate this clause)
                  (fuse-predicates (or restriction (predicate nil nil)))))))

  (select-if [this test clause]
    (if test (select this clause) this))

  (select-if [this test clause else]
    (if test (select this clause) (select this else)))

  (project [this fields]
    (assoc this :tcols fields))

  (join [this table2 join-on]
    (outer-join this table2 nil join-on))

  (outer-join [this table2 type join-on]
	      (let [j (into (or joins []) (-> table2 :joins (or [])))]
		(if (requires-subselect? table2)
		  (assoc this
		    :tcols (into (or tcols [])
				 (rename-subselects (:tname table2)
						    (-> table2 :grouped-by first)))
		    :joins (sort-joins (conj j
					     {:data     [table2 join-on]
					      :type     (if (keyword? type) :outer :join)
					      :position type})))
		  (assoc this
		    :tcols (if-let [t2cols (seq (:tcols table2))]
			     (apply conj (or tcols [])
				    (map #(add-tname (:tname table2) %)
					 (if (coll? t2cols)
					   t2cols [t2cols])))
			     tcols)
		    :joins (sort-joins (conj j
					     {:data     [(to-tablename (:tname table2)) join-on]
					      :type     (if (keyword? type) :outer :join)
					      :position type}))))))

  (modify [this new-modifiers]
    (assoc this :modifiers
           (into (or modifiers []) (if (coll? new-modifiers)
                                     new-modifiers
                                     [new-modifiers]))))

  (difference [this relations]
    (difference this relations nil))

  (difference [this relations opts]
    (assoc this :combinations
           (conj (or combinations [])
                 {:table relations :mode :except :opts opts})))

  (intersection [this relations]
    (intersection this relations nil))

  (intersection [this relations opts]
    (assoc this :combinations
           (conj (or combinations [])
                 {:table relations :mode :intersect :opts opts})))

  (union [this relations]
    (union this relations nil))

  (union [this relations opts]
    (assoc this :combinations
           (conj (or combinations [])
                 {:table relations :mode :union :opts opts})))

  (rename [this newnames]
    (assoc this :renames (merge (or renames {}) newnames)))

  (aggregate [this aggregates]
    (aggregate this aggregates []))

  (aggregate [this aggregates group-by]
     (let [grps (reduce conj group-by grouped-by)
           table (project this (into grps aggregates))]
      (if (seq grps)
        (grouped table grps)
        table)))

  (conj! [this records]
    (let [return (with-cnx cnx
                   (if (map? records)
                     (conj-rows tname (keys records) (vals records))
                     (->> records
                          (map #(conj-rows tname (keys %) (vals %)))
                          last)))]
      (with-meta this (meta return))))

  (disj! [this predicate]
     (with-cnx cnx
       (delete-rows tname (into [(str predicate)] (:env predicate))))
    this)

  (update-in! [this pred record]
    (let [predicate (into [(str pred)] (:env pred))
          retr      (with-cnx cnx
                      (when *debug* (prn predicate))
                      (update-or-insert-vals tname predicate record))]
      (with-meta this (meta retr))))

  (update! [this pred record]
    (let [predicate (into [(str pred)] (:env pred))
          retr      (with-cnx cnx
                      (when *debug* (prn predicate))
                      (update-vals tname predicate record))]
      (with-meta this (meta retr))))

  (grouped [this field]
    ;TODO: We shouldn't call to-fieldlist here, first in the compiler
    (let [colname (with-meta [(to-fieldlist tname
                                            (map #(if (vector? %) (first %) (identity %))
                                                 field))] {:prepend true})]
      (assoc this :grouped-by
             (conj (or grouped-by [])
                   (if (seq combinations)
                     colname
                     (with-meta colname {:prepend true}))))))

  (limit [this n]
    (if (seq combinations)
      ; Working on the entire statement
      (let [{:keys [limit offset]} scope]
        (assoc this :scope
               {:limit (if limit (min limit n) n)
                :offset offset}))
      ; Working in prepend mode
      (if (number? (:limit pre-scope))
        ; There is already a limit on the table
        (assoc (table cnx tname)
          :tcols     this
          :pre-scope {:limit n, :offset nil})
        ; There is no existing limit
        (let [{:keys [limit offset]} pre-scope]
          (assoc this :pre-scope
                 {:limit (if limit (min limit n) n)
                  :offset offset})))))

  (offset [this n]
    (if (seq combinations)
      ; Working on the entire statement
      (let [limit  (if (:limit scope)  (- (:limit scope)  n))
            offset (if (:offset scope) (+ (:offset scope) n) n)]
        (assoc this
          :scope {:limit limit :offset offset}))
      ; Working in prepend mode
      (let [limit  (if (:limit pre-scope)  (- (:limit pre-scope)  n))
            offset (if (:offset pre-scope) (+ (:offset pre-scope) n) n)]
        (if (some neg? (filter number? [limit offset]))
          (throw (Exception. (format "Limit/Offset cannot have negative values: (limit: %s, offset: %s)"
                                     limit offset)))
          (assoc this
            :pre-scope {:limit limit :offset offset})))))

  (order-by [this fields]
    (let [fields (if (seq combinations)
                   fields
                   (with-meta fields {:prepend true}))]
      (if (and (seq (filter #(true? (-> % meta :prepend)) order-by))
               (not (seq combinations)))
        (assoc (table cnx tname)
          :tcols (assoc this :order-by order-by)
          :order-by fields)
        (assoc this
          :order-by (conj (or order-by [])
                          fields))))))

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

                                        ; HELPERS

(defn interpolate-sql [[stmt & args]]
  "For compilation test purposes only"
  (reduce #(.replaceFirst %1 "\\?" (if (nil? %2) "NULL" (str %2))) stmt args))

(defmethod print-method RTable [tble ^String out]
  "RTables print as SQL92 compliant SQL"
  (when *debug*
    (doseq [[k v] tble]
      (.write out (format "%s\t\t\t\t%s\n" (str k) (str v)))))
  (.write out (-> tble (compile nil) interpolate-sql)))

(defn table
  "Constructs a relational object."
  ([table-name]
     (table nil table-name))
  ([connection-info table-name]
     (let [connection-info (if (fn? connection-info)
                             (connection-info)
                             connection-info)]
       (RTable. connection-info table-name [:*] nil nil nil nil nil nil nil nil nil nil nil))))

(defmacro declare-tables
  "Given a connection info map (or nil) and as list
   of tablenames as keywords a number of tables will
   be (def)ined with identical names of the keywords
   given.

   Ex. (declare-tables db :t1 :t2)
       @t1
       ({....} {...})"
  [conn-info & names]
  `(do
     ~@(for [nm names]
         (list 'def (-> nm name symbol)
               (list 'table conn-info nm)))))

(defn table?
  "Returns true if tinstance is an instnce of RTable"
  [tinstance]
  (instance? clojureql.core.RTable tinstance))

(defn pick [table kw]
  (transform table
             (fn [results]
               (cond
                (= 1 (count results)) (if (coll? kw)
                                        (map (first results) kw)
                                        ((first results) kw))
                (empty? results) nil
                :else (throw (Exception. "Multiple items in resultsetseq, keyword lookup not possible"))))))
