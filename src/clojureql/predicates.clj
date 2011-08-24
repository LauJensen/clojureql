(ns clojureql.predicates
  (:require [clojure.set :as set] )
  (:use clojureql.internal
        [clojure.string :only [join] :rename {join join-str}]))

(defn sanitize [expression]
  "Returns all values from an expression"
  (reduce #(if (coll? %2)
             (concat %1 %2)
             (concat %1 [%2])) [] (remove keyword? expression)))

(defn parameterize [expression]
  "Replace all values with questionmarks in an expression"
  (map #(if (keyword? %) (str (to-tablename %)) "?") expression))

(declare predicate)

(defprotocol Predicate
  (sql-or     [this exprs]           "Compiles to (expr OR expr)")
  (sql-and    [this exprs]           "Compiles to (expr AND expr)")
  (sql-not    [this exprs]           "Compiles to NOT(exprs)")
  (spec-op    [this expr]            "Compiles a special, ie. non infix operation")
  (infix      [this op exprs]        "Compiles an infix operation")
  (prefix     [this op field exprs]  "Compiles a prefix operation"))

(defrecord APredicate [stmt env cols]
  Object
  (toString [this] (apply str (flatten stmt)))
  Predicate
  (sql-or    [this exprs]
    (if (empty? (-> exprs first :stmt))
      (assoc this
        :stmt (mapcat :stmt exprs)
        :cols (concat (or cols '()) (mapcat :cols exprs))
        :env  (mapcat :env exprs))
      (assoc this
        :stmt (list stmt "(" (interpose " OR " (map :stmt exprs)) ")")
        :cols (concat (or cols '()) (mapcat :cols exprs))
        :env  (concat env (mapcat :env exprs)))))
  (sql-and   [this exprs]
    (if (empty? (-> exprs first :stmt))
      (assoc this
        :stmt (mapcat :stmt exprs)
        :cols (concat (or cols '()) (mapcat :cols exprs))
        :env  (mapcat :env exprs))
      (assoc this
        :stmt (list stmt "(" (interpose " AND " (map :stmt exprs)) ")")
        :cols (concat (or cols '()) (mapcat :cols exprs))
        :env  (concat env (mapcat :env exprs)))))
  (sql-not   [this expr]
    (if (empty? (-> expr first :stmt))
      (assoc this
        :stmt (mapcat :stmt expr)
        :cols (concat (or cols '()) (mapcat :cols expr))
        :env  (mapcat :env expr))
      (assoc this
        :stmt (list stmt "NOT(" (map :stmt expr) ")")
        :cols (concat (or cols '()) (mapcat :cols expr))
        :env  (concat env (mapcat :env expr)))))
  (spec-op [this expr]
    (let [[op p1 p2] expr]
      (cond
       (every? nil? (rest expr))
       (assoc this
         :stmt (list stmt "(NULL " op " NULL)")
         :env  env)
       (nil? p1)
       (.spec-op this [op p2 p1])
       (nil? p2)
       (assoc this
         :stmt (list stmt "(" (name p1) " " op " NULL)")
         :cols (concat (or cols []) (filter keyword? [p1 p2]))
         :env '())
       :else
       (infix this "=" (rest expr)))))
  (infix [this op expr]
    (assoc this
      :stmt (list stmt "(" (interpose (str \space (upper-name op) \space)
                                      (parameterize expr))
                  ")")
      :cols (filter keyword? expr)
      :env  (concat env (sanitize expr))))
  (prefix [this op field expr]
    (assoc this
      :stmt (list stmt
                  (nskeyword field) \space
                  (upper-name op) \space
                  "(" (->> (if (coll? (first expr))
                             (first expr)
                             expr)
                           parameterize
                           (interpose ","))
                  ")")
      :cols (list field)
      :env (concat env (sanitize expr)))))

(defn predicate
  ([]         (predicate [] []))
  ([stmt]     (predicate stmt []))
  ([stmt env] (predicate stmt env nil))
  ([stmt env col]
              (APredicate. stmt env col)))

(defn fuse-predicates
  "Combines two predicates into one using AND"
  [p1 p2]
  (if (and (nil? (:env p1)) (nil? (:stmt p1)))
    p2
    (predicate (list (:stmt p1) " AND " (:stmt p2))
               (mapcat :env [p1 p2])
               (mapcat :cols [p1 p2]))))

(defn qualify-predicate
  [this pred]
  (let [tname (to-tablename (:tname this))
        {:keys [stmt env cols]} pred
        aggregates (set (map nskeyword (find-aggregates this)))
        colnames (set (remove #(.contains % ".")
                              (map nskeyword cols)))
        qualify? (set/difference colnames aggregates)]
    (predicate
     (map #(if (qualify? %) (str (to-tablealias (:tname this))
                                 \. %) %)
          (if (string? pred)
            [pred]
            (flatten stmt)))
     env
     cols)))

(defn or*  [& args] (sql-or (predicate) args))
(defn and* [& args] (sql-and (predicate) args))
(defn not* [& args] (sql-not (predicate) args))

(defn =* [& args]
  (if (some #(nil? %) args)
    (spec-op (predicate) (into ["IS"] args))
    (infix (predicate) "=" args)))

(defn !=* [& args]
  (if (some #(nil? %) args)
    (spec-op (predicate) (into ["IS NOT"] args))
    (infix (predicate) "!=" args)))

(defn nil?* [field]
  (=* nil field))

(defmacro definfixoperator [name op doc]
  `(defn ~name ~doc [& args#]
     (infix (predicate) (name ~op) args#)))

(definfixoperator like :like "LIKE operator:      (like :x \"%y%\"")
(definfixoperator >*   :>    "> operator:         (> :x 5)")
(definfixoperator <*   :<    "< operator:         (< :x 5)")
(definfixoperator <=*  :<=   "<= operator:        (<= :x 5)")
(definfixoperator >=*  :>=   ">= operator:        (>= :x 5)")

(defmacro defprefixoperator [name op doc]
  `(defn ~name ~doc [field# & args#]
     (prefix (predicate) (name ~op) field# args#)))

(defprefixoperator in :in
  "IN operator:  (in :name \"Jack\" \"John\"). Accepts both
   a vector of items or an arbitrary amount of values as seen
   above.")

(defn restrict
  "Returns a query string.

   Takes a raw string with params as %1 %2 %n.

   (restrict 'id=%1 OR id < %2' 15 10) => 'id=15 OR id < 10'"
  [pred & args]
  (apply sql-clause pred args))

(defn restrict-not
  "The inverse of the restrict fn"
  ([ast]         (into [(str "NOT(" ast ")")] (:env ast)))
  ([pred & args] (str "NOT(" (apply sql-clause pred args) ")")))
