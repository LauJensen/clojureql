(ns clojureql.predicates
  (:use clojureql.internal
        [clojure.string :only [join] :rename {join join-str}]))

(defn sanitize [expression]
  (reduce #(conj %1 %2) [] (remove keyword? expression)))

(defn parameterize [op expression]
  (str "("
       (->> expression
            (map #(if (keyword? %)
                    (str (to-tablename %))
                    "?"))
            (join-str (str \space (upper-name op) \space)))
       ")"))

(declare predicate)

(defprotocol Predicate
  (sql-or     [this exprs]     "Compiles to (expr OR expr)")
  (sql-and    [this exprs]     "Compiles to (expr AND expr)")
  (sql-not    [this exprs]     "Compiles to NOT(exprs)")
  (spec-op    [this expr]      "Compiles a special, ie. non infix operation")
  (infix      [this op exprs]  "Compiles an infix operation"))

(defrecord APredicate [stmt env]
  Object
  (toString [this] (apply str stmt))
  Predicate
  (sql-or    [this exprs]
    (if (empty? (-> exprs first :stmt))
      (assoc this
        :stmt (map str exprs)
        :env  (mapcat :env exprs))
      (assoc this
        :stmt (conj stmt (str "(" (join-str " OR " exprs) ")"))
        :env  (into env (mapcat :env exprs)))))
  (sql-and   [this exprs]
    (if (empty? (-> exprs first :stmt))
      (assoc this
        :stmt (map str exprs)
        :env  (mapcat :env exprs))
      (assoc this
        :stmt (conj stmt (str "(" (join-str " AND " exprs) ")"))
        :env  (into env (mapcat :env exprs)))))
  (sql-not   [this expr]
    (if (empty? (-> expr first :stmt))
      (assoc this
        :stmt (map str expr)
        :env  (mapcat :env expr))
      (assoc this
        :stmt (conj stmt (str "NOT(" (join-str expr) ")"))
        :env  (into env (mapcat :env expr)))))
  (spec-op [this expr]
    (let [[op p1 p2] expr]
      (cond
       (every? nil? (rest expr))
       (assoc this
         :stmt (conj stmt "(NULL " op " NULL)")
         :env  env)
       (nil? p1)
       (.spec-op this [op p2 p1])
       (nil? p2)
       (assoc this
         :stmt (conj stmt (str "(" (name p1) " " op " NULL)"))
         :env [])
       :else
       (infix this "=" (rest expr)))))
  (infix [this op expr]
    (assoc this
      :stmt (conj stmt (parameterize op expr))
      :env  (into env (sanitize expr)))))

(defn predicate
  ([]         (predicate [] []))
  ([stmt]     (predicate stmt []))
  ([stmt env] (APredicate. stmt env)))

(defn replace-in
  "Helper function to update the env field of a predicate"
  [pred orig new]
  (predicate (str pred)
             (reduce #(if-let [p %2]
                        (if (string? p)
                          (conj %1 (.replaceAll p orig new))
                          (conj %1 p))) [] (:env pred))))

(defn apply-aliases-to-predicate
  "Takes a predicate and a hashmap of aliases and returns
   a statement with all aliases applied"
  [pred aliases]
  (reduce (fn [acc [old new]]
            (replace-in acc old (-> (.split new "\\.") first)))
          pred aliases))

(defn or*  [& args] (sql-or (predicate) args))
(defn and* [& args] (sql-and (predicate) args))
(defn not* [& args] (sql-not (predicate) args))

(defmacro defoperator [name op doc]
  `(defn ~name ~doc [& args#]
     (infix (predicate) (name ~op) args#)))

(defn =* [& args]
  (if (some #(nil? %) args)
    (spec-op (predicate) (into ["IS"] args))
    (infix (predicate) "=" args)))

(defn !=* [& args]
  (if (some #(nil? %) args)
    (spec-op (predicate) (into ["IS NOT"] args))
    (infix (predicate) "!=" args)))

(defoperator like :like "LIKE operator:      (like :x \"%y%\"")
(defoperator >*   :>    "> operator:         (> :x 5)")
(defoperator <*   :<    "< operator:         (< :x 5)")
(defoperator <=*  :<=   "<= operator:        (<= :x 5)")
(defoperator >=*  :>=   ">= operator:        (>= :x 5)")

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
