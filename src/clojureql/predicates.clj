(ns clojureql.predicates
  (:use clojureql.internal
        clojure.walk
        [clojure.string :only [join] :rename {join join-str}]))

                                        ; PREDICATE COMPILER

(defn compile-expr
  [expr]
  (letfn [(sanitize [e] (->> (rest e)
                             (map #(cond (keyword? %)     (name %)
                                         (and (string? %) (.contains % "(")) %
                                         (string? %)      (str "'" % "'")
                                         :else %))))]
    (case (first expr)
          :or  (str "(" (join-str " OR "  (map compile-expr (rest expr))) ")")
          :and (str "(" (join-str " AND " (map compile-expr (rest expr))) ")")
          :eq  (str "(" (join-str " = " (sanitize expr)) ")")
          :gt  (str "(" (join-str " > " (sanitize expr)) ")")
          :lt  (str "(" (join-str " < " (sanitize expr)) ")")
          :gt= (str "(" (join-str " >= " (sanitize expr)) ")")
          :lt= (str "(" (join-str " <= " (sanitize expr)) ")")
          :!=  (str "(" (join-str " != " (sanitize expr)) ")")
          (str expr))))

(defn or*
  " CQL version of OR.

    (where (or (= :id 5) (:title 'Dev'))) => '((id = 5) OR (title = 'Dev')'"
  [& conds]
  (compile-expr (apply vector :or conds)))

(defn and*
  " CQL version of AND.

    (where (and (< :wage 500) (>= :wage 1500))) => '((wage < 500) AND (wage >= 1500))'"
  [& conds]
  (compile-expr (apply vector :and conds)))

(defn =*
  [& args]
  (compile-expr (apply vector :eq args)))

(defn !=*
  " Same as not= "
  [& args]
  (compile-expr (apply vector :!= args)))

(defn >*
  [& args]
  (compile-expr (apply vector :gt args)))

(defn <*
  [& args]
  (compile-expr (apply vector :lt args)))

(defn <=*
  [& args]
  (compile-expr (apply vector :lt= args)))

(defn >=*
  [& args]
  (compile-expr (apply vector :gt= args)))

(defn where* [clause]
  (-> (postwalk-replace '{=   clojureql.predicates/=*
                          !=  clojureql.predicates/!=*
                          <   clojureql.predicates/<*
                          >   clojureql.predicates/>*
                          <=  clojureql.predicates/<=*
                          >=  clojureql.predicates/>=*
                          and clojureql.predicates/and*
                          or  clojureql.predicates/or*} clause)
      eval))

(defn restrict
  "Returns a query string. Can take a raw string with params as %1 %2 %n
   or an AST which compiles using compile-expr.

   (where 'id=%1 OR id < %2' 15 10) => 'WHERE id=15 OR id < 10'

   (where (either (= {:id 5}) (>= {:id 10})))
      'WHERE (id=5 OR id>=10)' "
  ([ast]         (str "WHERE "  (compile-expr ast)))
  ([pred & args] (str "WHERE "  (apply sql-clause pred args))))

(defn restrict-not
  "The inverse of the where fn"
  ([ast]         (str "WHERE not(" (compile-expr ast) ")"))
  ([pred & args] (str "WHERE not(" (apply sql-clause pred args) ")")))

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
