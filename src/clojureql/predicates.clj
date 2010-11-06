(ns clojureql.predicates
  (:refer-clojure :exclude [take sort conj! disj! < <= > >= =]
                 :rename {take take-coll})
  (:use clojureql.internal
        [clojure.string :only [join] :rename {join join-str}]))

                                        ; PREDICATE COMPILER

(defn compile-expr
  [expr]
  (letfn [(lefthand [e] (-> e last keys first to-name))
          (righthand [e] (let [retr (-> e last vals first)]
                           (if (string? retr)
                             (str "'" retr "'")
                             retr)))]
    (case (first expr)
          :or  (str "(" (join-str " OR "  (map compile-expr (rest expr))) ")")
          :and (str "(" (join-str " AND " (map compile-expr (rest expr))) ")")
          :eq  (str "(" (lefthand expr) " = " (righthand expr) ")")
          :gt  (str "(" (lefthand expr) " > " (righthand expr) ")")
          :lt  (str "(" (lefthand expr) " < " (righthand expr) ")")
          :gt= (str "(" (lefthand expr) " >= " (righthand expr) ")")
          :lt= (str "(" (lefthand expr) " <= " (righthand expr) ")")
          :!=  (str "(" (lefthand expr) " != " (righthand expr) ")")
          (str expr))))

(defn either
  " CQL version of OR.

    (either (= {:a 5)) (>= {:a 10})) means either a = 5 or a >= 10 "
  [& conds]
  (compile-expr (apply vector :or conds)))

(defn both
  " CQL version of AND.

    (both (= {:a 5}) (>= {:b 10})) means a=5 AND b >= 10 "
  [& conds]
  (compile-expr (apply vector :and conds)))

(defn =
  " Alpha - Subject to sanity.

    The idea is, that if this doesn't get passed a map, it assumes
    you want Clojures regular = operator "
  [& args]
  (if (map? (first args))
    (compile-expr (apply vector :eq args))
    (apply clojure.core/= args)))

(defn !=
  " Same as not= "
  [& args]
  (if (map? (first args))
    (compile-expr (apply vector :!= args))
    (apply clojure.core/not= args)))

(defn >
  " Alpha - Subject to sanity.

    The idea is, that if this doesn't get passed a map, it assumes
    you want Clojures regular > operator "
  [& args]
  (if (map? (first args))
    (compile-expr (apply vector :gt args))
    (apply clojure.core/> args)))

(defn <
  " Alpha - Subject to sanity.

    The idea is, that if this doesn't get passed a map, it assumes
    you want Clojures regular < operator "
  [& args]
  (if (map? (first args))
    (compile-expr (apply vector :lt args))
    (apply clojure.core/< args)))

(defn <=
  " Alpha - Subject to sanity.

    The idea is, that if this doesn't get passed a map, it assumes
    you want Clojures regular <= operator "
  [& args]
  (if (map? (first args))
    (compile-expr (apply vector :lt= args))
    (apply clojure.core/<= args)))

(defn >=
  " Alpha - Subject to sanity.

    The idea is, that if this doesn't get passed a map, it assumes
    you want Clojures regular >= operator "
  [& args]
  (if (map? (first args))
    (compile-expr (apply vector :gt= args))
    (apply clojure.core/>= args)))

(defn where
  "Returns a query string. Can take a raw string with params as %1 %2 %n
   or an AST which compiles using compile-expr.

   (where 'id=%1 OR id < %2' 15 10) => 'WHERE id=15 OR id < 10'

   (where (either (= {:id 5}) (>= {:id 10})))
      'WHERE (id=5 OR id>=10)' "
  ([ast]         (str "WHERE "  (compile-expr ast)))
  ([pred & args] (str "WHERE "  (apply sql-clause pred args))))

(defn where-not
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
