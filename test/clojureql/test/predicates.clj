(ns clojureql.test.predicates
  (:use clojureql.predicates clojure.test))

(deftest test-parameterize
  (are [op expression result] (= result (parameterize op expression))
       :< '(:x 5) "(x < ?)"
       "<" '(:x 5) "(x < ?)"
       :> '(:y 6) "(y > ?)"
       ">" '(:y 6) "(y > ?)"
       :<= '(:x 7) "(x <= ?)"
       "<=" '(:x 7) "(x <= ?)"
       :>= '(:y 8) "(y >= ?)"
       ">=" '(:y 9) "(y >= ?)"
       :like '(:x "foo%") "(x like ?)"
       "like" '(:x "foo%") "(x like ?)"
       "not like" '(:y "bar%") "(y not like ?)"))

(deftest test-compile-expr
  (are [expression result] (= result ((juxt str :env) expression))
       (=* :id 5)
       ["(id = ?)" [5]]
       (=* :id nil)
       ["(id IS NULL)" []]
       (=* nil :id)
       ["(id IS NULL)" []]
       (=* nil nil)
       ["(NULL IS NULL)" []]
       (!=* :id nil)
       ["(id IS NOT NULL)" []]
       (=* :lower/name "bob")
       ["(lower(name) = ?)" ["bob"]]))
