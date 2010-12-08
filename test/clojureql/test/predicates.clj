(ns clojureql.test.predicates
  (:use clojureql.predicates clojure.test))

(deftest test-parameterize
  (are [op expression result] (= result (parameterize op expression))
       :<         [:x 5]      "(x < ?)"
       "<"        [:x 5]      "(x < ?)"
       :>         [:y 6]      "(y > ?)"
       ">"        [:y 6]      "(y > ?)"
       :<=        [:x 7]      "(x <= ?)"
       "<="       [:x 7]      "(x <= ?)"
       :>=        [:y 8]      "(y >= ?)"
       ">="       [:y 9]      "(y >= ?)"
       :like      [:x "joh%"] "(x LIKE ?)"
       "like"     [:x "joh%"] "(x LIKE ?)"))

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
       ["(lower(name) = ?)" ["bob"]]
       (not* nil)
       ["" []]
       (not* (=* :id 5))
       ["NOT((id = ?))" [5]]
       (not* (=* :id 5))
       ["NOT((id = ?))" [5]]
       (not* (like :x "chris%"))
       ["NOT((x LIKE ?))" ["chris%"]]
       (not* (or* (<* :id 100) (>* :id 101)))
       ["NOT(((id < ?) OR (id > ?)))" [100 101]]))
