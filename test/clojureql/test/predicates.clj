(ns clojureql.test.predicates
  (:use clojureql.predicates clojure.test))

(deftest test-compile-expr
  (are [expression result]
    (is (= result (compile-expr expression)))
    [:eq :id 5]
    "(id = 5)"
    [:eq :id nil]
    "(id IS NULL)"
    [:eq nil :id]
    "(id IS NULL)"
    [:eq nil nil]
    "TRUE"))

(deftest test-sanitize-expr
  (are [expression result]
    (is (= result (sanitize-expr expression)))
    [:eq :id 5]
    '("id" 5)
    [:eq :id nil]
    '("id" "NULL")
    [:eq nil :id]
    '("NULL" "id")
    [:eq nil nil]
    '("NULL" "NULL")))
