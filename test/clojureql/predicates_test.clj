(ns clojureql.predicates-test
  (:use clojureql.predicates clojure.test))

(deftest test-compile-expr
  (are [expression result] (= result ((juxt str :env) expression))
       (=* :id 5)
       ["(id = ?)" [5]]
       (=* :id nil)
       ["(id IS ?)" ["NULL"]]
       (=* nil :id)
       ["(id IS ?)" ["NULL"]]
       (=* nil nil)
       ["TRUE" []]))