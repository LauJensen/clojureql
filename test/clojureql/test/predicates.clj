(ns clojureql.test.predicates
  (:use clojureql.predicates clojure.test))

(deftest test-compile-expr
  (are [expression result] (= result ((juxt str :env) expression))
       (=* :id 5)
       ["( ? = ? )" ["id" 5]]
       (=* :id nil)
       ["( ? IS ? )" ["id" "NULL"]]
       (=* nil :id)
       ["( ? IS ? )" ["id" "NULL"]]
       (=* nil nil)
       ["TRUE" []]))