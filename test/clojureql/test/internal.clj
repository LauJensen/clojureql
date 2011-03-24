(ns clojureql.test.internal
  (:refer-clojure
   :exclude [compile take drop sort distinct conj! disj! case])
  (:use clojure.test
        clojureql.internal))

(deftest test-to-tablename
  (are [tname expected]
    (is (= expected (to-tablename tname)))
    nil nil
    :user "user"
    "user" "user"
    {:user :developer} "user developer"
    {"user" "developer"} "user developer"))

(deftest test-to-tablealias
  (are [tname expected]
    (is (= expected (to-tablealias tname)))
    nil nil
    :user "user"
    "user" "user"
    {:user :developer} "developer"
    {"user" "developer"} "developer"))
