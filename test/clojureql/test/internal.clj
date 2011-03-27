(ns clojureql.test.internal
  (:refer-clojure
   :exclude [compile take drop sort distinct conj! disj! case])
  (:use clojure.test
        clojureql.internal))

(deftest test-to-orderlist
  (are [tname aggregates fields expected]
    (is (= expected (to-orderlist tname aggregates fields)))
    "user" [] [] ""
    "user" [] [:id] "user.id ASC"
    "user" [] [:id#asc] "user.id ASC"
    "user" [] [:id#asc :name#desc] "user.id ASC,user.name DESC"
    "continents" [] ["distance(location, ST_GeomFromText('SRID=4326;POINT(0 0)'))"] "distance(location, ST_GeomFromText('SRID=4326;POINT(0 0)')) ASC"))

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
