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

(deftest test-table-alias
  (are [tname expected]
       (is (= expected (table-alias tname)))
       :user "user"
       "user" "user"
       {:user :u} "u"))

(deftest test-subselect-table-alias
  (are [tname expected]
       (is (= expected (subselect-table-alias tname)))
       :user "user_subselect"
       "user" "user_subselect"
       {:user :u} "u"))

(deftest test-join-table-alias
  (are [table-name-alias expected]
       (is (= expected (join-table-alias table-name-alias)))
       "user" "user"
       "user u" "u"
       {:tname :user} "user_subselect"))

(deftest test-joins-by-table-alias
  (are [joins expected]
       (is (= expected (joins-by-table-alias joins)))

       ;; simple case : join to an unaliased table
       [{:data ["user" {:cols ["user.company_id" "companies.id"]}]}] 
       {"user" {:data ["user" {:cols ["user.company_id" "companies.id"]}]}}

       ;; join to an aliased table
       [{:data ["user u" {:cols ["u.company_id" "companies.id"]}]}] 
       {"u" {:data ["user u" {:cols ["u.company_id" "companies.id"]}]}}

       ;; join to a subselect
       [{:data [{:tname :user} {:cols ["user.company_id" "companies.id"]}]}] 
       {"user_subselect" {:data [{:tname :user} {:cols ["user.company_id" "companies.id"]}]}}

       ;; join to a subselect with explicit alias
       [{:data [{:tname {:user :u}} {:cols ["u.company_id" "companies.id"]}]}] 
       {"u" {:data [{:tname {:user :u}} {:cols ["u.company_id" "companies.id"]}]}}

       ;; multiple joins
       [{:data ["user" {:cols ["user.company_id" "companies.id"]}]}
        {:data ["hats" {:cols ["hats.id" "user.hat_id"]}]}]
       {"user" {:data ["user" {:cols ["user.company_id" "companies.id"]}]}
        "hats" {:data ["hats" {:cols ["hats.id" "user.hat_id"]}]}}))

(deftest test-join-column-names
  (are [table-name-alias cols expected]
       (is (= expected (join-column-names table-name-alias cols)))
       
       ;; simple case : unaliased table
       "user" ["user.company_id" "companies.id"] ["user.company_id" "companies.id"]
       
       ;; simple case : aliased table
       "user u" ["u.company_id" "companies.id"] ["u.company_id" "companies.id"]
       
       ;; subselect
       {:tname :user} ["user.company_id" "companies.id"] ["user_subselect.company_id" "companies.id"]
       
       ;; subselect with expicit alias
       {:tname {:user :u}} ["u.company_id" "companies.id"] ["u.company_id" "companies.id"]))

(deftest test-to-tbl-name
  (are [join expected]
       (is (= expected (to-tbl-name join)))

       ;; join to an unaliased table
       {:data ["user" {:cols ["user.company_id" "companies.id"]}]} "companies"
       
       ;; join to an aliased table
       {:data ["user u" {:cols ["u.company_id" "companies.id"]}]} "companies"
       
       ;; join to a subselect
       {:data [{:tname :user} {:cols ["user.company_id" "companies.id"]}]} "companies"
       
       ;; join to an explicitly aliased subselect
       {:data [{:tname {:user :u}} {:cols ["u.company_id" "companies.id"]}]} "companies"))

(deftest test-to-graph-el
  (are [m join expected]
       (is (= expected (to-graph-el m join)))

       ;; join to an unaliased table
       {} {:data ["user" {:cols ["user.company_id" "companies.id"]}]} {"user" "companies"}

       ;; join to an aliased table
       {} {:data ["user u" {:cols ["u.company_id" "companies.id"]}]} {"u" "companies"}
       
       ;; join to a subselect
       {} {:data [{:tname :user} {:cols ["user.company_id" "companies.id"]}]} {"user_subselect" "companies"}
       
       ;; join to an explicitly aliased subselect
       {} {:data [{:tname {:user :u}} {:cols ["u.company_id" "companies.id"]}]} {"u" "companies"}))

(deftest test-add-deps
  (are [map-of-joins edges tbl expected]
       (is (= expected (add-deps map-of-joins edges tbl)))
       
       ;; deps for a from a join b join c 
       {"b" :b-join "c" :c-join}
       {"b" "a" "c" "b"}
       "a"
       [nil [:b-join [:c-join]]]
       
       ;; deps for b from a join b join c
       {"b" :b-join "c" :c-join}
       {"b" "a" "c" "b"}
       "b"
       [:b-join [:c-join]]
       
       ;; deps for c from a join b join c
       {"b" :b-join "c" :c-join}
       {"b" "a" "c" "b"}
       "c"
       [:c-join]))