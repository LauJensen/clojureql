(ns clojureql.core-test
  (:use [clojureql core predicates]
        clojure.test))


(deftest sql-compilation
  (testing "Simple selects"
    (are [x y] (= x y)
         (-> (table {} :users [:*]) compile)
         "SELECT users.* FROM users"
         (-> (table {} :users [:id :name]) compile)
         "SELECT users.id,users.name FROM users"
         (-> (table {} :users [:avg:wage]) compile)
         "SELECT avg(users.wage) FROM users"
         (-> (table {} :users [[:avg:wage :as :avg]]) compile)
         "SELECT avg(users.wage) AS avg FROM users"))
  (testing "Where predicates"
    (are [x y] (= x y)
         (-> (table {} :users [:id])
                    (select (= {:id 5}))
                    compile)
         "SELECT users.id FROM users  WHERE (id = 5)"
         (-> (table {} :users [:id])
             (select (either (= {:id 5}) (>= {:id 10})))
             compile)
         "SELECT users.id FROM users  WHERE ((id = 5) OR (id >= 10))"
         (-> (table {} :users [:id])
             (select (both (= {:id 5}) (>= {:id 10})))
             compile)
         "SELECT users.id FROM users  WHERE ((id = 5) AND (id >= 10))"
         (-> (table {} :users [:id])
                    (select (both (= {:id 5}) (either (>= {:id 10})
                                                      (<= {:id 20}))))
                    compile)
         "SELECT users.id FROM users  WHERE ((id = 5) AND ((id >= 10) OR (id <= 20)))"
         (-> (table {} :users [:id])
                    (select (both (!= {:id 5}) (either (> {:id 10})
                                                       (< {:id 20}))))
                    compile)
         "SELECT users.id FROM users  WHERE ((id != 5) AND ((id > 10) OR (id < 20)))"))
  (testing "Projections"
    (are [x y] (= x y)
         (-> (table {} :users [:id])
             (project #{:name :title})
             compile)
         "SELECT users.title,users.name,users.id FROM users"))
  (testing "Joins"
    (are [x y] (= x y)
         (-> (table {} :users [:id])
                    (join (table {} :salary [:wage]) :id)
                    compile)
         "SELECT salary.wage,users.id FROM users JOIN salary USING(id)"
         (-> (table {} :users [:id])
                    (join (table {} :salary [:wage]) (= {:user.id :salary.id}))
                    compile)
         "SELECT salary.wage,users.id FROM users JOIN salary ON (user.id = salary.id)"))
  (testing "Renaming in joins"
    (are [x y] (= x y)
         (-> (table {} :users [:id])
                    (join (table {} :salary [:wage]) (= {:user.id :salary.id}))
                    (rename {:id :idx}) ; TODO: This should only work with fully qualified names
                    compile)
         "SELECT salary.wage,users.id FROM users AS users(idx) JOIN salary ON (user.id = salary.id)"))


)