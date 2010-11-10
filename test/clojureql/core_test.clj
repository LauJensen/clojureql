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
         (-> (table {} :users [:avg#wage]) compile)
         "SELECT avg(users.wage) FROM users"
         (-> (table {} :users [[:avg#wage :as :avg]]) compile)
         "SELECT avg(users.wage) AS avg FROM users"))
  (testing "Where predicates"
    (are [x y] (= x y)
         (-> (table {} :users [:id])
                    (select (= {:id 5}))
                    compile)
         "SELECT users.id FROM users WHERE (id = 5)"
         (-> (table {} :users [:id])
             (select (either (= {:id 5}) (>= {:id 10})))
             compile)
         "SELECT users.id FROM users WHERE ((id = 5) OR (id >= 10))"
         (-> (table {} :users [:id])
             (select (both (= {:id 5}) (>= {:id 10})))
             compile)
         "SELECT users.id FROM users WHERE ((id = 5) AND (id >= 10))"
         (-> (table {} :users [:id])
                    (select (both (= {:id 5}) (either (>= {:id 10})
                                                      (<= {:id 20}))))
                    compile)
         "SELECT users.id FROM users WHERE ((id = 5) AND ((id >= 10) OR (id <= 20)))"
         (-> (table {} :users [:id])
                    (select (both (!= {:id 5}) (either (> {:id 10})
                                                       (< {:id 20}))))
                    compile)
         "SELECT users.id FROM users WHERE ((id != 5) AND ((id > 10) OR (id < 20)))"))
  (testing "Projections"
    (are [x y] (= x y)
         (-> (table {} :users [:id])
             (project #{:name :title})
             compile)
         "SELECT users.id,users.name,users.title FROM users"))
  (testing "Joins"
    (are [x y] (= x y)
         (-> (table {} :users [:id])
                    (join (table {} :salary [:wage]) :id)
                    compile)
         "SELECT users.id,salary.wage FROM users JOIN salary USING(id)"
         (-> (table {} :users [:id])
                    (join (table {} :salary [:wage]) (= {:user.id :salary.id}))
                    compile)
         "SELECT users.id,salary.wage FROM users JOIN salary ON (user.id = salary.id)"))
  (testing "Renaming in joins"
    (are [x y] (= x y)
         (-> (table {} :users [:id])
                    (join (table {} :salary [:wage]) (= {:user.id :salary.id}))
                    (rename {:id :idx}) ; TODO: This should only work with fully qualified names
                    compile)
         "SELECT users.id,salary.wage FROM users AS users(idx) JOIN salary ON (user.id = salary.id)"))
  (testing "Aggregate functions"
    (are [x y] (= x y)
         (-> (table {} :users)
             (select (= {:admin true}))
             (aggregate [:count#* :avg#wage])
             compile)
         "SELECT count(users.*),avg(users.wage) FROM users WHERE (admin = true)"
         (-> (table {} :users)
             (select (= {:admin true}))
             (aggregate [:count#*, "corr(x,y)"] [:country :city])
             compile)
         "SELECT users.country,users.city,count(users.*),corr(x,y) FROM users WHERE (admin = true)  GROUP BY users.country,users.city"))
  (testing "Table aliases"
    (are [x y] (= x y)
        (let [u1 (table {} {:users :u1} [:id :article :price])
              w1 (table {} {:salary :w1})]
          (compile (join u1 w1 (= {:u1.id :w1.id}))))
        "SELECT u1.id,u1.article,u1.price FROM users u1 JOIN salary w1 ON (u1.id = w1.id)"
        (let [u1 (table {} {:users :u1} [:id :article :price])
              w1 (table {} {:salary :w1} [])]
          (-> (join u1 w1 (= {:u1.id :w1.id}))
              (select (= {:s2.article "NULL"}))
              compile))
        (str "SELECT u1.id,u1.article,u1.price FROM users u1 "
             "JOIN salary w1 ON (u1.id = w1.id) WHERE (s2.article = 'NULL')")))
)