(ns clojureql.core-test
  (:use clojureql.core
        clojure.test)
  (:refer-clojure
   :exclude [take drop group-by sort conj! disj!]))

(deftest sql-compilation
  (testing "Simple selects"
    (are [x y] (= x y)
         (-> (table {} :users) to-sql)
         "SELECT users.* FROM users"
         (-> (table {} :users) (project [:id :name]) to-sql)
         "SELECT users.id,users.name FROM users"
         (-> (table {} :users) (aggregate [:avg/wage]) to-sql)
         "SELECT avg(users.wage) FROM users"
         (-> (table {} :users) (aggregate [[:avg/wage :as :avg]]) to-sql)
         "SELECT avg(users.wage) AS avg FROM users"))
  (testing "Where predicates"
    (are [x y] (= x y)
         (-> (table {} :users)
             (select (where (= :id 5)))
             (project [:id])
             to-sql)
         "SELECT users.id FROM users WHERE (id = 5)"
         (-> (table {} :users)
             (select (where (or (= :id 5) (>= :id 10))))
             (project [:id])
             to-sql)
         "SELECT users.id FROM users WHERE ((id = 5) OR (id >= 10))"
         (-> (table {} :users)
             (select (where (and (= :id 5) (>= :id 10))))
             (project [:id])
             to-sql)
         "SELECT users.id FROM users WHERE ((id = 5) AND (id >= 10))"
         (-> (table {} :users)
             (select (where (and (= :id 5) (or (>= :id 10)
                                               (<= :id 20)))))
             (project [:id])
             to-sql)
         "SELECT users.id FROM users WHERE ((id = 5) AND ((id >= 10) OR (id <= 20)))"
         (-> (table {} :users)
             (select (where (and (!= :id 5) (or (> :id 10)
                                                (< :id 20)))))
             (project [:id])
             to-sql)
         "SELECT users.id FROM users WHERE ((id != 5) AND ((id > 10) OR (id < 20)))"))
  (testing "Projections"
    (are [x y] (= x y)
         (-> (table {} :users)
             (project [:id :name :title])
             to-sql)
         "SELECT users.id,users.name,users.title FROM users"))
  (testing "Joins"
    (are [x y] (= x y)
         (-> (table {} :users)
             (join (table {} :salary) :id)
             (project [:users.id :salary.wage])
             to-sql)
         "SELECT users.id,salary.wage FROM users JOIN salary USING(id)"
         (-> (table {} :users)
             (join (table {} :salary) (where (= :user.id :salary.id)))
             (project [:users.id :salary.wage])
             to-sql)
         "SELECT users.id,salary.wage FROM users JOIN salary ON (user.id = salary.id)"))
  (testing "Renaming in joins"
    (are [x y] (= x y)
         (-> (table {} :users)
             (join (table {} :salary) (where (= :user.id :salary.id)))
             (project [:users.id :salary.wage])
             (rename {:users.id :idx}) ; TODO: This should only work with fully qualified names
             to-sql)
         "SELECT users.id,salary.wage FROM users AS users(idx) JOIN salary ON (user.id = salary.id)"))
                                        ; TODO: Shouldn't this be ON (users.idx = salary.id) ?
  (testing "Aggregate functions"
    (are [x y] (= x y)
         (-> (table {} :users)
             (select (where (= :admin true)))
             (aggregate [:count/* :avg/wage])
             to-sql)
         "SELECT count(*),avg(users.wage) FROM users WHERE (admin = true)"
         (-> (table {} :users)
             (select (where (= :admin true)))
             (aggregate [:count/*, "corr(x,y)"] [:country :city])
             to-sql)
         "SELECT users.country,users.city,count(*),corr(x,y) FROM users WHERE (admin = true) GROUP BY users.country,users.city"
         (-> (table {} :users)
             (select (where (= :admin true)))
             (aggregate [:count/*, :corr/x:y] [:country :city])
             to-sql)
         "SELECT users.country,users.city,count(*),corr(users.x,users.y) FROM users WHERE (admin = true) GROUP BY users.country,users.city"))
  (testing "Table aliases"
    (let [u1 (-> (table {} {:users :u1}) (project [:id :article :price]))
          w1 (table {} {:salary :w1})]
      (are [x y] (= x y)
           (to-sql (join u1 w1 (where (= :u1.id :w1.id))))
           "SELECT u1.id,u1.article,u1.price,w1.* FROM users u1 JOIN salary w1 ON (u1.id = w1.id)"
           (-> (join u1 w1 (where (= :u1.id :w1.id)))
               (select (where (= :s2.article :NULL)))
               to-sql)
           (str "SELECT u1.id,u1.article,u1.price,w1.* FROM users u1 "
                "JOIN salary w1 ON (u1.id = w1.id) WHERE (s2.article = NULL)"))))
)