(ns clojureql.core-test
  (:use clojureql.core
        clojure.test)
  (:refer-clojure
   :exclude [take drop sort conj! disj!]))

(deftest sql-compilation

  (testing "simple selects"
    (are [x y] (= (to-sql x) y)
         (table :users)
         "SELECT users.* FROM users"
         (-> (table :users) (project [:id :name]))
         "SELECT users.id,users.name FROM users"
         (-> (table :users) (aggregate [:avg/wage]))
         "SELECT avg(users.wage) FROM users"
         (-> (table :users) (aggregate [[:avg/wage :as :avg]]))
         "SELECT avg(users.wage) AS avg FROM users"))

  (testing "where predicates"
    (are [x y] (= (to-sql x) y)
         (-> (table :users)
             (select (where (= :id 5)))
             (project [:id]))
         "SELECT users.id FROM users WHERE (id = 5)"
         (-> (table :users)
             (select (where (= :id nil))))
         "SELECT users.* FROM users WHERE (id IS NULL)"
         (-> (table :users)
             (select (where (or (= :id 5) (>= :id 10))))
             (project [:id]))
         "SELECT users.id FROM users WHERE ((id = 5) OR (id >= 10))"
         (-> (table :users)
             (select (where (and (= :id 5) (>= :id 10))))
             (project [:id]))
         "SELECT users.id FROM users WHERE ((id = 5) AND (id >= 10))"
         (-> (table {} :users)
             (select (where (and (= :id 5) (or (>= :id 10)
                                               (<= :id 20)))))
             (project [:id]))
         "SELECT users.id FROM users WHERE ((id = 5) AND ((id >= 10) OR (id <= 20)))"
         (-> (table :users)
             (select (where (and (!= :id 5) (or (> :id 10)
                                                (< :id 20)))))
             (project [:id]))
         "SELECT users.id FROM users WHERE ((id != 5) AND ((id > 10) OR (id < 20)))"))

  (testing "projections"
    (are [x y] (= (to-sql x) y)
         (-> (table :users)
             (project [:id :name :title]))
         "SELECT users.id,users.name,users.title FROM users"))

  (testing "joins"
    (are [x y] (= (to-sql x) y)
         (-> (table :users)
             (join (table :salary) :id)
             (project [:users.id :salary.wage]))
         "SELECT users.id,salary.wage FROM users JOIN salary USING(id)"
         (-> (table :users)
             (join (table :salary) (where (= :users.id :salary.id)))
             (project [:users.id :salary.wage]))
         "SELECT users.id,salary.wage FROM users JOIN salary ON (users.id = salary.id)"))

  (testing "renaming in joins"
    (are [x y] (= (to-sql x) y)
         (-> (table :users)
             (join (table :salary) (where (= :user.id :salary.id)))
             (project [:users.id :salary.wage])
             (rename {:users.id :idx})) ; TODO: This should only work with fully qualified names
         "SELECT users.id,salary.wage FROM users AS users(idx) JOIN salary ON (user.id = salary.id)"))
                                        ; TODO: Shouldn't this be ON (users.idx = salary.id) ?
  (testing "aggregate functions"
    (are [x y] (= (to-sql x) y)
         (-> (table :users)
             (select (where (= :admin true)))
             (aggregate [:count/* :avg/wage]))
         "SELECT count(*),avg(users.wage) FROM users WHERE (admin = true)"
         (-> (table :users)
             (select (where (= :admin true)))
             (aggregate [:count/*, "corr(x,y)"] [:country :city]))
         "SELECT users.country,users.city,count(*),corr(x,y) FROM users WHERE (admin = true) GROUP BY users.country,users.city"
         (-> (table :users)
             (select (where (= :admin true)))
             (aggregate [:count/*, :corr/x:y] [:country :city]))
         "SELECT users.country,users.city,count(*),corr(users.x,users.y) FROM users WHERE (admin = true) GROUP BY users.country,users.city"))

  (testing "join with aggregate"
    (let [photo-counts-by-user (-> (table :photos)
                                   (aggregate [[:count/* :as :cnt]] [:user_id]))]
      (are [x y] (= (to-sql x) y)
           (-> (table :users)
               (join photo-counts-by-user
                     (where (= :users.id :photos.user_id))))
           (str "SELECT users.*,photos_subselect.cnt FROM users JOIN "
                "(SELECT photos.user_id,count(*) AS cnt FROM photos GROUP BY photos.user_id) "
                "AS photos_subselect ON (users.id = photos_subselect.user_id)"))))

  (testing "table aliases"
    (let [u1 (-> (table {:users :u1}) (project [:id :article :price]))
          w1 (table {:salary :w1})]
      (are [x y] (= (to-sql x) y)
           (join u1 w1 (where (= :u1.id :w1.id)))
           "SELECT u1.id,u1.article,u1.price,w1.* FROM users u1 JOIN salary w1 ON (u1.id = w1.id)"
           (-> (join u1 w1 (where (= :u1.id :w1.id)))
               (select (where (= :s2.article nil))))
           (str "SELECT u1.id,u1.article,u1.price,w1.* FROM users u1 "
                "JOIN salary w1 ON (u1.id = w1.id) WHERE (s2.article IS NULL)"))))
  (testing "joining on multiple tables"
    (are [x y] (= (to-sql x) y)
         (-> (table :users)
                    (join (table :wages) :wid)
                    (join (table :commits) :cid))
         "SELECT users.*,wages.*,commits.* FROM users JOIN wages USING(wid) JOIN commits USING(cid)"))
)
