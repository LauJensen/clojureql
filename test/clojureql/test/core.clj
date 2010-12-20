(ns clojureql.test.core
  (:refer-clojure
   :exclude [compile take drop sort conj! disj!])
  (:use [clojureql.internal :only (update-or-insert-vals)]
        clojure.test
        clojureql.core
        clojure.contrib.mock))

(deftest sql-compilation

  (testing "simple selects"
    (are [x y] (= (-> x (compile nil) interpolate-sql) y)
         (table :users)
         "SELECT users.* FROM users"
         (-> (table :users) (project [:id :name]))
         "SELECT users.id,users.name FROM users"
         (-> (table :users) (aggregate [:avg/wage]))
         "SELECT avg(users.wage) FROM users"
         (-> (table :users) (aggregate [[:avg/wage :as :avg]]))
         "SELECT avg(users.wage) AS avg FROM users"))

  (testing "where predicates"
    (are [x y] (= (-> x (compile nil) interpolate-sql) y)
         (-> (table :users)
             (select (where (= :id 5)))
             (project [:id]))
         "SELECT users.id FROM users WHERE (id = 5)"
         (-> (table :users)
             (select (where (= :id nil))))
         "SELECT users.* FROM users WHERE (id IS NULL)"
         (-> (table :users)
             (select (where (!= :id nil))))
         "SELECT users.* FROM users WHERE (id IS NOT NULL)"
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
         "SELECT users.id FROM users WHERE ((id != 5) AND ((id > 10) OR (id < 20)))"
         (-> (table :users)
             (select (where (= :lower/name "bob"))))
         "SELECT users.* FROM users WHERE (lower(name) = bob)"))

  (testing "Nested where predicates"
    (are [x y] (= (-> x (compile nil) interpolate-sql) y)
         (-> (table :users)
             (select (where (= :id 5)))
             (select (where (= :title "Developer"))))
         "SELECT users.* FROM users WHERE (id = 5) AND (title = Developer)"))

  (testing "projections"
    (are [x y] (= (-> x (compile nil) interpolate-sql) y)
         (-> (table :users)
             (project [:id :name :title]))
         "SELECT users.id,users.name,users.title FROM users"))

  (testing "joins"
    (are [x y] (= (-> x (compile nil) interpolate-sql) y)
         (-> (table :users)
             (join (table :salary) :id)
             (project [:users.id :salary.wage]))
         "SELECT users.id,salary.wage FROM users JOIN salary USING(id)"
         (-> (table :users)
             (join (table :salary) (where (= :users.id :salary.id)))
             (project [:users.id :salary.wage]))
         "SELECT users.id,salary.wage FROM users JOIN salary ON (users.id = salary.id)"))

  (testing "renaming in joins"
    (are [x y] (= (-> x (compile nil) interpolate-sql) y)
         (-> (table :users)
             (join (table :salary) (where (= :user.id :salary.id)))
             (project [:users.id :salary.wage])
             (rename {:users.id :idx})) ; TODO: This should only work with fully qualified names
         "SELECT users.id,salary.wage FROM users AS users(idx) JOIN salary ON (user.id = salary.id)"))
                                        ; TODO: Shouldn't this be ON (users.idx = salary.id) ?
  (testing "aggregate functions"
    (are [x y] (= (-> x (compile nil) interpolate-sql) y)
         (-> (table :users)
             (select (where (= :admin true)))
             (aggregate [:count/* :avg/wage]))
         "SELECT count(*),avg(users.wage) FROM users WHERE (admin = true)"
         (-> (table :users)
             (select (where (= :admin true)))
             (aggregate [:count/*, "corr(x,y)"] [:country :city]))
         (str "SELECT users.country,users.city,count(*),corr(x,y) FROM users "
              "WHERE (admin = true) GROUP BY users.country,users.city")
         (-> (table :users)
             (select (where (= :admin true)))
             (aggregate [:count/*, :corr/x:y] [:country :city]))
         (str "SELECT users.country,users.city,count(*),corr(users.x,users.y) FROM users "
              "WHERE (admin = true) GROUP BY users.country,users.city")))

  (testing "join with aggregate"
    (let [photo-counts-by-user (-> (table :photos)
                                   (aggregate [[:count/* :as :cnt]] [:user_id]))]
      (are [x y] (= (-> x (compile nil) interpolate-sql) y)
           (-> (table :users)
               (join photo-counts-by-user
                     (where (= :users.id :photos.user_id))))
           (str "SELECT users.*,photos_subselect.cnt FROM users JOIN "
                "(SELECT photos.user_id,count(*) AS cnt FROM photos GROUP BY photos.user_id) "
                "AS photos_subselect ON (users.id = photos_subselect.user_id)"))))

  (testing "table aliases"
    (let [u1 (-> (table {:users :u1}) (project [:id :article :price]))
          w1 (table {:salary :w1})]
      (are [x y] (= (-> x (compile nil) interpolate-sql) y)
           (join u1 w1 (where (= :u1.id :w1.id)))
           "SELECT u1.id,u1.article,u1.price,w1.* FROM users u1 JOIN salary w1 ON (u1.id = w1.id)"
           (-> (join u1 w1 (where (= :u1.id :w1.id)))
               (select (where (= :s2.article nil))))
           (str "SELECT u1.id,u1.article,u1.price,w1.* FROM users u1 "
                "JOIN salary w1 ON (u1.id = w1.id) WHERE (s2.article IS NULL)")
           (-> u1 (join (project w1 [[:wage :as :income]]) (where (= :u1.id :w1.id))))
           (str "SELECT u1.id,u1.article,u1.price,w1.wage AS income "
                "FROM users u1 JOIN salary w1 ON (u1.id = w1.id)"))))

  (testing "joining on multiple tables"
    (are [x y] (= (-> x (compile nil) interpolate-sql) y)
         (-> (table :users)
             (join (table :wages) :wid)
             (join (table :commits) :cid))
         "SELECT users.*,wages.*,commits.* FROM users JOIN wages USING(wid) JOIN commits USING(cid)"))

  (testing "update-in!"
    (expect [update-or-insert-vals (has-args [:users ["(id = ?)" 1] {:name "Bob"}])]
      (update-in! (table :users) (where (= :id 1)) {:name "Bob"}))
    (expect [update-or-insert-vals (has-args [:users ["(salary IS NULL)"] {:salary 1000}])]
      (update-in! (table :users) (where (= :salary nil)) {:salary 1000})))

  (testing "difference"
    (are [x y] (= (-> x (compile nil) interpolate-sql) y)
         (difference (select (table :users) (where (>= :id 0)))
                (select (table :users) (where (= :id 1))))
         "SELECT users.* FROM users WHERE (id >= 0) EXCEPT SELECT users.* FROM users WHERE (id = 1)"
         (-> (select (table :users) (where (>= :id 0)))
             (difference (select (table :users) (where (= :id 1))))
             (difference (select (table :users) (where (<= :id 2)))))
         "SELECT users.* FROM users WHERE (id >= 0) EXCEPT SELECT users.* FROM users WHERE (id = 1) EXCEPT SELECT users.* FROM users WHERE (id <= 2)"
         (-> (select (table :users) (where (>= :id 0)))
             (difference (select (table :users) (where (= :id 1))) :all)
             (difference (select (table :users) (where (<= :id 2))) :distinct))
         "SELECT users.* FROM users WHERE (id >= 0) EXCEPT ALL SELECT users.* FROM users WHERE (id = 1) EXCEPT DISTINCT SELECT users.* FROM users WHERE (id <= 2)"))

  (testing "intersection"
    (are [x y] (= (-> x (compile nil) interpolate-sql) y)
         (intersection (select (table :users) (where (>= :id 0)))
                (select (table :users) (where (= :id 1))))
         "SELECT users.* FROM users WHERE (id >= 0) INTERSECT SELECT users.* FROM users WHERE (id = 1)"
         (-> (select (table :users) (where (>= :id 0)))
             (intersection (select (table :users) (where (= :id 1))))
             (intersection (select (table :users) (where (<= :id 2)))))
         "SELECT users.* FROM users WHERE (id >= 0) INTERSECT SELECT users.* FROM users WHERE (id = 1) INTERSECT SELECT users.* FROM users WHERE (id <= 2)"
         (-> (select (table :users) (where (>= :id 0)))
             (intersection (select (table :users) (where (= :id 1))) :all)
             (intersection (select (table :users) (where (<= :id 2))) :distinct))
         "SELECT users.* FROM users WHERE (id >= 0) INTERSECT ALL SELECT users.* FROM users WHERE (id = 1) INTERSECT DISTINCT SELECT users.* FROM users WHERE (id <= 2)"))

  (testing "union"
    (are [x y] (= (-> x (compile nil) interpolate-sql) y)
         (union (select (table :users) (where (>= :id 0)))
                (select (table :users) (where (= :id 1))))
         "SELECT users.* FROM users WHERE (id >= 0) UNION SELECT users.* FROM users WHERE (id = 1)"
         (-> (select (table :users) (where (>= :id 0)))
             (union (select (table :users) (where (= :id 1))))
             (union (select (table :users) (where (<= :id 2)))))
         "SELECT users.* FROM users WHERE (id >= 0) UNION SELECT users.* FROM users WHERE (id = 1) UNION SELECT users.* FROM users WHERE (id <= 2)"
         (-> (select (table :users) (where (>= :id 0)))
             (union (select (table :users) (where (= :id 1))) :all)
             (union (select (table :users) (where (<= :id 2))) :distinct))
         "SELECT users.* FROM users WHERE (id >= 0) UNION ALL SELECT users.* FROM users WHERE (id = 1) UNION DISTINCT SELECT users.* FROM users WHERE (id <= 2)"))

  (testing "difference, intersection and union"
    (are [x y] (= (-> x (compile nil) interpolate-sql) y)
         (let [t1 (table :t1) t2 (table :t2)]
           (-> (select t1 (where (= :id 1)))
               (union t2)
               (take 5)))
         "SELECT t1.* FROM t1 WHERE (id = 1) LIMIT 5 UNION SELECT t2.* FROM t2"
         (-> (select (table :users) (where (>= :id 0)))
             (difference (select (table :users) (where (= :id 1))) :all)
             (intersection (select (table :users) (where (= :id 2))))
             (union (select (table :users) (where (<= :id 3))) :distinct))
         "SELECT users.* FROM users WHERE (id >= 0) EXCEPT ALL SELECT users.* FROM users WHERE (id = 1) INTERSECT SELECT users.* FROM users WHERE (id = 2) UNION DISTINCT SELECT users.* FROM users WHERE (id <= 3)"))

  (testing "sort"
    (are [x y] (= (-> x (compile nil) interpolate-sql) y)
         (-> (table :t1)
             (sort "foo"))
         "SELECT t1.* FROM t1 ORDER BY foo asc"
         (-> (table :t1)
             (sort "foo")
             (take 5))
         "SELECT t1.* FROM t1 ORDER BY foo asc LIMIT 5"
         (-> (table :t1)
             (sort "foo")
             (take 5)
             (sort "bar"))
         "SELECT * FROM (SELECT t1.* FROM t1 ORDER BY foo asc LIMIT 5) ORDER BY bar asc"
         (-> (table :t1)
             (sort "foo")
             (drop 10)
             (take 5)
             (sort "bar"))
         "SELECT * FROM (SELECT t1.* FROM t1 ORDER BY foo asc LIMIT 5 OFFSET 10) ORDER BY bar asc")))
