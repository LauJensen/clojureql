(ns clojureql.test.core
  (:refer-clojure
   :exclude [compile take drop sort distinct conj! disj!])
  (:use [clojureql.internal :only (update-or-insert-vals)]
        clojure.test
        clojureql.core
        clojure.contrib.mock))

(def select-country-ids-with-spot-count
  (-> (table :spots)
      (aggregate [[:count/id :as :spots]] [:country_id])))

(def select-country-ids-with-region-count
  (-> (table :regions)
      (aggregate [[:count/id :as :regions]] [:country_id])))

(def select-countries-with-region-and-spot-count
  (-> (table :countries)
      (outer-join select-country-ids-with-region-count :left
                  (where (= :countries.id :regions.country_id)))
      (outer-join select-country-ids-with-spot-count :left
                  (where (= :countries.id :spots.country_id)))
      (select (where (= :regions_subselect.country_id :spots_subselect.country_id)))))

(defn select-location [table-name]
  (-> (table table-name)
      (select (where (!= :location nil)))
      (project [:location])))

(defn select-location-union []
  (-> (select-location :continents)
      (union (select-location :countries))
      (union (select-location :regions))
      (union (select-location :spots))))

(defn select-location-union-without-address []
  (-> (outer-join (table :addresses) (select-location-union) :right
                  (where (= :addresses.location :continents.location)))
      (select (where (= :addresses.location nil)))
      (project [:continents_subselect.location])))

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

  (testing "modifiers"
    (are [x y] (= (-> x (compile nil) interpolate-sql) y)
         (-> (table :users) distinct)
         "SELECT DISTINCT users.* FROM users"
         (-> (table :users) (modify [:high_priority :distinct]))
         "SELECT HIGH_PRIORITY DISTINCT users.* FROM users"))

  (testing "where predicates"
    (are [x y] (= (-> x (compile nil) interpolate-sql) y)
         (-> (table :users)
             (select (where (= :id 5)))
             (project [:id]))
         "SELECT users.id FROM users WHERE (users.id = 5)"
         (-> (table :users)
             (select (where (= :id nil))))
         "SELECT users.* FROM users WHERE (users.id IS NULL)"
         (-> (table :users)
             (select (where (!= :id nil))))
         "SELECT users.* FROM users WHERE (users.id IS NOT NULL)"
         (-> (table :users)
             (select (where (or (= :id 5) (>= :id 10))))
             (project [:id]))
         "SELECT users.id FROM users WHERE ((users.id = 5) OR (users.id >= 10))"
         (-> (table :users)
             (select (where (and (= :id 5) (>= :id 10))))
             (project [:id]))
         "SELECT users.id FROM users WHERE ((users.id = 5) AND (users.id >= 10))"
         (-> (table {} :users)
             (select (where (and (= :id 5) (or (>= :id 10)
                                               (<= :id 20)))))
             (project [:id]))
         "SELECT users.id FROM users WHERE ((users.id = 5) AND ((users.id >= 10) OR (users.id <= 20)))"
         (-> (table :users)
             (select (where (and (!= :id 5) (or (> :id 10)
                                                (< :id 20)))))
             (project [:id]))
         "SELECT users.id FROM users WHERE ((users.id != 5) AND ((users.id > 10) OR (users.id < 20)))"
         (-> (table :users)
             (select (where (= :lower/name "bob"))))
         "SELECT users.* FROM users WHERE (lower(name) = bob)"))

  (testing "Nested where predicates"
    (are [x y] (= (-> x (compile nil) interpolate-sql) y)
         (-> (table :users)
             (select (where (= :id 5)))
             (select (where (= :title "Developer"))))
         "SELECT users.* FROM users WHERE (users.id = 5) AND (users.title = Developer)"))

  (testing "projections"
    (are [x y] (= (-> x (compile nil) interpolate-sql) y)
         (-> (table :users)
             (project [:id :name :title]))
         "SELECT users.id,users.name,users.title FROM users"))

  (testing "projection from aliased column on join table"
    (are [x y] (= (-> x (compile nil) interpolate-sql) y)
	 (-> (table :users)
	     (join (table :salary) (where (= :users.id :salary.id)))
	     (project [:salary.wage :as :something]))
	 "SELECT salary.wage AS something FROM users JOIN salary ON (users.id = salary.id)"))

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
             (join (table :salary) (where (= :users.id :salary.id)))
             (project [:users.id :salary.wage])
             (rename {:users.id :idx})) ; TODO: This should only work with fully qualified names
         "SELECT users.id,salary.wage FROM users AS users(idx) JOIN salary ON (users.id = salary.id)"))
                                        ; TODO: Shouldn't this be ON (users.idx = salary.id) ?
  (testing "aggregate functions"
    (are [x y] (= (-> x (compile nil) interpolate-sql) y)
         (-> (table :users)
             (select (where (= :admin true)))
             (aggregate [:count/* :avg/wage]))
         "SELECT count(*),avg(users.wage) FROM users WHERE (users.admin = true)"
         (-> (table :users)
             (select (where (= :admin true)))
             (aggregate [:count/*, "corr(x,y)"] [:country :city]))
         (str "SELECT users.country,users.city,count(*),corr(x,y) FROM users "
              "WHERE (users.admin = true) GROUP BY users.country,users.city")
         (-> (table :users)
             (select (where (= :admin true)))
             (aggregate [:count/*, :corr/x:y] [:country :city]))
         (str "SELECT users.country,users.city,count(*),corr(users.x,users.y) FROM users "
              "WHERE (users.admin = true) GROUP BY users.country,users.city")
         select-country-ids-with-region-count
         (str "SELECT regions.country_id,count(regions.id) AS regions FROM regions GROUP BY regions.country_id")
         select-country-ids-with-spot-count
         (str "SELECT spots.country_id,count(spots.id) AS spots FROM spots GROUP BY spots.country_id")))

  (testing "join with aggregate"
    (let [photo-counts-by-user (-> (table :photos)
                                   (aggregate [[:count/* :as :cnt]] [:user_id]))]
      (are [x y] (= (-> x (compile nil) interpolate-sql) y)
           (-> (table :users)
               (join photo-counts-by-user
                     (where (= :users.id :photos.user_id))))
           (str "SELECT users.*,photos_subselect.user_id,photos_subselect.cnt FROM users JOIN "
                "(SELECT photos.user_id,count(*) AS cnt FROM photos GROUP BY photos.user_id) "
                "AS photos_subselect ON (users.id = photos_subselect.user_id)")))
    (let [photo-counts-by-user (-> (table :photos)
                                   (aggregate [[:count/* :as :cnt] [:sum/* :as :sum]]
                                              [:user_id]))]
      (are [x y] (= (-> x (compile nil) interpolate-sql) y)
           (-> (table :users)
               (join photo-counts-by-user
                     (where (= :users.id :photos.user_id))))
           (str "SELECT users.*,photos_subselect.user_id,photos_subselect.cnt,photos_subselect.sum FROM users JOIN "
                "(SELECT photos.user_id,count(*) AS cnt,sum(*) AS sum FROM photos "
                "GROUP BY photos.user_id) "
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
         "SELECT users.*,wages.*,commits.* FROM users JOIN wages USING(wid) JOIN commits USING(cid)"
         select-countries-with-region-and-spot-count
         (str "SELECT countries.*,regions_subselect.country_id,spots_subselect.country_id,regions_subselect.regions,spots_subselect.spots FROM countries "
              "LEFT OUTER JOIN (SELECT regions.country_id,count(regions.id) AS regions FROM regions GROUP BY regions.country_id) AS regions_subselect "
              "ON (countries.id = regions_subselect.country_id) "
              "LEFT OUTER JOIN (SELECT spots.country_id,count(spots.id) AS spots FROM spots GROUP BY spots.country_id) AS spots_subselect "
              "ON (countries.id = spots_subselect.country_id) "
              "WHERE (regions_subselect.country_id = spots_subselect.country_id)")))

  (testing "update-in!"
    (expect [update-or-insert-vals (has-args [:users ["(id = ?)" 1] {:name "Bob"}])]
      (update-in! (table :users) (where (= :id 1)) {:name "Bob"}))
    (expect [update-or-insert-vals (has-args [:users ["(salary IS NULL)"] {:salary 1000}])]
      (update-in! (table :users) (where (= :salary nil)) {:salary 1000})))

  (testing "difference"
    (are [x y] (= (-> x (compile nil) interpolate-sql) y)
         (difference (select (table :users) (where (>= :id 0)))
                (select (table :users) (where (= :id 1))))
         "(SELECT users.* FROM users WHERE (users.id >= 0)) EXCEPT (SELECT users.* FROM users WHERE (users.id = 1))"
         (-> (select (table :users) (where (>= :id 0)))
             (difference (select (table :users) (where (= :id 1))))
             (difference (select (table :users) (where (<= :id 2)))))
         "(SELECT users.* FROM users WHERE (users.id >= 0)) EXCEPT (SELECT users.* FROM users WHERE (users.id = 1)) EXCEPT (SELECT users.* FROM users WHERE (users.id <= 2))"
         (-> (select (table :users) (where (>= :id 0)))
             (difference (select (table :users) (where (= :id 1))) :all)
             (difference (select (table :users) (where (<= :id 2))) :distinct))
         "(SELECT users.* FROM users WHERE (users.id >= 0)) EXCEPT ALL (SELECT users.* FROM users WHERE (users.id = 1)) EXCEPT DISTINCT (SELECT users.* FROM users WHERE (users.id <= 2))"))

  (testing "intersection"
    (are [x y] (= (-> x (compile nil) interpolate-sql) y)
         (intersection (select (table :users) (where (>= :id 0)))
                (select (table :users) (where (= :id 1))))
         "(SELECT users.* FROM users WHERE (users.id >= 0)) INTERSECT (SELECT users.* FROM users WHERE (users.id = 1))"
         (-> (select (table :users) (where (>= :id 0)))
             (intersection (select (table :users) (where (= :id 1))))
             (intersection (select (table :users) (where (<= :id 2)))))
         "(SELECT users.* FROM users WHERE (users.id >= 0)) INTERSECT (SELECT users.* FROM users WHERE (users.id = 1)) INTERSECT (SELECT users.* FROM users WHERE (users.id <= 2))"
         (-> (select (table :users) (where (>= :id 0)))
             (intersection (select (table :users) (where (= :id 1))) :all)
             (intersection (select (table :users) (where (<= :id 2))) :distinct))
         "(SELECT users.* FROM users WHERE (users.id >= 0)) INTERSECT ALL (SELECT users.* FROM users WHERE (users.id = 1)) INTERSECT DISTINCT (SELECT users.* FROM users WHERE (users.id <= 2))"))

  (testing "union"
    (are [x y] (= (-> x (compile nil) interpolate-sql) y)
         (union (select (table :users) (where (>= :id 0)))
                (select (table :users) (where (= :id 1))))
         "(SELECT users.* FROM users WHERE (users.id >= 0)) UNION (SELECT users.* FROM users WHERE (users.id = 1))"
         (-> (select (table :users) (where (>= :id 0)))
             (union (select (table :users) (where (= :id 1))))
             (union (select (table :users) (where (<= :id 2)))))
         "(SELECT users.* FROM users WHERE (users.id >= 0)) UNION (SELECT users.* FROM users WHERE (users.id = 1)) UNION (SELECT users.* FROM users WHERE (users.id <= 2))"
         (-> (select (table :users) (where (>= :id 0)))
             (union (select (table :users) (where (= :id 1))) :all)
             (union (select (table :users) (where (<= :id 2))) :distinct))
         "(SELECT users.* FROM users WHERE (users.id >= 0)) UNION ALL (SELECT users.* FROM users WHERE (users.id = 1)) UNION DISTINCT (SELECT users.* FROM users WHERE (users.id <= 2))"
         (select-location-union)
         "(SELECT continents.location FROM continents WHERE (continents.location IS NOT NULL)) UNION (SELECT countries.location FROM countries WHERE (countries.location IS NOT NULL)) UNION (SELECT regions.location FROM regions WHERE (regions.location IS NOT NULL)) UNION (SELECT spots.location FROM spots WHERE (spots.location IS NOT NULL))"
         (select-location-union-without-address)
         "SELECT continents_subselect.location,continents_subselect.location FROM addresses RIGHT OUTER JOIN ((SELECT continents.location FROM continents WHERE (continents.location IS NOT NULL)) UNION (SELECT countries.location FROM countries WHERE (countries.location IS NOT NULL)) UNION (SELECT regions.location FROM regions WHERE (regions.location IS NOT NULL)) UNION (SELECT spots.location FROM spots WHERE (spots.location IS NOT NULL))) AS continents_subselect ON (addresses.location = continents_subselect.location) WHERE (addresses.location IS NULL)"))

  (testing "difference, intersection and union"
    (are [x y] (= (-> x (compile nil) interpolate-sql) y)
         (let [t1 (table :t1) t2 (table :t2)]
           (-> (select t1 (where (= :id 1)))
               (union t2)
               (take 5)))
         "(SELECT t1.* FROM t1 WHERE (t1.id = 1)) UNION (SELECT t2.* FROM t2) LIMIT 5"
         (-> (select (table :users) (where (>= :id 0)))
             (difference (select (table :users) (where (= :id 1))) :all)
             (intersection (select (table :users) (where (= :id 2))))
             (union (select (table :users) (where (<= :id 3))) :distinct))
         "(SELECT users.* FROM users WHERE (users.id >= 0)) EXCEPT ALL (SELECT users.* FROM users WHERE (users.id = 1)) INTERSECT (SELECT users.* FROM users WHERE (users.id = 2)) UNION DISTINCT (SELECT users.* FROM users WHERE (users.id <= 3))"))

  (testing "sort"
    (are [x y] (= (-> x (compile nil) interpolate-sql) y)
         (-> (table :t1)
             (sort [:id]))
         "SELECT t1.* FROM t1 ORDER BY id asc"
         (-> (table :t1)
             (sort [:id])
             (take 5))
         "SELECT t1.* FROM t1 ORDER BY id asc LIMIT 5"
         (-> (table :t1)
             (sort [:id])
             (take 5)
             (sort [:wage]))
         "SELECT * FROM (SELECT t1.* FROM t1 ORDER BY id asc LIMIT 5) ORDER BY wage asc"
         (-> (table :t1)
             (sort [:id])
             (drop 10)
             (take 5)
             (sort [:wage]))
         "SELECT * FROM (SELECT t1.* FROM t1 ORDER BY id asc LIMIT 5 OFFSET 10) ORDER BY wage asc"))

  (testing "combinations with sort/limit"
    (are [x y] (= (-> x (compile nil) interpolate-sql) y)
         (-> (union (table :t1) (table :t2))
             (sort [:t2.id]))
         "(SELECT t1.* FROM t1 ) UNION (SELECT t2.* FROM t2) ORDER BY t2.id asc"
         (-> (table :t1)
             (union (sort (table :t2) [:id])))
         "(SELECT t1.* FROM t1 ) UNION (SELECT t2.* FROM t2 ORDER BY id asc)"
         (-> (sort (table :t1) [:username])
             (union (table :t2)))
         "(SELECT t1.* FROM t1 ORDER BY username asc) UNION (SELECT t2.* FROM t2)"
         (union (sort (table :t1) [:id])
                (sort (table :t2) [:id]))
         "(SELECT t1.* FROM t1 ORDER BY id asc) UNION (SELECT t2.* FROM t2 ORDER BY id asc)"
         (-> (union (sort (table :t1) [:id])
                    (sort (table :t2) [:id]))
             (sort [:em]))
         "(SELECT t1.* FROM t1 ORDER BY id asc) UNION (SELECT t2.* FROM t2 ORDER BY id asc) ORDER BY em asc"
         (-> (aggregate (table :t1) [[:count/* :as :cnt]] [:id])
             (union (table :t2)))
         "(SELECT t1.id,count(*) AS cnt FROM t1 GROUP BY t1.id) UNION (SELECT t2.* FROM t2) GROUP BY t1.id"
         (-> (take (table :t1) 5)
             (union (table :t2))
             (take 10))
         "(SELECT t1.* FROM t1 LIMIT 5) UNION (SELECT t2.* FROM t2) LIMIT 10"))
  )
