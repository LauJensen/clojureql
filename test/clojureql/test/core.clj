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
              "WHERE (admin = true) GROUP BY users.country,users.city")
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

  (testing "joins are associative"
    (let [ta (join (table :t1) (table :t2) :id)
	  tb (join (table :t3) ta :id)] ;; swapping argument order of "ta" and "(table :t3)" works
      (are [x y] (= (-> x (compile nil) interpolate-sql (.replaceAll "SELECT .* FROM" "SELECT * FROM")) y)
	   tb
	   "SELECT * FROM t3 JOIN t2 USING(id) JOIN t1 USING(id)"))
    (let [ta (-> (table :t1)
		 (join (table :t2) (where (= :t1.a :t2.a)))
		 (join (table :t6) (where (= :t6.e :t2.e))))
	  tb (-> (table :t3)
		 (join (table :t4) (where (= :t3.b :t4.b)))
		 (join (table :t5) (where (= :t5.d :t4.d))))
	  qu (join ta tb (where (= :t3.c :t2.c)))]
      (are [x y] (= (-> x (compile nil) interpolate-sql (.replaceAll "SELECT .* FROM" "SELECT * FROM")) y)
	   qu
	   (str "SELECT * FROM t1 "
		"JOIN t2 ON (t1.a = t2.a) "
		"JOIN t6 ON (t6.e = t2.e) "
		"JOIN t3 ON (t3.c = t2.c) "
		"JOIN t4 ON (t3.b = t4.b) "
		"JOIN t5 ON (t5.d = t4.d)")))
    (let [product-variants-table (project (table :product_variants)
					      [[:id			:as :product_variant_id]
					       [:product_id		:as :product_variant_product_id]
					       [:product_code		:as :product_variant_product_code]
					       [:status			:as :product_variant_status]
					       [:price_id		:as :product_variant_price_id]])
	  products-table (project (table :products)
				      [[:id				:as :product_id]
				       [:name				:as :product_name]
				       [:description			:as :product_description]
				       [:manufacturer_id		:as :product_manufacturer_id]])
	  product-variant-skus-table (project (table :product_variant_skus)
						  [[:id			:as :product_variant_sku_id]
						   [:product_variant_id	:as :product_variant_sku_product_variant_id]
						   [:sku_id		:as :product_variant_sku_sku_id]
						   [:quantity		:as :product_variant_sku_quantity]])
	  orders-table   (project (table :orders)
				      [[:id				:as :order_id]
				       [:customer_id			:as :order_customer_id]
				       [:customer_ref			:as :order_customer_ref]
				       [:created			:as :order_created]
				       [:status				:as :order_status]
				       [:created_by			:as :order_created_by]
				       [:source_id			:as :order_source_id]
				       [:updated			:as :order_updated]
				       [:cancellation_reason_id		:as :order_cancellation_reason_id]
				       [:expirable			:as :order_expirable]
				       [:shipping_method_id		:as :order_shipping_method_id]])
	  order-lines-table (project (table :order_lines)
					 [[:id				:as :order_line_id]
					  [:order_id			:as :order_line_order_id]
					  [:product_variant_id		:as :order_line_product_variant_id]
					  [:quantity			:as :order_line_quantity]
					  [:status			:as :order_line_status]
					  [:updated			:as :order_line_updated]
					  [:price_id			:as :order_line_price_id]
					  [:shippable_estimate		:as :order_line_shippable_estimate]])
	  orders-with-lines-query (-> orders-table
				      (join order-lines-table (where (= :orders.id :order_lines.order_id))))
	  sku-table (project (table :skus) [[:id		:as :sku_id]
						    [:stock_code	:as :sku_stock_code]
						    [:barcode		:as :sku_barcode]
						    [:reorder_quantity	:as :sku_reorder_quantity]
						    [:minimum_level	:as :sku_minimum_level]])
	  products-with-skus-query (-> product-variants-table
				       (join products-table (where (= :products.id :product_variants.product_id)))
				       (join product-variant-skus-table (where (= :product_variants.id :product_variant_skus.product_variant_id)))
				       (join sku-table (where (= :skus.id :product_variant_skus.sku_id))))
	  orders-with-skus-query (-> orders-with-lines-query
				     (join products-with-skus-query
					       (where (= :order_lines.product_variant_id :product_variants.id))))
	  open-orders-with-skus-query  (-> orders-with-skus-query
					   (select (where (= :orders.status 1))))]
      (are [x y] (= (-> x (compile nil) interpolate-sql (.replaceAll "SELECT .* FROM" "SELECT * FROM")) y)
	   open-orders-with-skus-query
	   (str "SELECT * FROM orders "
		"JOIN order_lines ON (orders.id = order_lines.order_id) "
		"JOIN product_variants ON (order_lines.product_variant_id = product_variants.id)"
		"JOIN products ON (products.id = product_variants.product_id) "
		"JOIN product_variant_skus ON (product_variants.id = product_variant_skus.product_variant_id) "
		"JOIN skus ON (skus.id = product_variant_skus.sku_id) "
		" WHERE (orders.status = 1)"))))
  
  (testing "update-in!"
    (expect [update-or-insert-vals (has-args [:users ["(id = ?)" 1] {:name "Bob"}])]
      (update-in! (table :users) (where (= :id 1)) {:name "Bob"}))
    (expect [update-or-insert-vals (has-args [:users ["(salary IS NULL)"] {:salary 1000}])]
      (update-in! (table :users) (where (= :salary nil)) {:salary 1000})))

  (testing "difference"
    (are [x y] (= (-> x (compile nil) interpolate-sql) y)
         (difference (select (table :users) (where (>= :id 0)))
                (select (table :users) (where (= :id 1))))
         "(SELECT users.* FROM users WHERE (id >= 0)) EXCEPT (SELECT users.* FROM users WHERE (id = 1))"
         (-> (select (table :users) (where (>= :id 0)))
             (difference (select (table :users) (where (= :id 1))))
             (difference (select (table :users) (where (<= :id 2)))))
         "(SELECT users.* FROM users WHERE (id >= 0)) EXCEPT (SELECT users.* FROM users WHERE (id = 1)) EXCEPT (SELECT users.* FROM users WHERE (id <= 2))"
         (-> (select (table :users) (where (>= :id 0)))
             (difference (select (table :users) (where (= :id 1))) :all)
             (difference (select (table :users) (where (<= :id 2))) :distinct))
         "(SELECT users.* FROM users WHERE (id >= 0)) EXCEPT ALL (SELECT users.* FROM users WHERE (id = 1)) EXCEPT DISTINCT (SELECT users.* FROM users WHERE (id <= 2))"))

  (testing "intersection"
    (are [x y] (= (-> x (compile nil) interpolate-sql) y)
         (intersection (select (table :users) (where (>= :id 0)))
                (select (table :users) (where (= :id 1))))
         "(SELECT users.* FROM users WHERE (id >= 0)) INTERSECT (SELECT users.* FROM users WHERE (id = 1))"
         (-> (select (table :users) (where (>= :id 0)))
             (intersection (select (table :users) (where (= :id 1))))
             (intersection (select (table :users) (where (<= :id 2)))))
         "(SELECT users.* FROM users WHERE (id >= 0)) INTERSECT (SELECT users.* FROM users WHERE (id = 1)) INTERSECT (SELECT users.* FROM users WHERE (id <= 2))"
         (-> (select (table :users) (where (>= :id 0)))
             (intersection (select (table :users) (where (= :id 1))) :all)
             (intersection (select (table :users) (where (<= :id 2))) :distinct))
         "(SELECT users.* FROM users WHERE (id >= 0)) INTERSECT ALL (SELECT users.* FROM users WHERE (id = 1)) INTERSECT DISTINCT (SELECT users.* FROM users WHERE (id <= 2))"))

  (testing "union"
    (are [x y] (= (-> x (compile nil) interpolate-sql) y)
         (union (select (table :users) (where (>= :id 0)))
                (select (table :users) (where (= :id 1))))
         "(SELECT users.* FROM users WHERE (id >= 0)) UNION (SELECT users.* FROM users WHERE (id = 1))"
         (-> (select (table :users) (where (>= :id 0)))
             (union (select (table :users) (where (= :id 1))))
             (union (select (table :users) (where (<= :id 2)))))
         "(SELECT users.* FROM users WHERE (id >= 0)) UNION (SELECT users.* FROM users WHERE (id = 1)) UNION (SELECT users.* FROM users WHERE (id <= 2))"
         (-> (select (table :users) (where (>= :id 0)))
             (union (select (table :users) (where (= :id 1))) :all)
             (union (select (table :users) (where (<= :id 2))) :distinct))
         "(SELECT users.* FROM users WHERE (id >= 0)) UNION ALL (SELECT users.* FROM users WHERE (id = 1)) UNION DISTINCT (SELECT users.* FROM users WHERE (id <= 2))"))

  (testing "difference, intersection and union"
    (are [x y] (= (-> x (compile nil) interpolate-sql) y)
         (let [t1 (table :t1) t2 (table :t2)]
           (-> (select t1 (where (= :id 1)))
               (union t2)
               (take 5)))
         "(SELECT t1.* FROM t1 WHERE (id = 1)) UNION (SELECT t2.* FROM t2) LIMIT 5"
         (-> (select (table :users) (where (>= :id 0)))
             (difference (select (table :users) (where (= :id 1))) :all)
             (intersection (select (table :users) (where (= :id 2))))
             (union (select (table :users) (where (<= :id 3))) :distinct))
         "(SELECT users.* FROM users WHERE (id >= 0)) EXCEPT ALL (SELECT users.* FROM users WHERE (id = 1)) INTERSECT (SELECT users.* FROM users WHERE (id = 2)) UNION DISTINCT (SELECT users.* FROM users WHERE (id <= 3))"))

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


