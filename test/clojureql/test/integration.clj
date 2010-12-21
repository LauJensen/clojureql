(ns clojureql.test.integration
  (:import java.sql.Timestamp)
  (:use clojure.test clojureql.core clojureql.test)
  (:refer-clojure
   :exclude [compile take sort drop conj! disj! distinct]
   :rename {take take-coll}))

(database-test test-conj!
  (is @(conj! users roster))
  (is (= @(conj! salary wages)
         '({:wage 100, :id 1}
           {:wage 200, :id 2}
           {:wage 300, :id 3}
           {:wage 400, :id 4}
           {:wage 100, :id 5} ; Duplicates because the macro runs insert-data
           {:wage 200, :id 6}
           {:wage 300, :id 7}
           {:wage 400, :id 8}))))

(database-test test-join-explicitly
  (is (= @(join users salary (where (= :users.id :salary.id)))
         '({:wage 100, :title "Dev", :name "Lau Jensen", :id 1}
           {:wage 200, :title "Design Guru", :name "Christophe", :id 2}
           {:wage 300, :title "Mr. Macros", :name "sthuebner", :id 3}
           {:wage 400, :title "Engineer", :name "Frank", :id 4}))))

(database-test test-join-using
  (is (= @(join users salary :id)
         '({:wage 100, :title "Dev", :name "Lau Jensen", :id 1}
           {:wage 200, :title "Design Guru", :name "Christophe", :id 2}
           {:wage 300, :title "Mr. Macros", :name "sthuebner", :id 3}
           {:wage 400, :title "Engineer", :name "Frank", :id 4}))))

(database-test test-chained-statements
  (is (= @(-> users
              (conj! {:name "Jack"})          ; Add a single row
              (disj! (where (= :id 1)))       ; Remove another
              (update-in! (where (= :id 2))
                          {:name "John"})     ; Update a third
              (sort [:id#desc])               ; Prepare to sort
              (project #{:id :title})         ; Returns colums id and title
              (select (where (<= :id 10)))    ; Where ID is <= 10
              (join salary :id)               ; Join with table salary
              (limit 10))                     ; Take a maximum of 10 entries
         '({:id 4, :title "Engineer", :wage 400}
           {:id 3, :title "Mr. Macros", :wage 300}
           {:id 2, :title "Design Guru", :wage 200}))))

(database-test test-implicit-asc-sort
  (is (= @(-> (disj! users (where (or (= :id 3) (= :id 4))))
              (sort [:id]))
         '({:title "Dev", :name "Lau Jensen", :id 1}
           {:title "Design Guru", :name "Christophe", :id 2}))))

(database-test test-implicit-asc-sort
  (is (= 1 (count @(limit users 1)))))

(database-test test-avg
  (insert-data)
  (cond
   (postgresql?) (is (= @(-> (table :salary) (project [:avg/wage]))
                        '({:avg 250.0000000000000000M})))
   (mysql?)      (is (= @(-> (table :salary) (project [[:avg/wage :as :avg]]))
                        '({:avg 250.0000M})))
   (sqlite3?)    (is (= @(-> (table :salary) (project [[:avg/wage :as :avg]]))
                        '({:avg 250.0})))
   :else true))

(database-test test-select-with-nil-and-value
  (is (empty? @(select (table :users) (where (and (= :name "a") (= :id nil)))))))

; TODO: Isnt this test useless? ["SELECT users.* FROM users WHERE (NULL IS NULL)"]
#_(database-test test-select-with-nil
    (is (empty? @(select (table :users) (where (= nil nil))))))

(database-test test-select-is-null
  (when (or (postgresql?) (mysql?)) ; (where true) not supported by sqlite3
    (let [[alice bob] @(-> (disj! users (where true))
                           (conj! [{:name "Alice" :title "Developer"}
                                   {:name "Bob"}]))]
      (is (= bob (first @(select users (where (= :title nil)))))))))

(database-test test-select-is-not-null
  (when (or (postgresql?) (mysql?)) ; (where true) not supported by sqlite3
    (let [[alice bob] @(-> (disj! users (where true))
                           (conj! [{:name "Alice" :title "Developer"} {:name "Bob"}]))]
      (is (= alice (first @(select users (where (!= :title nil)))))))))

(database-test test-select-or
  (is (= @(select users (where (or (= :id 1) (>= :id 10))))
         '({:title "Dev", :name "Lau Jensen", :id 1}))))

(database-test test-select-not
  (is (= @(select users (where (not (> :id 1))))
         '({:title "Dev", :name "Lau Jensen", :id 1})))
  (is (= @(select users (where (not (or (< :id 2) (> :id 2)))))
         '({:title "Design Guru", :name "Christophe", :id 2})))
  (is (= @(select users (where (not (or (< :id 2) (not (< :id 3))))))
         '({:title "Design Guru", :name "Christophe", :id 2}))))

(database-test test-update-in!
  (is (= @(-> (update-in! users (where (= :id 2)) {:name "John"})
              (select (where (= :id 2))))
         '({:title "Design Guru", :name "John", :id 2}))))

(database-test test-update-in!-with-nil
  (when (or (postgresql?) (sqlite3?)) ; TODO: MySQL does not insert new row!?
    (is (= @(-> (update-in! users (where (= :id nil)) {:name "John"})
                (project [:id :name :title]))
           '({:title "Dev", :name "Lau Jensen", :id 1}
             {:title "Design Guru", :name "Christophe", :id 2}
             {:title "Mr. Macros", :name "sthuebner", :id 3}
             {:title "Engineer", :name "Frank", :id 4}
             {:title nil, :name "John", :id 5})))))

(database-test test-update-in!-with-timestamp
  (let [user (first @(update-in! users
                                 (where (= :id 1))
                                 {:birthday (Timestamp/valueOf "1980-01-01 00:00:00.00")}))]
    (is @(update-in! (table :users)
                     (where (= :id (:id user)))
                     {:birthday (Timestamp/valueOf "1980-01-02 00:00:00.00")}))))

(database-test test-difference
  (when (or (postgresql?) (sqlite3?))
    (drop-schema)
    (create-schema)
    (let [[alice bob] @(conj! users [{:name "Alice" :title "Developer"} {:name "Bob"}])]
      (is (empty? @(difference (select (table :users) (where (= :id (:id alice))))
                               (select (table :users) (where (= :id (:id alice))))))))))

(database-test test-intersection
  (when (or (postgresql?) (sqlite3?))
    (let [[alice bob] @(conj! users [{:name "Alice" :title "Developer"} {:name "Bob"}])]
      (is (= (map :id [alice])
             (map :id @(intersection (select (table :users) (where (= :id (:id alice))))
                                     (table :users))))))))

(database-test test-union
  (let [[alice bob] @(conj! users [{:name "Alice" :title "Developer"} {:name "Bob"}])]
    (is (= (map :id [alice bob])
           (map :id @(union (select (table :users) (where (= :id (:id alice))))
                            (select (table :users) (where (= :id (:id bob))))))))
    (let [query (select (table :users) (where (= :id (:id alice))))]
      (is (= (map :id [alice alice alice])
             (map :id @(-> query (union query :all) (union query :all))))))))

(deftest should-accept-fn-with-connection-info
  (let [connection-info-fn (fn [] mysql)
	connection-info-from-var (table mysql :users)
	connection-info-from-fn (table connection-info-fn :users)]
    (is (= connection-info-from-var connection-info-from-fn))))

(database-test test-distinct
  (let [alice {:name "Alice" :title "Dev"}]
    @(conj! users [alice alice])
    (let [query (-> (table :users)
		    (project [:name :title])
		    (select (where (= "Dev" :title))))
	  non-distinct @query
	  distinct @(-> query
			distinct)]
      (is (= 3 (count non-distinct)))
      (is (= 2 (count distinct))))))
	  
      
		 
