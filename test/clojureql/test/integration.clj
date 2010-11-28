(ns clojureql.test.integration
  (:import java.sql.Timestamp)
  (:use clojure.test clojureql.core clojureql.test)
  (:refer-clojure
   :exclude [group-by take sort drop conj! disj!]
   :rename {take take-coll}))

(def users  (-> (table :users) (project [:id :name :title])))
(def salary (-> (table :salary) (project [:id :wage])))

(def roster
  [{:name "Lau Jensen" :title "Dev"}
   {:name "Christophe" :title "Design Guru"}
   {:name "sthuebner"  :title "Mr. Macros"}
   {:name "Frank"      :title "Engineer"}])

(def wages (map #(hash-map :wage %) [100 200 300 400]))

(defn insert-data []
  @(conj! users roster)
  @(conj! salary wages))

(database-test test-conj!
  (is @(conj! users roster))
  (is @(conj! salary wages)))

(database-test test-join-explicitly
  (insert-data)
  (is @(join users salary (where (= :users.id :salary.id)))))

(database-test test-join-using
  (insert-data)
  (is @(join users salary :id)))

(database-test test-chained-statements
  (insert-data)
  (is @(-> users
           (conj! {:name "Jack"})          ; Add a single row
           (disj! (where (= :id 1)))       ; Remove another
           (update-in! (where (= :id 2)) {:name "John"}) ; Update a third
           (sort [:id#desc])               ; Prepare to sort
           (project #{:id :title})         ; Returns colums id and title
           (select (where (<= :id 10)))    ; Where ID is <= 10
           (join salary :id)               ; Join with table salary
           (limit 10))))

(database-test test-implicit-asc-sort
  (insert-data)
  (is @(-> (disj! users (where (or (= :id 3) (= :id 4))))
           (sort [:id]))))

(database-test test-implicit-asc-sort
  (insert-data)
  (is (= 1 (count @(limit users 1)))))

(database-test test-avg
  (insert-data)
  (is @(-> (table :salary) (project [:avg/wage]))))

(database-test test-select-with-nil-and-value
  (is (empty? @(select (table :users) (where (and (= :name "a") (= :id nil)))))))

(database-test test-select-with-nil
  (is (empty? @(select (table :users) (where (= nil nil))))))

(database-test test-select-is-null
  (let [[alice bob] @(conj! users [{:name "Alice" :title "Developer"} {:name "Bob"}])]
    (is (= bob (first @(select users (where (= :title nil))))))))

(database-test test-select-is-not-null
  (let [[alice bob] @(conj! users [{:name "Alice" :title "Developer"} {:name "Bob"}])]
    (is (= alice (first @(select users (where (!= :title nil))))))))

(database-test test-select-or
  (insert-data)
  (is @(select users (where (or (= :id 1) (>= :id 10))))))

(database-test test-update-in!
  (is @(update-in! (table :users) (where (= :id 2)) {:name "John"})))

(database-test test-update-in!-with-nil
  (is @(update-in! (table :users) (where (= :id nil)) {:name "John"})))

(database-test test-update-in!-with-timestamp
  (let [user (first @(update-in! (table :users) (where (= :id 1)) {:birthday (Timestamp/valueOf "1980-01-01 00:00:00.00")}))]
    (is @(update-in! (table :users) (where (= :id (:id user))) {:birthday (Timestamp/valueOf "1980-01-02 00:00:00.00")}))))

(database-test test-difference
  (when (or (postgresql?) (sqlite3?))
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
                            (select (table :users) (where (= :id (:id bob))))))))))
