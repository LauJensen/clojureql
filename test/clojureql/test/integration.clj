(ns clojureql.test.integration
  "To run integration tests, you can use cake command line arguments:

  cake test --integration=true

  You will need to create MySQL, PostgreSQL and Sqlite3 databases according
  to the parameters you can find in test/clojureql/test.clj"

  (:import java.sql.Timestamp)
  (:use clojure.test clojureql.core clojureql.test)
  (:refer-clojure
   :exclude [compile take sort drop distinct conj! disj! case]
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

(database-test test-generated-keys
  ; old derby releases don't support generated keys
  ; SQLite has a bug here, see http://code.google.com/p/sqlite-jdbc/issues/detail?id=10
  (when (or (mysql?) (postgresql?)) 
    (is (= 5 (-> (conj! salary {:wage 1337})
                 meta :last-index)))
    (is (= 6 (-> (update-in! salary (where (= :id 512))
                           {:wage 1337})
                 meta :last-index)))))

(database-test test-join-explicitly
  (is (= @(join users salary (where (= :users.id :salary.id)))
         '({:wage 100, :title "Dev", :name "Lau Jensen", :id 1}
           {:wage 200, :title "Design Guru", :name "Christophe", :id 2}
           {:wage 300, :title "Mr. Macros", :name "sthuebner", :id 3}
           {:wage 400, :title "Engineer", :name "Frank", :id 4}))))

(database-test test-join-using
  (when (not (derby?)) ; derby doesn't support using
    (is (= @(join users salary :id)
           '({:wage 100, :title "Dev", :name "Lau Jensen", :id 1}
             {:wage 200, :title "Design Guru", :name "Christophe", :id 2}
             {:wage 300, :title "Mr. Macros", :name "sthuebner", :id 3}
             {:wage 400, :title "Engineer", :name "Frank", :id 4})))))

(database-test test-case
  (when (not (derby?)) ; derby doesn't support ? in case
    (is (= @(-> (project salary
                         [:id (case :wages
                                    (<= :wage 150)  "low"
                                    (>= :wage 150)  "high"
                                    :else "average")])
                (select (where (<= :id 2))))
           '({:wages "low" :id 1}
             {:wages "high" :id 2})))))

(database-test test-chained-statements
  (if (derby?) ; derby doesn't support using
    (is (= @(-> users
                (conj! {:name "Jack"})          ; Add a single row
                (disj! (where (= :id 1)))       ; Remove another
                (update-in! (where (= :id 2))
                            {:name "John"})     ; Update a third
                (sort [:id#desc])               ; Prepare to sort
                (project [:id :title])          ; Returns colums id and title
                (select (where (<= :id 10)))    ; Where ID is <= 10
                (join salary
                      (where (= :users.id
                                :salary.id))))  ; Join with table salary explicitly
           '({:id 4, :title "Engineer", :wage 400}
             {:id 3, :title "Mr. Macros", :wage 300}
             {:id 2, :title "Design Guru", :wage 200})))               
    (is (= @(-> users
                (conj! {:name "Jack"})          ; Add a single row
                (disj! (where (= :id 1)))       ; Remove another
                (update-in! (where (= :id 2))
                            {:name "John"})     ; Update a third
                (sort [:id#desc])               ; Prepare to sort
                (project [:id :title])          ; Returns colums id and title
                (select (where (<= :id 10)))    ; Where ID is <= 10
                (join salary :id)               ; Join with table salary
                (limit 10))                     ; Take a maximum of 10 entries
           '({:id 4, :title "Engineer", :wage 400}
             {:id 3, :title "Mr. Macros", :wage 300}
             {:id 2, :title "Design Guru", :wage 200})))))

(database-test test-implicit-asc-sort
  (is (= @(-> (disj! users (where (or (= :id 3) (= :id 4))))
              (sort [:id]))
         '({:title "Dev", :name "Lau Jensen", :id 1}
           {:title "Design Guru", :name "Christophe", :id 2}))))

(database-test test-limit-1
  (if (not (derby?))
    (is (= 1 (count @(limit users 1))))))

(database-test test-avg
  (insert-data)
  (cond
   (postgresql?) (is (= @(-> (table :salary) (project [:avg/wage]))
                        '({:avg 250.0000000000000000M})))
   (mysql?)      (is (= @(-> (table :salary) (project [[:avg/wage :as :avg]]))
                        '({:avg 250.0000M})))
   (sqlite3?)    (is (= @(-> (table :salary) (project [[:avg/wage :as :avg]]))
                        '({:avg 250.0})))
   (derby?)      (is (= @(-> (table :salary) (project [[:avg/wage :as :avgx]])) ; avg is a keyword in derby
                        '({:avgx 250.0})))
   :else true))

(database-test test-select-with-nil-and-value
  (is (empty? @(select (table :users) (where (and (= :name "a") (= :id nil)))))))

; TODO: Isnt this test useless? ["SELECT users.* FROM users WHERE (NULL IS NULL)"]
#_(database-test test-select-with-nil
    (is (empty? @(select (table :users) (where (= nil nil))))))

(database-test test-select-is-null
  (when (or (postgresql?) (mysql?)) ; (where true) not supported by sqlite3, derby
    (let [[alice bob] @(-> (disj! users (where true))
                           (conj! [{:name "Alice" :title "Developer"}
                                   {:name "Bob"}]))]
      (is (= bob (first @(select users (where (= :title nil)))))))))

(database-test test-select-is-not-null
  (when (or (postgresql?) (mysql?)) ; (where true) not supported by sqlite3, derby
    (let [[alice bob] @(-> (disj! users (where true))
                           (conj! [{:name "Alice" :title "Developer"} {:name "Bob"}]))]
      (is (= alice (first @(select users (where (!= :title nil)))))))))

(database-test test-select-equals
  (is (= @(select users (where (= :title "Dev")))
         '({:title "Dev", :name "Lau Jensen", :id 1}))))

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

(database-test test-update!
  (is (= @(-> (update! users (where (= :id 2)) {:name "John"})
              (select (where (= :id 2))))
         '({:title "Design Guru", :name "John", :id 2}))))

(database-test test-update-in!
  (is (= @(-> (update-in! users (where (= :id 2)) {:name "John"})
              (select (where (= :id 2))))
         '({:title "Design Guru", :name "John", :id 2}))))

(database-test test-update-in!-with-nil
  (let [t #(is (= @(-> (update-in! users (where (= :id nil)) {:name "John"})
                       (project [:id :name :title]))
                  '({:title "Dev", :name "Lau Jensen", :id 1}
                    {:title "Design Guru", :name "Christophe", :id 2}
                    {:title "Mr. Macros", :name "sthuebner", :id 3}
                    {:title "Engineer", :name "Frank", :id 4}
                    {:title nil, :name "John", :id 5})))]
    (if (mysql?) ;; mysql bugs here, if we stay in the same connection
      (clojure.java.jdbc/with-connection mysql (t))
      (t))))

(database-test test-update-in!-with-timestamp
  (let [user (first @(update-in! users
                                 (where (= :id 1))
                                 {:birthday (Timestamp/valueOf "1980-01-01 00:00:00.00")}))]
    (is @(update-in! (table :users)
                     (where (= :id (:id user)))
                     {:birthday (Timestamp/valueOf "1980-01-02 00:00:00.00")}))))

(database-test test-difference
  (when (postgresql?) ;(mysql?)) ;(sqlite3?)) TODO:  sqlite does not support queries in parens
    (drop-schema)
    (create-schema)
    (let [[alice bob] @(conj! users [{:name "Alice" :title "Developer"} {:name "Bob"}])]
      (is (empty? @(difference (select (table :users) (where (= :id (:id alice))))
                               (select (table :users) (where (= :id (:id alice))))))))))

(database-test test-intersection
  (when (postgresql?) ; (mysql?)) ;(sqlite3?)) TODO: MySQL does not have INTERSECT
    (let [[alice bob] @(conj! users [{:name "Alice" :title "Developer"} {:name "Bob"}])]
      (is (= (map :id [alice])
             (map :id @(intersection (select (table :users) (where (= :id (:id alice))))
                                     (table :users))))))))

(database-test test-union
  (when (or (postgresql?) (mysql?) (derby?))
    (let [[alice bob] @(conj! users [{:name "Alice" :title "Developer"} {:name "Bob"}])]
      (is (= (map :id [alice bob])
             (map :id @(union (select (table :users) (where (= :id (:id alice))))
                              (select (table :users) (where (= :id (:id bob))))))))
      (let [query (select (table :users) (where (= :id (:id alice))))]
        (is (= (map :id [alice alice alice])
               (map :id @(-> query (union query :all) (union query :all)))))))))

(deftest should-accept-fn-with-connection-info
  (let [connection-info-fn (fn [] mysql)
        connection-info-from-var (table mysql :users)
        connection-info-from-fn (table connection-info-fn :users)]
    (is (= connection-info-from-var connection-info-from-fn))))

(database-test test-resultset
  (let [tbl (if (derby?) ; derby doesn't support join using
              (join users salary (where (= :users.id :salary.id)))
              (join users salary :id))
        no-missing? #(not
                     (some keyword? ; which would be :clojureql.internal/missing
                           (mapcat vals %)))]
    (is (no-missing? @tbl))
    (with-results [res tbl]
      (is (no-missing? res))
      (is (= res @tbl)))))

(database-test test-dupes
  (let [users-tbl  (project users  [[:name :as :dupe]])
        salary-tbl (project salary [[:wage :as :dupe]])
        tbl (if derby? ; derby doesn't support join using
              (join users-tbl salary-tbl (where (= :users.id :salary.id)))
              (join users-tbl salary-tbl :id))]
    (is (thrown-with-msg? Exception
          #".*:dupe.*" @tbl)))
  (let [tbl (project users [[:name :as :dupe]
                            [:name :as :dupe]])]
    (is (= (map :name @users)
           (map :dupe @tbl)))))

(database-test test-transform
  (is (= @(transform users #(map (juxt :id :name) %))
         '([1 "Lau Jensen"] [2 "Christophe"] [3 "sthuebner"] [4 "Frank"]))))

(database-test test-transform-and-with-results
  (with-results [names (transform users #(map :name %))]
    (is (= names
           '("Lau Jensen" "Christophe" "sthuebner" "Frank")))))

(database-test test-pick
  (let [q (select users (where (= :id 4)))
        eq (select users (where (= :id 1337)))]
    (is (= @(pick q :name) "Frank"))
    (is (= @(pick q :id) 4))
    (is (= @(pick q [:title :name]) ["Engineer" "Frank"]))
    (is (= @(pick eq :name) nil))
    (is (= @(pick eq [:title :name])) nil)))

(database-test test-composing-transforms
  (is (= @(-> users
              (transform #(map (juxt :id :name) %))
              (transform first))
         [1 "Lau Jensen"])))

(database-test test-transform-with-join
  (is (= @(-> users
              (transform #(map (juxt :name :wage) %))
              (join (transform salary first) ; this transform will be ignored
                    (where (= :users.id :salary.id))))
         '(["Lau Jensen" 100] ["Christophe" 200] ["sthuebner" 300] ["Frank" 400]))))
