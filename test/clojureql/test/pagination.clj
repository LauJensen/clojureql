(ns clojureql.test.core
  (:refer-clojure :exclude [sort])
  (:use [clojureql.core :only (table sort)]
        clojure.test
        clojureql.test
        clojureql.pagination))

(deftest test-offset
  (is (thrown-with-msg? IllegalArgumentException #"Page must be greater than 0."
        (offset 0 25)))
  (are [page per-page expected]
    (is (= expected (offset page per-page)))
    1 25 0
    2 25 25
    3 25 50))

(database-test test-total
  (is (= 4 (total (table :users))))
  (is (= 4 (total (sort (table :users) [:name])))))

(database-test test-paginate
  (binding [*per-page* 2]
    (let [users (sort (table :users) [:name])]
      (is (thrown-with-msg? IllegalArgumentException #"Page must be greater than 0."
            (paginate users :page 0)))
      (testing "page #1"
        (is (= (paginate users) (paginate users :page 1)))
        (let [result (paginate users)]
          (is (= *per-page* (count result)))
          (is (= ["Christophe" "Frank"] (map :name result)))
          (let [meta (meta result)]
            (is (= 1 (:page meta)))
            (is (= *per-page* (:per-page meta)))
            (is (= 4 (:total meta))))))
      (testing "page #1 with 3 records per page"
        (let [result (paginate users :page 1 :per-page 3)]
          (is (= 3 (count result)))
          (is (= ["Christophe" "Frank" "Lau Jensen"] (map :name result)))
          (let [meta (meta result)]
            (is (= 1 (:page meta)))
            (is (= 3 (:per-page meta)))
            (is (= 4 (:total meta))))))
      (testing "page #2"
        (let [result (paginate users :page 2)]
          (is (= *per-page* (count result)))
          (is (= ["Lau Jensen" "sthuebner"] (map :name result)))
          (let [meta (meta result)]
            (is (= 2 (:page meta)))
            (is (= *per-page* (:per-page meta)))
            (is (= 4 (:total meta))))))
      (testing "page #2 with 3 records per page"
        (let [result (paginate users :page 2 :per-page 3)]
          (is (= 1 (count result)))
          (is (= ["sthuebner"] (map :name result)))
          (let [meta (meta result)]
            (is (= 2 (:page meta)))
            (is (= 3 (:per-page meta)))
            (is (= 4 (:total meta)))))))))
