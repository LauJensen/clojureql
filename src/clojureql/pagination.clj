(ns clojureql.pagination
  (:refer-clojure :exclude [drop])
  (:use [clojureql.core :only (drop limit)]))

(def *per-page* 25)

(defn- assert-page
  "Ensure that page is greater than zero."
  [page] (if-not (pos? page) (throw (IllegalArgumentException. "Page must be greater than 0."))))

(defn offset
  "Calculate the SQL offset from page and per-page."
  [page per-page]
  (assert-page page)
  (* (dec page) per-page))

(defn total
  "Count the total number of records for the given relation."
  [table & {:keys [page per-page]}]
  (:count (first @(assoc table :tcols [:count/* :as :count] :order-by nil))))

(defn paginate
  "Paginate the given relation by page and per-page."
  [table & {:keys [page per-page]}]
  (let [page (or page 1) per-page (or per-page *per-page*)]
    (with-meta @(-> table (drop (offset page per-page)) (limit per-page))
      {:page page
       :per-page per-page
       :total (total table)})))
