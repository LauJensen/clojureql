(defproject clojureql "1.1.0-SNAPSHOT"
  :description      "Superior SQL integration for Clojure"
  :dependencies     [[org.clojure/clojure         "1.2.1"]
                     [org.clojure/core.incubator  "0.1.0"]
                     [org.clojure.contrib/mock    "1.3.0-alpha4"]
                     [org.clojure/java.jdbc       "0.0.5"]]
  :dev-dependencies [[swank-clojure               "1.3.2"]
                     [mysql/mysql-connector-java  "5.1.17"]
                     [org.xerial/sqlite-jdbc      "3.7.2"]
                     [postgresql/postgresql       "8.4-702.jdbc4"]
                     [hsqldb/hsqldb               "1.6.1"]]

  :repositories {"clojure-releases"  {:url "http://build.clojure.org/releases"}})



