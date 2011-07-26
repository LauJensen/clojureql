(defproject clojureql "1.1.0-SNAPSHOT"
  :description      "Superior SQL integration for Clojure"
  :dependencies     [[org.clojure/clojure         "1.2.0"]
                     [org.clojure/core.incubator  "0.1.0"]
                     [org.clojure.contrib/mock    "1.3.0-alpha4"]
                     [org.clojure/java.jdbc       "0.0.5"]]
  :dev-dependencies [[swank-clojure               "1.3.0-SNAPSHOT"]
                     [mysql/mysql-connector-java  "5.1.6"]
                     [org.xerial/sqlite-jdbc      "3.6.20.1"]
                     [postgresql/postgresql       "8.4-701.jdbc4"]]

  :repositories {"clojure-releases"  {:url "http://build.clojure.org/releases"}})



