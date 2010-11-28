(ns clojureql.test
  (:use clojure.contrib.sql clojure.test))

(def mysql
  {:classname   "com.mysql.jdbc.Driver"
   :subprotocol "mysql"
   :user        "cql"
   :password    "cql"
   :auto-commit true
   :fetch-size  500
   :subname     "//localhost:3306/cql"})

(def postgresql
  {:classname   "org.postgresql.Driver"
   :subprotocol "postgresql"
   :user        "cql"
   :password    "cql"
   :auto-commit true
   :fetch-size  500
   :subname     "//localhost:5432/cql"})

(def sqlite3
  {:classname "org.sqlite.JDBC"
   :subprotocol "sqlite"
   :subname "/tmp/cql.sqlite3"
   :create true})

(def databases [mysql postgresql sqlite3])

(defn mysql? []
  (isa? (class (connection)) com.mysql.jdbc.JDBC4Connection))

(defn postgresql? []
  (isa? (class (connection)) org.postgresql.jdbc4.Jdbc4Connection))

(defn sqlite3? []
  (isa? (class (connection)) org.sqlite.Conn))

(defn drop-if [table]
  (try (drop-table table) (catch Exception _)))

(defn drop-schema []
  (drop-if :salary)
  (drop-if :users))

(defmulti create-schema #(class (connection)))

(defmethod create-schema com.mysql.jdbc.JDBC4Connection []
  (create-table
   :users
   [:id        :integer "PRIMARY KEY" "AUTO_INCREMENT"]
   [:name      "varchar(255)"]
   [:title     "varchar(255)"]
   [:birthday  "TIMESTAMP"])
  (create-table
   :salary
   [:id        :integer "PRIMARY KEY" "AUTO_INCREMENT"]
   [:wage      :integer]))

(defmethod create-schema org.postgresql.jdbc4.Jdbc4Connection []
  (create-table
   :users
   [:id        "SERIAL"]
   [:name      "varchar(255)"]
   [:title     "varchar(255)"]
   [:birthday  "TIMESTAMP"])
  (create-table
   :salary
   [:id        "SERIAL"]
   [:wage      :integer]))

(defmethod create-schema org.sqlite.Conn []
  (create-table
   :users
   [:id        :integer "PRIMARY KEY" "AUTOINCREMENT"]
   [:name      "varchar(255)"]
   [:title     "varchar(255)"]
   [:birthday  "TIMESTAMP"])
  (create-table
   :salary
   [:id        :integer "PRIMARY KEY" "AUTOINCREMENT"]
   [:wage      :integer]))

(defmacro database-test [name & body]
  (let [name# name body# body]
    `(do ~@(for [database# databases]
             `(deftest ~(symbol (str name# "-" (:subprotocol database#)))
                (with-connection ~database#
                  (drop-schema)
                  (create-schema)
                  ~@body#))))))
