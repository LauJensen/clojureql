(ns clojureql.test
  (:refer-clojure :exclude [compile drop take sort distinct conj! disj!])
  (:use clojure.contrib.sql
        clojure.test
        clojureql.core))

(try 
  (use '[cake :only [*opts*]])
  (catch Exception _
    (def *opts* {:integration true})))

(when (:show-sql *opts*)
  (alter-var-root #'clojureql.core/*debug* (constantly true)))

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
  {:classname   "org.sqlite.JDBC"
   :subprotocol "sqlite"
   :subname     "/tmp/cql.sqlite3"
   :create      true})

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

(def users  (-> (table :users) (project [:id :name :title])))
(def salary (-> (table :salary) (project [:id :wage])))

(def roster
  [{:name "Lau Jensen" :title "Dev"}
   {:name "Christophe" :title "Design Guru"}
   {:name "sthuebner"  :title "Mr. Macros"}
   {:name "Frank"      :title "Engineer"}])

(def wages (map #(hash-map :wage %) [100 200 300 400]))

(defn insert-data []
  (conj! users roster)
  (conj! salary wages))

(defmacro database-test [name & body]
  (let [name# name body# body]
    `(do ~@(for [database# databases]
             `(deftest ~(symbol (str name# "-" (:subprotocol database#)))
                (when (:integration *opts*)
                  (with-connection ~database#
                    (drop-schema)
                    (create-schema)
                    (insert-data)
                    (println "Testing (" (:subprotocol ~database#) "):\t\t\t" ~(str name#))
                    ~@body#)))))))

