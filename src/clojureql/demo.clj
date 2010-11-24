(ns clojureql.demo
  (:use clojureql.core
        clojure.contrib.sql)
  (:refer-clojure
   :exclude [group-by take sort drop conj! disj!]
   :rename {take take-coll}))

(def db
     {:classname   "com.mysql.jdbc.Driver"
      :subprotocol "mysql"
      :user        "cql"
      :password    "cql"
      :auto-commit true
      :fetch-size  500                                         ; Do selects in chunks of 500
      :subname     "//localhost:3306/cql"})

(defmacro tst [expr]
  `(do (print "Code:   " (quote ~expr) "\nSQL:     ")
       (println "Return: " ~expr "\n")))

(defn test-suite []
  (letfn [(drop-if [t] (try (drop-table t)
                        (catch Exception _)))]
    (with-connection db
      (drop-if :users)
      (create-table :users
                    [:id    :integer "PRIMARY KEY" "AUTO_INCREMENT"]
                    [:name  "varchar(255)"]
                    [:title "varchar(255)"])
      (drop-if :salary)
      (create-table :salary
                    [:id    :integer "PRIMARY KEY" "AUTO_INCREMENT"]
                    [:wage  :integer])))
  (open-global :mysql db)                                      ; Open a persistent connection
  (binding [*debug* true]                                      ; Causes all SQL statements to be printed
    (let [users  (-> (table :mysql :users)
                     (project [:id :name :title]))
          salary (-> (table :mysql :salary)
                     (project [:id :wage]))
          roster [{:name "Lau Jensen" :title "Dev"}
                  {:name "Christophe" :title "Design Guru"}
                  {:name "sthuebner"  :title "Mr. Macros"}
                  {:name "Frank"      :title "Engineer"}]
          wages  (map #(hash-map :wage %) [100 200 300 400])]
      (tst @(conj! users roster))                              ; Add multiple rows
      (tst @(conj! salary wages))                              ; Same
      (tst @(join users salary (where (= :users.id
                                         :salary.id))))        ; Join two tables explicitly
      (tst @(join users salary :id))                           ; Join two tables with USING
      (tst @(-> users
                (conj! {:name "Jack"})                         ; Add a single row
                (disj! (where (= :id 1)))                      ; Remove another
                (update-in! (where (= :id 2)) {:name "John"})  ; Update a third
                (sort [:id#desc])                              ; Prepare to sort
                (project #{:id :title})                        ; Returns colums id and title
                (select (where (<= :id 10)))                   ; Where ID is <= 10
                (join salary :id)                              ; Join with table salary
                (limit 10)))                                   ; Limit return to 10 rows
      (tst @(-> (disj! users (where (or (= :id 3) (= :id 4))))
                (sort [:id])))                                 ; ASC is implicit
      (tst @(limit users 1))
      (tst @(-> (table db :salary) (project [:avg/wage])))
      #_(tst (select users (where "id=%1 OR id=%2" 1 10)))
      (tst @(select users (where (or (= :id 1) (>= :id 10)))))))
  (close-global :mysql))