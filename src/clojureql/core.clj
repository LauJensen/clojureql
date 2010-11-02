(ns clojureql.core
  (:use
   clojureql.internal
   [clojure.string :only [join] :rename {join join-str}]
   [clojure.contrib sql])
  (refer-clojure :exclude [take drop sort conj! disj!] :rename {take take-coll}))


(def db
     {:classname   "com.mysql.jdbc.Driver"
      :subprotocol "mysql"
      :user        "cql"
      :password    "cql"
      :subname     "//localhost:3306/cql"})


(defprotocol Relation
  (select [_    predicate]      "Queries the table using a predicate")
  (drop   [_    predicate]      "Queries the table using an inverted predicate")
  (conj!  [this records]        "Inserts record(s) into the table")
  (disj!  [this predicate]      "Deletes record(s) from the table")
  (take   [_    n]              "Queries the table with LIMIT n")
  (sort   [_    col type]       "Sorts the query either :asc or :desc")
  (join   [_    table2 join_on] "Joins two table")
  )

(defrecord Table [cnx tname tcols]
  clojure.lang.IDeref
  (deref [_]
         (with-connection cnx
           (with-query-results rs
             [(format "SELECT %s FROM %s" (colkeys->string tcols) (name tname))]
             (doall rs))))
  Relation
  (select [_ predicate]
         (with-connection cnx
           (with-query-results rs
             [(format "SELECT %s FROM %s %s" (colkeys->string tcols) (name tname) predicate)]
             (doall rs))))
  (drop [_ predicate]
         (with-connection cnx
           (with-query-results rs
             [(format "SELECT %s FROM %s WHERE not(%s)" (colkeys->string tcols) (name tname) predicate)]
             (doall rs))))
  (conj! [this records]
         (with-connection cnx
           (if (map? records)
             (insert-records tname records)
             (apply insert-records tname records)))
         this)
  (disj! [this predicate]
         (with-connection cnx
           (delete-rows tname [(map->predicate predicate)]))
         this)
  (take  [_ n]
         (with-connection cnx
           (with-query-results rs
             [(format "SELECT %s FROM %s LIMIT %d" (colkeys->string tcols) (name tname) n)]
             (doall rs))))
  (sort  [_ col type]
         (with-connection cnx
           (with-query-results rs
             [(format "SELECT %s FROM %s ORDER BY %s %s"
                      (colkeys->string tcols)
                      (name tname)
                      (name col)
                      (if (= :asc type) "ASC" "DESC"))]
             (doall rs))))
  (join  [_ table2 join_on]
         (with-cnx cnx
           (with-results rs
             [(format "SELECT %s FROM %s JOIN %s ON %s"
                      (colkeys->string tcols)
                      (name tname)
                      (-> table2 :tname name)
                      (->> join_on (map name) (join-str \=)))]
             (doall rs))))
  )

(defn table [connection-info table-name table-colums]
  " Returns a reference to a table, which will be accessed via the connection-info
    (contrib.sql spec) and query the table-name (keyword) for the colums defined in
    table-colums.

    (table *conn-info* :table1 [:name :id]) "
  (Table. connection-info table-name table-colums))



#_(do

   (def users (table users [:name :title]))
   (def salary (table salary ["*"]))

   @users  ; select <constructor supplied columns> from users
   ({:name "Lau" :title "Developer"} {:name "cgrand" :title "Design Guru"})

   (conj! users {:name "sthuebner" :title "Mr. Macros"}) ; insert into

   (disj! users {:name "Lau" 'title "Dev"}) ; remove entry with name=Lau OR title=Dev

   (sort users :col :asc)      ; select <cols> from users order by 'col' ASC

   (take users 5)              ; select <cols> from users LIMIT 5

   (join users salary #{:users.id :salary.id})

   (let [where-pred (sql-clause "%1 = %2" "id" 2)]
     (pick users where-pred)  ; select <cols> where id = 2
     (drop users where-pred)) ; select <cols> where id != 2

   )