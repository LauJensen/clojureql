ClojureQL
=========

ClojureQL is an abstraction layer sitting on top of standard low-level JDBC SQL integration.
It let's you interact with a database through a series of objects which work as Clojure data
type.

ClojureQL is modeled around the primitives defined in Relational Algebra.
http://en.wikipedia.org/wiki/Relational_algebra

For the user this means that all queries compose and are never executed unless dereferenced
or called with a function that has the ! suffix.

As a help for debugging, wrap your statements in (binding [*debug* true]) to see the
compiled SQL statement printed to stdout.

Installation
============

Add the following to your **project.clj** or pom.xml:

Cake/Lein artifact:

    [clojureql "1.0.0-beta2-SNAPSHOT"]

Maven:

    <dependency>
      <groupId>clojureql</groupId>
      <artifactId>clojureql</artifactId>
      <version>1.0.0-beta2-SNAPSHOT</version>
    </dependency>

Then execute

    cake deps

And import the library into your namespace

    (:use clojureql.core)


Manual
============

Initialization
--------------


**Example**:

    (def db
     {:classname   "com.mysql.jdbc.Driver"
      :subprotocol "mysql"
      :user        "cql"
      :password    "cql"
      :subname     "//localhost:3306/cql"})

**Complete specification**:

    Factory:
      :factory     (required) a function of one argument, a map of params
      (others)     (optional) passed to the factory function in a map

    DriverManager:
      :classname   (required) a String, the jdbc driver class name
      :subprotocol (required) a String, the jdbc subprotocol
      :subname     (required) a String, the jdbc subname
      (others)     (optional) passed to the driver as properties.

    DataSource:
      :datasource  (required) a javax.sql.DataSource
      :username    (optional) a String
      :password    (optional) a String, required if :username is supplied

    JNDI:
      :name        (required) a String or javax.naming.Name
      :environment (optional) a java.util.Map"

    Options:
      :auto-commit (optional) a Boolean
      :fetch-size  (optional) an integer


Queries
-----

    (def users (table db :users))  ; Defaults to all colums in table users

    @users
    >>> ({:id 1 :name "Lau"}
         {:id 2 :name "Christophe"}
         {:id 3 :name "Frank"})

    @(select users (where (< :id 3)))) ; Only selects IDs below 3
    >>> ({:name "Lau Jensen", :id 1}
         {:name "Christophe", :id 2})

    @(-> (select users (where (< :id 3)))
         (project [:title]))  ; <-- Only return the column :title
    >>> ({:title "Dev"}
         {:title "Design Guru"})

    @(-> (select users (where (!= :id 3))))  <-- Matches where ID is NOT 3
    >>> ({:name "Lau Jensen", :id 1}
         {:name "Christophe", :id 2})

    @(select users (where (and (= :id 1) (= :title "Dev")))) ;Strings are auto-quoted
    >>> ({:name "Lau Jensen", :id 1})

    @(select users (where (or (= :id 1) (= :title "Design Guru"))))
    >>> ({:name "Lau Jensen", :id 1}
         {:name "Christophe", :id 2})

For lazy traversal of the results, use **with-results**

    (with-results [results users]
       (doseq [r results]
         (println r)))

**Note:** No alteration of the query will trigger execution. Only dereferencing (@) will!

Aliasing
--------

*Tables:*

    (-> (table db {:salary :s1})
        (select (where (= :s1.id 5))))
    >>> "SELECT s1.* FROM salary s1 WHERE (s1.id = 5)"

*Columns:*

    (-> (table db :salary)
        (project [[:id :as :userid]])     ; Nester vector means aliasing
        (select (where (= :userid 5))))
    >>> "SELECT salary.id AS userid FROM salary  WHERE (userid = 5)"

Aggregates
----------

    @(-> (table db :salary)
         (aggregate [:avg/wage]))
    >>> ({:avg(wage) 250.0000M})

    @(-> (table db :salary)
         (aggregate [[:avg/wage :as :average]]))
    >>> ({:average 250.0000M})

    (-> (table db :salary)
        (aggregate [:avg/wage:expenses])
        (compile nil))
    >>> "SELECT avg(salary.wage, salary.expenses) FROM salary;

     (-> (table {} :users)
         (select (where (= :admin true)))
         (aggregate [:count/*])
         (compile nil))
     >>> "SELECT count(users.*) FROM users WHERE (admin = true)"

     (-> (table {} :users)
         (select (where (= :admin true)))
         (aggregate [:count/*] [:country])
         (compile nil))
     >>> "SELECT users.country,count(users.*) FROM users  WHERE (admin = true) GROUP BY country"

Manipulation
------------

    @(conj! users {:name "Jack"})
    >>> ({:id 1 :name "Lau"}
         {:id 2 :name "Christophe"}
         {:id 3 :name "Frank"}
         {:id 4 :name "Jack"})

    @(disj! users {:name "Jack"})
    >>> ({:id 1 :name "Lau"}
         {:id 2 :name "Christophe"}
         {:id 3 :name "Frank"})

    @(update-in! users (where (= :id 1)) {:name "Test"})
    >>> ({:id 1 :name "Tst"}
         {:id 2 :name "Christophe"}
         {:id 3 :name "Frank"})

**Note:** All of these take either a single map or a collection of maps as their final argument.

**Note:** These function execute and return a pointer to the table, so the can be chained with other calls.

Joins
------

    (def visitors (-> (table db :visitors)
                      (project [:id :guest])))

    @(join users visitors :id)                       ; USING(id)
    >>> ({:id 1 :name "Lau" :guest "false"} {:id 3 :name "Frank" :guest "true"})

    @(join users visitors (where (= :users.id :visitors.id)))  ; ON users.id = visitors.id
    >>> ({:id 1 :name "Lau" :guest "false"} {:id 3 :name "Frank" :guest "true"})

    (-> (outer-join users visitors :right (where (= :users.id :visitors.id)))
        (compile nil))
    >>> "SELECT users.* FROM users RIGHT OUTER JOIN
           (SELECT avg(visitors.field) FROM visitors GROUP BY field)
           AS visitors_aggregation
         ON (users.id = visitors_aggregation.id)"

**Note**: In the final example, visitors contains an aggregate field

Compound ops
------------

Since this is a true Relational Algebra implementation, everything composes!

    @(-> (conj! users {:name "Jack"})       ; Add a row
         (disj! (where (= {:name "Lau"})))  ; Remove another
         (sort [:id#desc])                  ; Prepare to sort in descending order
         (project [:id :title])             ; Select only these columns in the query
         (select (where (!= :id 5)))        ; But filter out ID = 5
         (join :salary :id)                 ; Join with table salary USING column id
         (take 10)                          ; Dont extract more than 10 hits
         distinct)                          ; Make the result distinct
    >>> ({:id 3 :name "Frank"} {:id 2 :name "Christophe"})

**Note:** This executes SQL statements 3 times in this order: conj!, disj!, @

Predicate building
------------------

ClojureQL provides a macro called **where** which allows you to write elegant predicates using
common mathematical operators as well as and/or. If you prefer to write out the compilable
datastructure yourself, import the functions from **predicate.clj** and use them without the where
macro. *There is no shadowing of clojure.core operators*

    (where (= 5 4)) expands to (=* 5 4)
    (where (< 5 4)) expands to (<* 5 4)
    (where (> 5 4)) expands to (>* 5 4)

And/or are also implemented. Refer to colums as keywords.

    (where (or (= :title "Admin") (>= :id 50)))

Strings are auto-quoted

    (where (= :title "Dev"))
    > "(title = 'Dev')"

Except when containing a parens

    (where (>= "avg(x.sales y.sales)" 500))
    > "(avg(x.sales y.sales) >= 500)"

The syntax for aggregates is the same as when defining columns: :function/field:fields

    (where (or (<= :avg/sales 500) (!= :max/expenses 250)))
    > "((avg(sales) <= 500) OR (max(expenses) != 250))"

    (where (>= :avg/x.sales:y.sales 500))
    > "(avg(x.sales,y.sales) >= 500)"

For more complicated aggregates, use strings. For this there is also 2 helper functions
available:

    (restrict "(%1 < %2) AND (avg(%1) < %3)" :income :cost :expenses)
    > "WHERE (income < cost) AND (avg(income) < expenses)"

    (restrict-not "(%1 < %2) AND (avg(%1) < %3)" :income :cost :expenses)
    > "WHERE not((income < cost) AND (avg(income) < expenses))"

License
=======

Eclipse Public License - v 1.0, see LICENSE.

Credit
======

ClojureQL is primarily developed by [Lau Jensen](http://twitter.com/laujensen) of
[Best In Class](http://www.bestinclass.dk).

Large and **significant** contributions to both the design and codebase have been
rendered by [Justin Balthrop](http://twitter.com/ninjudd) aka. ninjudd author
of the powerful build tool [Cake](http://github.com/ninjudd/cake).

In addition, the following people have made important contributions to ClojureQL:

   - Roman Scherer (r0man)
   - Christian Kebekus (ck)
