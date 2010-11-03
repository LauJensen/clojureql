ClojureQL
=========

ClojureQL is an abstraction layer sitting on top of standard low-level JDBC SQL integration.
It let's you interact with a database through a series of objects which work as Clojure data
type.

ClojureQL provides little to no assistance in creating specialized query strings, so that
compatability with the database backend is left to the user.

To try out this library locally, simply clone the repo, load the core namespace and run
the fn (test-suite). Make sure you have a local MySQL installation with the user/psw combo
"cql", "cql" or change the global 'db'.

This project is still in the pre-alpha design phase, input is welcomed!

Query
-----

    (def users (table connection-info :users ["*"]))  ; All columns in table 'users'

    @users
    > ({:id 1 :name "Lau"} {:id 2 :name "Christophe"} {:id 3 :name "Frank"})

    (select users (where "id=%1" 1))
    > ({:id 1 :name "Lau"})

    (select users (where-not "id=1"))
    > ({:id 2 :name "Christophe"} {:id 3 :name "Frank"})

    (select users (where (both (>= {:id 2}) (= {:title "Dev"}))))
    > ()

    (select users (where (either (= {:name "Lau"}) (= {:title "Dev"}))))
    > ({:id 1 :name "Lau" :title "Dev"} {:id 4 :name "Frank" :title "Dev"})

Aggregates
----------

    (def salary (table connection-info :salary [[:avg.wage :as :avg]]))

    @salary
    > ({:wage 150.00000M})

Manipulation
------------

    @(conj! users {:name "Jack"})
    > ({:id 1 :name "Lau"} {:id 2 :name "Christophe"} {:id 3 :name "Frank"} {:id 4 :name "Jack"})

    @(disj! users {:name "Jack"})
    > ({:id 1 :name "Lau"} {:id 2 :name "Christophe"} {:id 3 :name "Frank"})

Compound ops
------------

    (-> (conj! users {:name "Jack"})
        (disj! {:name "Lau"})
        (sort :id :desc))
    > ({:id 3 :name "Frank"} {:id 2 :name "Christophe"})

Joins
------

    (def visitors (table connection-info :visitors [:id :guest]))

    (join users visitors #{users.id visitors.id})
    > ({:id 1 :name "Lau" :guest "false"} {:id 3 :name "Frank" :guest "true"})

Helpers
-------

    (where "(%1 < %2) AND (avg(%1) < %3)" :income :cost :expenses)
    > "WHERE (income < cost) AND (avg(income) < expenses)"

    (where-not (either (= {:id 4}) (>= {:wage 200})))
    > "WHERE not ((id = 4) OR (wage >= 200))"

    (where (both (= {:id 4}) (< {:wage 100})))
    > "WHERE ((id = 4) AND (wage < 100))"

    (where (either (= {:id 2}) (> {:avg.id 4})))
    > "WHERE (id = 2 OR avg(id) > 4)"

    (-> (where "id > 2") (group-by :name))
    > "WHERE id > 2 GROUP BY name"

    (-> (where "id > 2") (order-by :name))
    > "WHERE id > 2 ORDER BY name"

    (-> (where "id > 2") (having "id=%1 OR id=%2" 3 5))
    > "WHERE id > 2 HAVING id=3 OR id=5"
