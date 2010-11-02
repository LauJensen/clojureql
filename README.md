ClojureQL
=========

ClojureQL is an abstraction layer sitting on top of standard low-level JDBC SQL integration.
It let's you interact with a database through a series of objects which work as Clojure data
type.

ClojureQL provides little to no assistance in creating specialized query strings, so that
compatability with the database backend is left to the users.

This project is still in the pre-alpha design phase, input is welcomed!


Query
-----

    (def users (table connection-info :users ["*"]))  ; All columns in table 'users'

    @users
    > ({:id 1 :name "Lau"} {:id 2 :name "Christophe"} {:id 3 :name "Frank"})

    (pick users "id=1")
    > ({:id 1 :name "Lau"})

    (drop users "id=1")
    > ({:id 2 :name "Christophe"} {:id 3 :name "Frank"})

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
    ({:id 1 :name "Lau" :guest "false"} {:id 3 :name "Frank" :guest "true"})

Helpers
-------

    (sql-clause "(%1 < %2) AND (avg(%1) < %3)" :income :cost :expenses)
    > "(income < cost) AND (avg(income) < expenses)"

