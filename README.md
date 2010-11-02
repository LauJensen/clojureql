ClojureQL
=========

ClojureQL is an abstraction layer sitting on top of standard low-level JDBC SQL integration.
It let's you interact with a database through a series of objects which work as Clojure data
type.

ClojureQL provides little to no assistance in creating specialized query strings, so that
compatability with the database backend is left to the user.

This project is still in the pre-alpha design phase, input is welcomed!


Query
-----

    (def users (table connection-info :users ["*"]))  ; All columns in table 'users'

    @users
    > ({:id 1 :name "Lau"} {:id 2 :name "Christophe"} {:id 3 :name "Frank"})

    (select users (where "id=1"))
    > ({:id 1 :name "Lau"})

    (select users (where "id=1" :invert))
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

    (where "(%1 < %2) AND (avg(%1) < %3)" :income :cost :expenses)
    > " WHERE (income < cost) AND (avg(income) < expenses)"

    (where "(%1 < %2) AND (avg(%1) < %3)" :income :cost :expenses :invert)
    > " WHERE not((income < cost) AND (avg(income) < expenses))"

    (-> (where "id > 2") (group-by :name))
    > "WHERE id > 2 GROUP BY name"

    (-> (where "id > 2") (order-by :name))
    > "WHERE id > 2 ORDER BY name"

    (-> (where "id > 2") (having "id=%1 OR id=%2" 3 5))
    > "WHERE id > 2 HAVING id=3 OR id=5"
