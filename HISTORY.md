## November 25th 2010

ClojureQL now released as beta2

* All queries are automatically parameterized
* to-sql returns a vector with the parameterized query
* Predicate compiler is now a record which captures the environment
* Is protected from SQL injection attacks

## November 18th 2010

ClojureQL now released as beta1

* First public release
* All interfaces should be final
* For constructing queries: table, select, where, project, join, outer-join, rename, aggregate, limit, offset
* For manipulation: conj!, disj!, update-in!
* Helpers: with-results, with-connection, restrict
* Fully working demo.clj
