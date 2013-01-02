# General

ClojureQL is designed to have a consistent uniform API. The following
rules are always guaranteed:

* All functions which work on tables return a new table. This allows
  you to compound multiple operations.
* Any function which name ends in a bang (!) forces execution of the query
* All functions take the table as their first argument, allowing use
  of the -> threading operator
* In all places where you specificy columns can you use both keywords
  and strings, however certain compiler optimizations are only
  available when using keywords.
* To execute a query you must dereference it. Here are two equivalent
  examples of dereferencing:

```clojure
@(table db :t1)

(deref (table db :t1))
```

## Column specification syntax

ClojureQL uses a bit of syntax to keep your queries concise and
elegant. Below are example conversions of keywords to SQL column
specifications. (note: It is not necessary to learn this syntax as
strings can be used instead, however it is recommended)

<table>
  <thead>
    <td>Input</td>
    <td>Output</td>
  </thead>
  <tr>
    <td><code>:table</code></td>
    <td>"table"</td>
  </tr>
  <tr>
    <td><code>:table.col</code></td>
    <td>"table.col"</td>
  </tr>
  <tr>
    <td><code>:function/col</code></td>
    <td>"function(col)"</td>
  </tr>
  <tr>
    <td><code>:function/col1:col2</code></td>
    <td>"function(col1, col2)"</td>
  </tr>
  <tr>
    <td><code>:col#asc</code></td>
    <td> "col asc"</td>
  </tr>
  <tr>
    <td><code>:col#desc</code></td>
    <td>"col desc"</td>
  </tr>
</table>

## Connection specification

Example
```clojure
(def db
 {:classname   "com.mysql.jdbc.Driver"
  :subprotocol "mysql"
  :user        "cql"
  :password    "cql"
  :subname     "//localhost:3306/cql"})
```
Complete specification

### DriverManager:
`:classname`   (required) a `String`, the jdbc driver class name.  
`:subprotocol` (required) a `String`, the jdbc subprotocol.  
`:subname`     (required) a `String`, the jdbc subname.  
`(others)`     (optional) passed to the driver as properties.  

### DataSource:
`:datasource`  (optional) a `javax.sql.DataSource`  
`:username`    (optional) a `String`  
`:password`    (optional) a `String`, required if `:username` is supplied  

### JNDI:
`:name`        (optional) a String or `javax.naming.Name`  
`:environment` (optional) a `java.util.Map`  

### Options:
`:auto-commit` (optional) a `Boolean`  
`:fetch-size`  (optional) an integer  

## Public functions for composing queries

### table (args: table-name, or: connection-info, table-name)

Creates a table object optionally associated with a connection
specification. This can be either a keyword from a previous call to
(`open-global`) or a `hash-map`.

### select (args: this, predicate)

Confines the query to rows for which the predicate is true

Ex. 
```clojure
 (select (table :users) (where (= :id 5)))
```

### project (args: this, fields)

Confines the query to the fieldlist supplied in fields

Ex. 
```clojure
(project (table :users) [:email])
```

### take (args: table, n)

Limits the query to n number of rows

### drop (args: table, n)

Skips n number of rows

### sort (args: table, spec)

Sorts a table as per the definition given in spec.

Ex. 
```clojure
(sort (table :t1) [:id :rank]) ; implicitly ASC

(sort (table :t2) [:id#asc :rank#desc])
```

### distinct (args: table)

Makes the query distinct

### join (args: this, table2, join_on)

Joins two tables on join_on

Ex. 
```clojure
(join (table :one) (table :two) :id)

(join (table :one) (table :two) (where (= :one.col :two.col)))
```

### outer-join (args: this, table2, type, join_on)

Joins two tables on join_on and sets the direction of the join. type
can be `:right`, `:left`, `:full` etc. Backend support may vary.

Ex. 
```clojure
(outer-join (table :one) (table :two) :left :id)

(outer-join (table :one) (table :two) :left (where (= :one.id :two.id)))
```

### rename (args: this, newnames)

Renames colums when joining. Newnames is a map of replacement pairs

Ex. 
```clojure
(-> (join (table :one) (table :two) :id)

(project [:id]) (rename {:one.id :idx}))
```

### aggregate (args: either (this, aggregates) or (this aggregates, group-by))

Selects aggregates from a table. Aggregates are denoted with the
`:function/field` syntax. They can be aliased by supplying a vector
`[:function/field :as :myfn]`. Optionally accepts a group-by argument

Ex. 
```clojure
(-> (table :one) (aggregate [[:count/* :as :cnt]] [:id]))
```

### modify (args: this, modifiers)

Allows for arbitrary modifiers to be applied on the result. Can either
be called directly or via helper interfaces like `distinct`.

Ex. 
```clojure
(-> (table :one) (modify \"TOP 5\")) ; MSSqls special LIMIT syntax (-> (table :one) distinct)
```

### difference (args: this, relations, opts)

Selects the difference between tables. Mode can take a keyword which
can be anything which your backend supports. Commonly `:all` is used
to allow duplicate rows.

Ex. 
```clojure
(-> (table :one) (difference (table :two) :all))
```

### intersection (args: this, relations, opts)

Selects the intersection between tables. Mode can take a keyword which
can be anything which your backend supports. Commonly `:all` is used to
allow duplicate rows.

Ex. 
```clojure
(-> (table :one) (intersection (table :two) :all))
```

### union (args: this, relations, opts)

Selects the union between tables. Mode can take a keyword which can be
anything which your backend supports. Commonly `:all` is used to allow
duplicate rows.

Ex. 
```clojure
(-> (table :one) (union (table :two) :all))
```

## Table manipulation functions

### conj! (args: this, records)

Inserts record(s) into the table

Ex. 
```clojure
(conj! (table :one) {:age 22})

(conj! (table :one) [{:age 22} {:age 23}])
```

### disj! (args: this, predicate)

Deletes record(s) from the table

Ex.
```clojure
(disj! (table :one) (where (= :age 22)))
```

### update-in! (args: this, pred, records)

Inserts or updates record(s) where `pred` is `true`. Accepts records
as both maps and collections.

Ex. 
```clojure
(update-in! (table :one) (where (= :id 5)) {:age 22})
```

## Helper functions

### pick! (args: table, keyword)

For queries where you know only a single result will be returned,
`pick!` calls the keyword on that result. You can supply multiple
keywords in a collection. Returns nil for no-hits, throws an exception
on multiple hits.

Ex. 
```clojure
(-> (table :users) (select (where (= :id 5))) ; We know this will only match 1 row 
     (pick! :email))
```

### where (args: clause)

Constructs a where-clause for queries.

Ex. 
```clojure
(where (or (< :a 2) (>= :b 4))) => \"((a < ?) OR (b >= ?))\"
```

If you call str on the result, you'll get the above. If you call
(`:env`) you will see the captured environment

Use as: `(select tble (where ...))`

### with-results (args: [results, table])

Executes the body, wherein the results of the query can be accessed
via the name supplies as results.

Ex. 
```clojure
(with-results [res table]
 (println res))
```
