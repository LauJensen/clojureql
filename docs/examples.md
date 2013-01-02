# Example code

Below you will find illustrative examples. In addition we recommend
viewing the screencast which accompanied the beta1 release of
*ClojureQL* found here - The content is dated but still relevant, with
only two exceptions: `to-sql` is now renamed to `compile` and
`with-results` has an updated syntax which you can see under documentation.

## Select all

```clojure
user=> (table :users)
SELECT users.* FROM users
```

The simplest of all queries. When reading, disregard the `user=>` part
as it simply demonstrates that these queries are executed in a REPL.

## Select certain columns

```clojure
user=> (-> (table :users)
           (project [:id :name :email]))
SELECT users.id,users.name,users.email FROM users
```

Project is a non-additive way to specify which columns to include in
the select.

## Sort the results

```clojure
user=> (-> (table :users)
           (sort [:id]))
SELECT users.* FROM users ORDER BY id asc
```

If you do not specify sorting order (`asc` / `desc`) then `asc` is assumed.

### Sort in descending order

```clojure
user=> (-> (table :users)    
           (sort [:id#desc]))
SELECT users.* FROM users ORDER BY users.id desc
```

### Multiple calls to sort

```clojure
user=> (-> (table :users)
           (sort [:id#asc])
           (sort [:id#desc]))
SELECT * FROM (SELECT users.* FROM users ORDER BY users.id asc) ORDER
BY users.id desc
```

`sort`, `take` and `limit` spawn subselect when called multiple times on the
same table.

## Limit / Offset

```clojure
user=> (-> (table :users)         
           (take 5)
           (drop 2))
SELECT users.* FROM users LIMIT 3 OFFSET 2
```

## Distinct

```clojure
user=> (-> (table :users) distinct)
SELECT DISTINCT users.* FROM users
```

Distinct is mostly interesting because of its simple implementation,
demonstrating how simple it is for users to extend *ClojureQL*. Here
is a sample implementation:

```clojure
(defn distinct
  "A distinct which works on both tables and collections"
  [table]
  (modify obj :distinct))
```

## Aggregates

```clojure
user=> (-> (table :users) 
           (aggregate [[:count/* :as :cnt]] [:id]))
SELECT users.id,count(*) AS cnt FROM users GROUP BY users.id
```

Aggregates accepts an optional second argument which is a vector
containing the column names to group by. Above you also see the syntax
for aliasing.
