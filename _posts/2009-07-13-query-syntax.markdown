---
layout: default
title: Query Syntax
---

## General Form ##

All query creating forms, which involve columns, are macros which quote
their arguments. So besides strings and keywords you may specify also
symbols as column names.

        {% highlight clojure %}
        (query ["a" :b c] [table])
        {% endhighlight %}

If there is only one column or one table in the query the vector can be
dropped.

        (query * table)

Column names follow the usual SQL form of @column@ or @table.column@.

        (query ["table.a" :table.b table.c] table)

If there are several columns from the same table, the prefix might
be abbreviated similar to the list form of require and friends. Simply
provide the table name followed by @:cols@ and the desired columns.

        (query [[table1 :cols a b] [table2 :cols x y]] [table1 table2])

This is equivalent to the following.

        (query [table1.a table1.b table2.x table2.y] [table1 table2])

## Injecting Values ##

Since the queries are quoted, one cannot simply name a variable.

To inject a value from the environment one must use the known @~@ to
unquote parts of the form. So in the following example @x@ is replaced
with @"a"@ in the query.

        (let [x "a"]
          (query [~x :b c] table))

## Conditions ##

Conditions on the query (aka *WHERE*) can be specified as a third
argument to the query form. The usual predicates like `=`, `<>`,
`like` and friends are available. Also `and` and `or`.

        (let [x 6]
          (query [a b c] table (and (< a 5) (= b ~x))))

## Aliases ##

Using the `:as` notation columns and tables can be assigned an alias.
The alias can be used as a shorthand notation.

        (query [[t1 :cols [aaaaaaa :as a] [bbbbbbb :as b]]
                [t2 :cols [xxxxxxx :as x] [yyyyyyy :as y]]]
               [[taaaaaaable1 :as t1] [taaaaaaable2 :as t2]]
               (or (> a 5) (<= y 7)))

Note: the column/table must be wrapped in a vector. Also the optional
vector for on single columns/tables becomes mandatory.

        (query [t :cols a b c] [[table :as t]])
