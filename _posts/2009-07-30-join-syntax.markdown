---
layout: default
title: Join Syntax
---

## The JOIN combinator ##

To form a join you first define a special [query][]. The query should
consist of a query on two tables. You can specify any columns and
predicates as you wish.

Then to form a JOIN pass this query on to the `join` combinator.
Tell it on which two columns you actually want to do the JOIN.

        {% highlight clojure %}
        (let [the-query (query [foo.a foo.x bar.b bar.c]
                               [foo bar]
                               (> bar.c 35))]
          (join the-query :inner [foo.a bar.a]))
        {% endhighlight %}

The JOIN type may be one of `:inner`, `:left`, `:right` or `:full`.
Note that eg. a FULL JOIN is emulated on backends, which don't support
it. These are for example Derby and MySQL.

[query]: http://lau-of-dk.github.com/clojureql/2009/07/13/query-syntax.html
