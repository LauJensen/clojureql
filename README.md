ClojureQL
=========

ClojureQL is an abstraction layer sitting on top of standard low-level JDBC SQL integration.
It lets you interact with a database through a series of objects which work as Clojure data
types.

ClojureQL is modeled around the primitives defined in Relational Algebra.
http://en.wikipedia.org/wiki/Relational_algebra

For the user, this means that all queries compose and are never executed unless dereferenced
or called with a function that has the ! suffix.

As a help for debugging, wrap your statements in (binding [\*debug\* true]) to see the
compiled SQL statement printed to stdout.

Installation
============

Add the following to your **project.clj** or pom.xml:

Cake/Lein artifact:

    [clojureql "1.0.4"]

Maven:

    <dependency>
      <groupId>clojureql</groupId>
      <artifactId>clojureql</artifactId>
      <version>1.0.4</version>
    </dependency>

Then execute

    cake deps

And import the library into your namespace

    (:use clojureql.core)


Manual
============

Please visit [ClojureQL.org](http://www.clojureql.org) for updated documentation.

Credit
======

ClojureQL is primarily developed by [Lau Jensen](http://twitter.com/laujensen) of
[Best In Class](http://www.bestinclass.dk).

Large and **significant** contributions to both the design and codebase have been
rendered by [Justin Balthrop](http://twitter.com/ninjudd) aka. ninjudd author
of the powerful build tool [Cake](http://github.com/ninjudd/cake).

In addition, the following people have made important contributions to ClojureQL:

   - Roman Scherer      (r0man)
   - Christian Kebekus  (ck)
   - Herwig Hochleitner (bendlas)

License
=======

Eclipse Public License - v 1.0, see LICENSE.
