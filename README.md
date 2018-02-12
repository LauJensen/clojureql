ClojureQL
=========

ClojureQL is an abstraction layer sitting on top of standard low-level JDBC SQL integration.
It lets you interact with a database through objects which work as Clojure data
types and thus can be composed and extended.

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

    [clojureql "1.0.5"]

Maven:

    <dependency>
      <groupId>clojureql</groupId>
      <artifactId>clojureql</artifactId>
      <version>1.0.5</version>
    </dependency>

Then execute

    cake deps or lein deps

And import the library into your namespace

    (:use clojureql.core)


Manual
============

Please visit [ClojureQL.SabreCMS.com](http://clojureql.sabrecms.com/en/welcome) for updated documentation.

Credit
======

ClojureQL was bootstrapped by Lau Jensen of [SabreCMS](https://www.sabrecms.com) and
[Best In Class](http://www.bestinclass.dk).

Large and **significant** contributions to both the design and codebase have been
rendered by [Justin Balthrop](http://twitter.com/ninjudd) aka. ninjudd author
of the powerful build tool [Cake](http://github.com/ninjudd/cake).

In addition, the following people have made important contributions to ClojureQL:

   - Roman Scherer      (r0man)
   - Christian Kebekus  (ck)
   - Herwig Hochleitner (bendlas)
   - And several others

If you want to pitch in, we're actively looking for more brainpower.

License
=======

Eclipse Public License - v 1.0, see LICENSE.
