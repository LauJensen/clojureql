;;; clojure.contrib.mock.clj: mocking/expectation framework for Clojure

;;  by Matt Clark

;;  Copyright (c) Matt Clark, 2009, 2010. All rights reserved.  The use and
;;  distribution terms for this software are covered by the Eclipse Public
;;  License 1.0 (http://opensource.org/licenses/eclipse-1.0.php) which can
;;  be found in the file epl-v10.html at the root of this distribution.  By
;;  using this software in any fashion, you are agreeing to be bound by the
;;  terms of this license.  You must not remove this notice, or any other
;;  from this software.
;;------------------------------------------------------------------------------

(ns ^{:author "Matt Clark"
      :doc "Mock is a function mocking utility inspired by the various ruby and
java mocking frameworks such as mockito, easymock, and rspec yet designed to
fit the functional style of clojure."
      :see-also [["Mocking with clojure.contrib.mock"
                  "http://notesonclojure.blogspot.com/2010/06/mocking-with-clojurecontribmock.html"]]}
  clojure.contrib.mock)

;; positions, ripped from clojure.contrib.seq

(defn indexed
  "Returns a lazy sequence of [index, item] pairs, where items come
  from 's' and indexes count up from zero.

  (indexed '(a b c d))  =>  ([0 a] [1 b] [2 c] [3 d])"
  [s]
  (map vector (iterate inc 0) s))

(defn positions
  "Returns a lazy sequence containing the positions at which pred
   is true for items in coll."
  [pred coll]
  (for [[idx elt] (indexed coll) :when (pred elt)] idx))

;;------------------------------------------------------------------------------
;; These are the error condition functions. Override them to integrate into
;; the test framework of your choice, or to simply customize error handling.

(defn report-problem
  {:dynamic true
   :doc "Override this to customize error handling. By default, prints an
error message to *out*."}
  ([function expected actual]
     (report-problem function expected actual "Expectation not met."))
  ([function expected actual message]
     (prn (str message " Function name: " function
               " expected: " expected " actual: " actual))))

(defn no-matching-function-signature
  {:dynamic true
   :doc "Override to customize unmatched function signature errors.
Prints an error message to *out* by default."}
  [function expected actual]
  (report-problem function expected actual
                  "No matching real function signature for given argument count."))

(defn unexpected-args
  {:dynamic true
   :doc "Override to customize unexpected argument errors. Prints an error
message to *out* by default."}
  [function expected actual i]
  (report-problem function expected actual
                  (str "Argument " i " has an unexpected value for function.")))

(defn incorrect-invocation-count
  {:dynamic true
   :doc "Override to customize incorrect invocation counts. Prints an error
to *out* by default."}
  [function expected actual]
  (report-problem function expected actual "Unexpected invocation count."))


;;------------------------------------------------------------------------------
;;  Internal Functions - ignore these


(defn- has-arg-count-match?
  {:doc "Given the sequence of accepted argument vectors for a function
returns true if at least one matches the given-count value."
   :skip-wiki true}
  [arg-lists given-count]
  (some #(let [[ind] (positions #{'&} %)]
           (if ind
             (>= given-count ind)
             (= (count %) given-count)))
        arg-lists))


(defn has-matching-signature?
  ^{:doc "Calls no-matching-function-signature if no match is found for the
given function. If no argslist meta data is available for the function, it is
not called."
    :skip-wiki true}
  [fn-name args]
  (let [arg-count (count args)
        arg-lists (:arglists (meta (resolve fn-name)))]
    (if (and arg-lists (not (has-arg-count-match? arg-lists arg-count)))
      (no-matching-function-signature fn-name arg-lists args))))


(defn make-arg-checker
  ^{:doc "Creates the argument verifying function for a replaced dependency
within the expectation bound scope. These functions take the additional
argument of the name of the replaced function, then the rest of their args.
It is designed to be called from the mock function generated in the first
argument of the mock info object created by make-mock."
    :skip-wiki true}
  [arg-preds arg-pred-forms]
  (let [sanitized-preds (map (fn [v] (if (fn? v) v #(= v %))) arg-preds)]
    (fn [fn-name & args]
      (every? true?
              (map (fn [pred arg pred-form i] (if (pred arg) true
                                                  (unexpected-args fn-name
                                                                   pred-form arg i)))
                   sanitized-preds args arg-pred-forms (iterate inc 0))))))


(defn make-count-checker
  ^{:doc "creates the count checker that is invoked at the end of an
expectation, after the code under test has all been executed. The function
returned takes the name of the associated dependency and the invocation
count as arguments."
    :skip-wiki true}
  [pred pred-form]
  (let [pred-fn (if (integer? pred) #(= pred %) pred)]
    (fn [fn-name v] (if (pred-fn v) true
                        (incorrect-invocation-count fn-name pred-form v)))))

(defn make-mock
  ^{:doc "creates a vector containing the following information for the
named function:
1. dependent function replacement - verifies signature, calls arg checker
increases count, returns return value.
2. an atom containing the invocation count
3. the invocation count checker function
4. a symbol of the name of the function being replaced."
    :skip-wiki true}
  [fn-name expectation-hash]
  {:pre [(map? expectation-hash)
         (symbol? fn-name)]}
  (let [arg-checker (or (expectation-hash :has-args) (fn [& args] true))
        count-atom (atom 0)
        ret-fn (or
                (expectation-hash :calls)
                (fn [& args] (expectation-hash :returns)))]
    [(fn [& args]
       (has-matching-signature? fn-name args)
       (apply arg-checker fn-name args)
       (swap! count-atom inc)
       (apply ret-fn args))
     count-atom
     (or (expectation-hash :times) (fn [fn-name v] true))
     fn-name]))


(defn validate-counts
  ^{:doc "given the sequence of all mock data for the expectation, simply
calls the count checker for each dependency."
    :skip-wiki true}
  [mock-data] (doseq [[mfn i checker fn-name] mock-data] (checker fn-name @i)))

(defn- make-bindings
  ^{:skip-wiki true}
  [expect-bindings mock-data-sym]
  `[~@(interleave (map #(first %) (partition 2 expect-bindings))
                  (map (fn [i] `(nth (nth ~mock-data-sym ~i) 0))
                       (range (quot (count expect-bindings) 2))))])


;;------------------------------------------------------------------------------
;; These are convenience functions to improve the readability and use of this
;; library. Useful in expressions such as:
;; (expect [dep-fn1 (times (more-than 1) (returns 15)) etc)

;; best used in the times function
(defn once "Predicate which ensures argument equals 1. For times function."
  [x] (= 1 x))

(defn never "Predicate which ensures argument is zero. For times function."
  [x] (zero? x))

(defn more-than "Returns a predicate which ensures argument is more than x."
  [x] #(< x %))

(defn less-than "Returns a predicate which ensures argument is less than x."
  [x] #(> x %))

(defn between "Returns a predicate which ensures argument is between x and y."
  [x y] #(and (< x %) (> y %)))

(defn anything "Syntactic sugar for has-args function. A predicate which
always returns true."
  [x] true)


;;------------------------------------------------------------------------------
;; The following functions can be used to build up the expectation hash.

(defn ^{:arglists '([ret-value expectation-hash?])} returns
  "Creates or associates to an existing expectation hash the :returns key with
a value to be returned by the expectation after a successful invocation
matching its expected arguments (if applicable)."

  ([val] (returns val {}))
  ([val expectation-hash]
   (assoc expectation-hash :returns val)))


(defn ^{:arglists '([replacement-fn expectation-hash?])} calls
  "Creates or associates to an existing expectation hash the :calls key with a
function that will be called with the given arguments. The return value from
this function will be returned by the expected function. If both this
and :returns are specified, the return value of :calls will have precedence."

  ([val] (calls val {}))
  ([val expectation-hash]
   {:pre [(fn? val)]}
   (assoc expectation-hash :calls val)))


(defmacro ^{:arglists '([pred-coll expectation-hash?])} has-args
  "Creates or associates to an existing expectation hash the :has-args key with
a value corresponding to a function that will either return true if its
argument expectations are met or throw an exception with the details of the
first failed argument it encounters.
Only specify as many predicates as you are interested in verifying. The rest
of the values are safely ignored."

  ([arg-pred-forms] `(has-args ~arg-pred-forms {}))
  ([arg-pred-forms expectation-hash]
   {:pre [(vector? arg-pred-forms)]}
    `(assoc ~expectation-hash :has-args
       (make-arg-checker ~arg-pred-forms '~arg-pred-forms))))


(defmacro ^{:arglists '([n expectation-hash?])} times
  "Creates or associates to an existing expectation hash the :times key with a
value corresponding to a predicate function which expects an integer value.
Also, an integer can be specified, in which case the times will only be an
exact match. The times check is called at the end of an expect expression to
validate that an expected dependency function was called the expected
number of times."
  
  ([times-fn] `(times ~times-fn {}))
  ([times-fn expectation-hash]
   `(assoc ~expectation-hash :times (make-count-checker ~times-fn '~times-fn))))


;-------------------------------------------------------------------------------
; The main expect macro.
(defmacro expect
  "Use expect to redirect calls to dependent functions that are made within the
code under test. Instead of calling the functions that would normally be used
temporary stubs are used, which can verify function parameters and call counts.
Return values of overridden functions can also be specified as needed."

  [expect-bindings & body]
   {:pre [(vector? expect-bindings)
          (even? (count expect-bindings))]}
  (let [mock-data (gensym "mock-data_")]
    `(let [~mock-data (map (fn [args#]
                             (apply clojure.contrib.mock/make-mock args#))
                           ~(cons 'list (map (fn [[n m]]
                                               (vector (list 'quote n) m))
                                       (partition 2 expect-bindings))))]
       (with-redefs ~(make-bindings expect-bindings mock-data) ~@body)
       (clojure.contrib.mock/validate-counts ~mock-data) true)))
