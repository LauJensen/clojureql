(in-ns 'clojureql.core)

(def global-connections (atom {}))

(defn open-global
  "Opens a global connection with the supplied specs. If given a
  conn-name, use it as a key to access that connection, else set the
  default global connection."
  ([specs] (open-global ::clojureql.internal/default-connection specs))
  ([conn-name specs]
     (let [con (jdbcint/get-connection specs)]
       (when-let [ac (-> specs :auto-commit)]
         (.setAutoCommit con ac))
       (swap! global-connections assoc conn-name {:connection con :opts specs}))))

(defn close-global
  "Supplied with a keyword identifying a global connection, that
  connection is closed and the reference dropped. When called without
  argument, close the default global connection."
  [& [conn-name]]
  (let [conn-name (or conn-name ::clojureql.internal/default-connection)]
    (if-let [conn (conn-name @global-connections)]
      (do
        (.close (:connection conn))
        (swap! global-connections dissoc conn-name)
        true)
      (throw
       (Exception.
        (format "No global connection by that name is open (%s)" conn-name))))))

(defmacro with-cnx
  "For internal use only. If you want to wrap a query in a connection, use
   with-connection"
  [db-spec & body]
  `(with-cnx* ~db-spec (fn [] ~@body)))

(defn with-cnx*
  "Evaluates func in the context of a database connection, with
  following precedence: Already open connection > connection passed on
  table > global connection"
  [con-info func]
  (io!
   (cond

    (find-connection*) ; an already open c.j.jdbc connection takes precedence
    (func)
    
    (map? con-info) ; then we try a passed connection info (presumably associated with some table in the query)
    (with-open [con (jdbcint/get-connection con-info)]
      (binding [jdbcint/*db*
                (assoc jdbcint/*db*
                  :connection con
                  :level 0
                  :rollback (atom false)
                  :opts     con-info)]
        (.setAutoCommit con (:auto-commit con-info true))
        (func)))

    :default ; try global connection
    (if-let [con (@clojureql.core/global-connections
                  (or con-info :clojureql.internal/default-connection))]
      (binding [jdbcint/*db*
                (assoc jdbcint/*db*
                  :connection (:connection con)
                  :level 0
                  :rollback (atom false)
                  :opts     (:opts con))]
        (func))
      (throw
       (Exception.
        "Missing connection information; no global connection :"))))))
