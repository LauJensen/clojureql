(ns clojureql.compiler
  ^{:author "Lau B. Jensen    <lau.jensen@bestinclass.dk>"
    :doc    "Please see the README.md for documentation"
    :url    "http://github.com/LauJensen/clojureql"}
  (:refer-clojure :exclude [compile])
  (:use
   [clojureql internal]
   [clojure.string :only [join upper-case] :rename {join join-str}]))

(defmulti compile
  (fn [table db] (:dialect db)))

(defn build-join
  "Generates a JOIN statement from the joins field of a table"
  [dialect {[tname pred] :data type :type pos :position} aliases]
  (let [pred (if (and (seq aliases) (string? pred))
               (reduce (fn [acc a]
                         (let [t1name (if (or (keyword? tname) (string? tname))
                                        (to-tablename tname)
                                        (to-tablename (:tname tname)))
                               alias  (-> (.split a "\\.") first)]
                             acc))
                       pred (map last aliases))
               pred)
        [subselect env] (when (requires-subselect? tname)
                          (compile tname (or dialect :default)))]
    [(assemble-sql "%s %s JOIN %s %s %s"
       (if (keyword? pos)  (-> pos name upper-case) "")
       (if (not= :join type) (-> type name upper-case) "")
       (if (requires-subselect? tname)
         (assemble-sql "(%s) AS %s_subselect" subselect
                       (to-tablename (:tname tname)))
         (to-tablename tname))
       (if-not (keyword? pred) " ON " "")
       (if (keyword? pred)
         (format " USING(%s) " (name pred))
         (-> (str pred) (apply-aliases aliases) first)))
     (if (and subselect (map? pred))
       (assoc pred :env (into (:env pred) env))
       pred)]))

(defmethod compile :default [tble db]
  (let [{:keys [cnx tname tcols restriction renames joins combinations
                grouped-by pre-scope scope order-by modifiers subtable]} tble
        aliases   (when joins (extract-aliases joins))
        mods      (join-str \space (map upper-name modifiers))
        combs     (if (seq combinations)
                    (for [{:keys [table mode opts]} combinations]
                      (let [[stmt & [env]] (compile table (or (:dialect cnx) :default))]
                        [(format " %s (%s)"
                                 (str (upper-name mode) (if opts (str \space (upper-name opts))))
                                 stmt) env])))
        fields    (str (if tcols (to-fieldlist tname tcols) "*")
                       (when (seq aliases)
                         (str ","
                              (->> (map rest aliases)
                                   flatten
                                   (join-str ",")))))
        jdata     (when joins
                    (for [join-data joins]
                      (build-join (:dialect cnx) join-data aliases)))
        tables    (cond
                   joins
                   (str (if renames
                          (with-rename tname (map #(add-tname tname %) tcols) renames)
                          (to-tablename tname))
                        \space
                        (join-str " " (map first jdata)))
                   subtable
                   (compile subtable (or (:dialect cnx) :default))
                   :else
                   (if renames
                     (with-rename tname (map #(add-tname tname %) tcols) renames)
                     (to-tablename tname)))
        pre-order  (filter #(true? (-> % meta :prepend)) order-by)
        post-order (remove #(true? (-> % meta :prepend)) order-by)
        preds     (when restriction restriction)
        statement (clean-sql [(when combs "(")
                       "SELECT" mods (or fields "*")
                       (when tables "FROM") (if (string? tables)
                                              tables
                                              (format "(%s)" (first tables)))
                       (when preds "WHERE") (str preds)

                       (when (or (and (seq grouped-by) (not (seq combs)))
                                 (-> grouped-by first meta :prepend))
                         (str "GROUP BY " (to-fieldlist tname (first grouped-by))))
                       (when (seq pre-order)
                         (str "ORDER BY " (to-orderlist tname (first pre-order))))
                       (when-let [limit (-> pre-scope :limit)]
                         (str "LIMIT " limit))
                       (when-let [offset (-> pre-scope :offset)]
                         (str "OFFSET " offset))

                       (when combs
                         (->> (map first combs) (interpose \space)
                              (apply str)       (format ") %s")))

                       (when (and (seq grouped-by) (seq combs)
                                  (nil? (-> order-by first meta :prepend)))
                         (str "GROUP BY " (to-fieldlist tname (first grouped-by))))
                       (when (seq post-order)
                         (str "ORDER BY " (to-orderlist tname (first post-order))))
                       (when-let [limit (-> scope :limit)]
                         (str "LIMIT " limit))
                       (when-let [offset (-> scope :offset)]
                         (str "OFFSET " offset))
                       ])
        env       (concat
                   (->> [(map (comp :env last) jdata)
                         (when subtable (rest tables))
                         (when preds [(:env preds)])]
                        flatten (remove nil?) vec)
                   (->> (mapcat rest combs)
                        (remove nil?)))
        sql-vec   (into [statement] env)]
    sql-vec))
