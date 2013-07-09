(in-ns 'clojureql.core)

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
        [subselect & env]
        (when (requires-subselect? tname)
          (compile tname (or dialect :default)))]
    [(assemble-sql "%s %s JOIN %s %s %s"
       (if (keyword? pos)  (-> pos name .toUpperCase) "")
       (if (not= :join type) (-> type name .toUpperCase) "")
       (if (requires-subselect? tname)
         (assemble-sql (if (map? (:tname tname))
                                 "(%s) AS %s"
                                 "(%s) AS %s_subselect") subselect
                       (if (map? (:tname tname))
                         (-> (:tname tname) vals last nskeyword)
                         (to-tablename (:tname tname))))
         (to-tablename tname))
       (if-not (keyword? pred) " ON " "")
       (if (keyword? pred)
         (format " USING(%s) " (name pred))
         (-> (str pred) (apply-aliases aliases) first)))
     (if (and subselect (map? pred))
       (assoc pred :env (into (:env pred) env))
       pred)
     env]))

(defmethod compile :default [tble db]
  (let [{:keys [cnx tname tcols restriction renames joins combinations
                grouped-by pre-scope scope order-by modifiers having]} tble
        aliases    (when joins (extract-aliases joins))
        aggregates (-?>> (if (table? tcols) (:tcols tcols) tcols)
                         (filter #(and (vector? %) (= 3 (count %))))
                         (map (comp name last)))
        mods       (join-str \space (map upper-name modifiers))
        combs      (if (seq combinations)
                     (for [{:keys [table mode opts]} combinations]
                       (let [[stmt & [& env]] (compile table (or (:dialect cnx) :default))]
                         (into [(format " %s (%s)"
                                  (str (upper-name mode) (if opts (str \space (upper-name opts))))
                                  stmt)] env))))
        fields     (when-not (table? tcols)
                     (str (if tcols (to-fieldlist tname tcols) "*")
                          (when (seq aliases)
                            (str ","
                                 (->> (map rest aliases)
                                      flatten
                                      (join-str ","))))))
        jdata      (when joins
                     (for [join-data joins]
                       (build-join (:dialect cnx) join-data aliases)))
        tables    (cond
                   joins
                    (str (if renames
                           (with-rename tname (map #(add-tname tname %) tcols) renames)
                           (to-tablename tname))
                         \space
                         (join-str " " (map first jdata)))
                    (table? tcols)
                    (compile tcols (or (:dialect cnx) :default))
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
                       (when (seq having) (str "HAVING " having))
                       (when (seq pre-order)
                         (str "ORDER BY " (to-orderlist tname aggregates (first pre-order))))
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
                       (when (and (seq having) (seq combs)) (str "HAVING " having))
                       (when (seq post-order)
                         (str "ORDER BY " (to-orderlist tname :all (first post-order))))
                       (when-let [limit (-> scope :limit)]
                         (str "LIMIT " limit))
                       (when-let [offset (-> scope :offset)]
                         (str "OFFSET " offset))
                       ])
        env       (concat
                   (->> [(if-let [cases (filter map? tcols)]
                           (interleave (mapcat #(mapcat (fn [clause] (:env clause)) %)
                                               (map :clauses cases))
                                       (mapcat :returns cases)))
                         (mapcat last jdata)
                         (map :else (filter map? tcols))
                         ; Not sure why jdata is referenced twice; see issue #138
                         ;(map (comp :env second) jdata)
                         (if (table? tcols) (rest tables))
                         (if preds [(:env preds)])
                         (if having [(:env having)])]
                        flatten (remove nil?) vec)
                   (->> (mapcat rest combs)
                        (remove nil?)))
        sql-vec   (into [statement] env)]
    (when *debug* (prn sql-vec))
    sql-vec))
