(in-ns 'clojureql.core)

(defn- combination-op [combination]
  (->> [(:type combination) (:mode combination)]
       (remove nil?)
       (map name)
       (join-str " ")
       upper-case))

(defn- append-combination [type relation-1 relation-2 & [mode]]
  (assoc relation-1
    :combination
    (if-let [combination (:combination relation-1)]
      {:relation (append-combination type (:relation combination) relation-2 mode)
       :type (:type combination)
       :mode (:mode combination)}
      {:relation relation-2 :type type :mode mode}) ))

(defn- append-combinations [type relation relations & [mode]]
  (reduce #(append-combination type %1 %2 mode)
          relation (if (vector? relations) relations [relations])))

(defn build-join
  "Generates a JOIN statement from the joins field of a table"
  [{[tname pred] :data type :type pos :position} aliases]
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
                          (compile tname :default))]
    [(assemble-sql "%s %s JOIN %s %s %s"
       (if (keyword? pos)  (-> pos name .toUpperCase) "")
       (if (not= :join type) (-> type name .toUpperCase) "")
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
  (let [{:keys [cnx tname tcols restriction renames joins
                grouped-by limit offset order-by modifiers]} tble
        aliases   (when joins (extract-aliases joins))
        mods      (join-str \space (map upper-name modifiers))
        combination (if (:combination tble) (compile (:relation (:combination tble)) :default))
        fields    (when-not (table? tcols)
                    (str (if tcols (to-fieldlist tname tcols) "*")
                         (when (seq aliases)
                           (str ","
                                (->> (map rest aliases)
                                     (map #(join-str "," %))
                                     (apply str))))))
        jdata     (when joins
                    (for [join-data joins] (build-join join-data aliases)))
        tables    (cond
                   joins
                    (str (if renames
                           (with-rename tname (map #(add-tname tname %) tcols) renames)
                           (to-tablename tname))
                         \space
                         (join-str " " (map first jdata)))
                    (table? tcols)
                    (compile tcols nil)
                    :else
                    (if renames
                      (with-rename tname (map #(add-tname tname %) tcols) renames)
                      (to-tablename tname)))
        preds     (when restriction restriction)
        statement (clean-sql ["SELECT" mods (or fields "*")
                       (when tables "FROM") (if (string? tables)
                                              tables
                                              (format "(%s)" (first tables)))
                       (when preds "WHERE") (str preds)
                       (when (seq order-by) (str "ORDER BY " (to-orderlist tname order-by)))
                       (when grouped-by     (str "GROUP BY " (to-fieldlist tname grouped-by)))
                       (when limit          (str "LIMIT " limit))
                       (when offset         (str "OFFSET " offset))
                       (when combination    (str (combination-op (:combination tble))
                                                 \space
                                                 (first combination)))])
        env       (concat
                   (->> [(map (comp :env last) jdata)
                         (if (table? tcols) (rest tables))
                         (if preds [(:env preds)])]
                        flatten (remove nil?) vec)
                   (rest combination))
        sql-vec   (into [statement] env)]
    (when *debug* (prn sql-vec))
    sql-vec))
