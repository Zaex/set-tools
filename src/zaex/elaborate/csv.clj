(ns zaex.elaborate.csv)

(defn csv>rel
  "assumes first row is column names"
  ([[columns & rows]]
   (csv>rel columns rows))
  ([columns rows & {:keys [column-fn]
                    :or {column-fn keyword}}]
   (let [cols (map column-fn columns)]
     (->> rows
          (into #{} (map (partial zipmap cols)))))))
                
(defn rel>csv
  "assumes all rows have the same columns"
  [[{:as columns} :as rel] & {:keys [column-fn]
                              :or {column-fn name}}]
  (let [cols (mapv column-fn (keys columns))]
    (->> rel
         (into [cols] (map (comp vec vals))))))


