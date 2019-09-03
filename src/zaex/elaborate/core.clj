(ns zaex.elaborate.core)

(defn merge!
  "behaves the same way as merge but assumes the first map is a transient."
  [& [m & ms :as maps]]
  (when (some identity maps)
    (reduce conj!
            (or m (transient {}))
            ms)))
  
(defn merge-with!
  "behaves the same way as merge-with but assumes the first map is a transient"
  [f & [m & ms :as maps]]
  (when (some identity maps)
    (letfn [(merge-entry! [m k v]
              (if (contains? m k)
                (assoc! m k (f (get m k) v))
                (assoc! m k v)))
            (merge2! [m1 m2]
              (reduce-kv merge-entry!
                         (or m1 (transient {}))
                         m2))]
      (reduce merge2! m ms))))

(persistent!
 (merge-with! +
              (transient {:a 1 :b 2 :c 4})
              {:a 2 :b 3}))

(persistent!
 (merge!
  (transient {:a 1 :b 2 :c 4})
  {:a 2 :b 3}))
