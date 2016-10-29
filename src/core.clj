(ns zaex.rel-tools
  (:require [clojure.set :refer [intersection difference project rename rename-keys map-invert index union]]))

(defn join
  "Most of the source has been shamelessly taken from clojure.set/join. Added 1-arty and transient support"
  ([rel] rel)
  ([xrel yrel] ;natural join
   (if (and (seq xrel) (seq yrel))
     (let [ks (intersection (set (keys (first xrel))) (set (keys (first yrel))))
           idx (index xrel ks)]
       (persistent!
         (reduce (fn [ret x]
                   (if-let [found (idx (select-keys x ks))]
                     (reduce #(conj! %1 (merge %2 x)) ret found)
                     ret))
                 (transient #{}) yrel)))
     #{}))
  ([xrel yrel km] ;arbitrary key mapping
   (let [k (map-invert km)
         idx (index xrel (vals k))]
     (persistent!
       (reduce (fn [ret x]
                 (if-let [found (idx (rename-keys (select-keys x (keys k)) k))]
                   (reduce #(conj! %1 (merge %2 x)) ret found)
                   ret))
               (transient #{}) yrel)))))

(defn left-join
  "Most of the source has been shamelessly taken from clojure.set/join. Added 1-arty and transient support"
  ([rel] rel)
  ([xrel yrel] ;natural join
   (if (and (seq xrel) (seq yrel))
     (let [ks (intersection (set (keys (first xrel))) (set (keys (first yrel))))
           idx (index yrel ks)]
       (persistent!
         (reduce (fn [ret x]
                   (if-let [found (idx (select-keys x ks))]
                     (reduce #(conj! %1 (merge %2 x)) ret found)
                     (conj! ret x)))
                 (transient #{}) xrel)))
     #{}))
  ([xrel yrel km] ;arbitrary key mapping
   (let [idx (index yrel (vals km))]
     (persistent!
       (reduce (fn [ret x]
                 (if-let [found (idx (rename-keys (select-keys x (keys km)) km))]
                   (reduce #(conj! %1 (merge %2 x)) ret found)
                   (conj! ret x)))
               (transient #{}) xrel)))))

(defn left-outer-join
  "Most of the source has been shamelessly taken from clojure.set/join. Added 1-arty and transient support"
  ([rel] rel)
  ([xrel yrel] ;natural join
   (if (and (seq xrel) (seq yrel))
     (let [ks (intersection (set (keys (first xrel))) (set (keys (first yrel))))
           idx (index yrel ks)]
       (into #{}
             (filter #(nil? (idx (select-keys % ks))))
             xrel))
     #{}))
  ([xrel yrel km] ;arbitrary key mapping
   (let [idx (index yrel (vals km))]
     (into #{}
           (filter #(nil? (idx (rename-keys (select-keys % (keys km)) km))))
           xrel))))

(defn right-join
  "Most of the source has been shamelessly taken from clojure.set/join. Added 1-arty and transient support"
  ([rel] rel)
  ([xrel yrel] ;natural join
   (left-join yrel xrel))
  ([xrel yrel km] ;arbitrary key mapping
   (left-join yrel xrel (map-invert km))))

(defn right-outer-join
  "Most of the source has been shamelessly taken from clojure.set/join. Added 1-arty and transient support"
  ([rel] rel)
  ([xrel yrel] ;natural join
   (left-outer-join yrel xrel))
  ([xrel yrel km] ;arbitrary key mapping
   (left-outer-join yrel xrel (map-invert km))))


(defn outer-join
  "Most of the source has been shamelessly taken from clojure.set/join. Added 1-arty and transient support"
  ([rel] rel)
  ([xrel yrel] ;natural join
   (if (and (seq xrel) (seq yrel))
     (let [ks (intersection (set (keys (first xrel))) (set (keys (first yrel))))
           idx (index xrel ks)]
       (apply union
              (map persistent!
                   (reduce (fn [[ret notfound] x]
                             (if-let [found (idx (select-keys x ks))]
                               [ret (reduce #(disj! % %2) notfound found)]
                               [(conj! ret x) notfound]))
                           [(transient #{}) (transient (set xrel))] yrel))))
     #{}))
  ([xrel yrel km] ;arbitrary key mapping
   (let [k (map-invert km)
         idx (index xrel (vals k))]
     (apply union
            (map persistent!
                 (reduce (fn [[ret notfound] x]
                           (if-let [found (idx (select-keys x k))]
                             [ret (reduce #(disj! % %2) notfound found)]
                             [(conj! ret x) notfound]))
                         [(transient #{}) (transient (set xrel))] yrel))))))


(defn- conceptual-right-join
  "find what is in m2 that is not in m1 and union the difference and m1"
  ([rel] rel)
  ([xrel yrel]
   (let [ks (intersection (set (keys (first xrel))) (set (keys (first yrel))))]
     (union
       (join (difference (project yrel ks) (project xrel ks)) yrel)
       (join xrel yrel))))
  ([xrel yrel km]
   (union
     (join (difference (project yrel (set (vals km)))
                       (rename (project xrel (set (keys km))) km)) yrel)
     (join xrel yrel km))))



(comment

  (conceptual-right-join #{{:name "Bob" :movie "Matrix"}
                           {:name "Jim" :movie "Expendibles"}}
                         #{{:name "Bob" :food "pizza"}
                           {:name "Edward" :food "speghetti"}})

  (right-join #{{:name "Bob" :movie "Matrix"}
                {:name "Jim" :movie "Expendibles"}}
              #{{:name "Bob" :food "pizza"}
                {:name "Edward" :food "speghetti"}})

  (outer-join #{{:name "Bob" :movie "Matrix"}
                {:name "Jim" :movie "Expendibles"}}
              #{{:name "Bob" :food "pizza"}
                {:name "Edward" :food "speghetti"}})
  )

(first #{20 2 3 4 5})



