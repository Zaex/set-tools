(ns rel-tools.core
  (:require [clojure.set :refer [intersection difference project rename rename-keys map-invert index union]]))

(defn join
  "This behaves the same way as clojure.set/join but can now act
  as a reducer function in a transducer process"
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
                               [ret (reduce disj! notfound found)]
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
                             [ret (reduce disj! notfound found)]
                             [(conj! ret x) notfound]))
                         [(transient #{}) (transient (set xrel))] yrel))))))


(first #{20 2 3 4 5})
