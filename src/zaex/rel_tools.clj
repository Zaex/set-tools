(ns zaex.rel-tools
  "Most of the source has been shamelessly taken from clojure.set/join 
  and then enhanced with transients and 1-arty support."
  (:require [clojure.set :refer [intersection
                                 difference
                                 project
                                 rename
                                 rename-keys
                                 map-invert
                                 index
                                 union]]
            #_[net.cgrand.xforms :as x]))


(defn join
  "This behaves the same way as clojure.set/join but
  can be used with transducers hence this has 1-arty support"
  ([rel] rel)
  ([xrel yrel]
   (let [ks (intersection (set (keys (first xrel)))
                          (set (keys (first yrel))))]
     (join xrel yrel (zipmap ks ks))))
  ([xrel yrel km]
   (if (and (set? xrel) (set? yrel))
     (let [[r s k] (if (<= (count xrel) (count yrel))
                     [xrel yrel (map-invert km)]
                     [yrel xrel km]) 
           idx (index r (vals k))]
       (persistent!
        (reduce (fn [ret x]
                  (if-let [found (idx (rename-keys (select-keys x (keys k)) k))]
                    (reduce #(conj! %1 (merge %2 x)) ret found)
                    ret))
                (transient #{})
                s))))))
       


(defn left-join
  "Can be used with transducers as it has 1-arty support"
  ([rel] rel)
  ([xrel yrel]
   (let [ks (intersection (set (keys (first xrel)))
                          (set (keys (first yrel))))]
     (left-join xrel yrel (zipmap ks ks))))
  ([xrel yrel km]
   (if (and (set? xrel) (set? yrel))
     (let [k (map-invert km)
           idx (index yrel (vals km))]
       (persistent!
        (reduce (fn [ret x]
                  (if-let [found (idx (rename-keys (select-keys x (keys k)) k))]
                    (reduce #(conj! %1 (merge %2 x)) ret found)
                    (conj! ret x)))
                (transient #{})
                xrel))))))

(defn left-outer-join
  "Most of the source has been shamelessly taken from clojure.set/join. Added 1-arty and transient support"
  ([rel] rel)
  ([xrel yrel]
   (let [ks (intersection (set (keys (first xrel)))
                          (set (keys (first yrel))))]
     (left-join xrel yrel (zipmap ks ks))))
  ([xrel yrel km]
   (if (and (set? xrel) (set? yrel))
     (let [k (map-invert km)
           idx (index yrel (vals km))]
       (persistent!
        (reduce (fn [ret x]
                  (if-let [found (idx (rename-keys (select-keys x (keys k)) k))]
                    (reduce disj! notfound found)
                    (conj! ret x)))
                (transient #{})
                xrel))))))

(defn right-join
  ([rel] rel)
  ([xrel yrel] 
   (left-join yrel xrel))
  ([xrel yrel km] 
   (left-join yrel xrel (map-invert km))))

(defn right-outer-join
  ([rel] rel)
  ([xrel yrel] 
   (left-outer-join yrel xrel))
  ([xrel yrel km] 
   (left-outer-join yrel xrel (map-invert km))))

(defn outer-join
  "Can be used with transducers as it has 1-arty support" 
  ([rel] rel)
  ([xrel yrel]
   (if (and (seq xrel) (seq yrel))
     (let [ks (intersection (set (keys (first xrel))) (set (keys (first yrel))))
           idx (index xrel ks)]
       (persistent!
        (reduce (fn [notfound x]
                  (if-let [found (idx (select-keys x ks))]
                    (reduce disj! notfound found)
                    (conj! notfound x)))
                (transient (set xrel))
                yrel)))
     #{}))
  ([xrel yrel km] 
   (let [k (map-invert km)
         idx (index xrel (vals k))]
     (persistent!
      (reduce (fn [notfound x]
                (if-let [found (idx (select-keys x k))]
                  (reduce disj! notfound found)
                  (conj! notfound x)))
              (transient (set xrel))
              yrel)))))


