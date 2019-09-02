(ns rel-tools.test
  (:require [rel-tools.core :refer :all]
            [clojure.set :refer [intersection
                                 difference
                                 project
                                 rename
                                 rename-keys
                                 map-invert
                                 index
                                 union]]))


(defn- conceptual-right-join
  "find what is in m2 that is not in m1 and union the difference and m1"
  ([rel] rel)
  ([xrel yrel]
   (let [ks (intersection (set (keys (first xrel))) (set (keys (first yrel))))]
     (union
       (clojure.set/join (difference (project yrel ks) (project xrel ks)) yrel)
       (clojure.set/join xrel yrel))))
  ([xrel yrel km]
   (union
     (clojure.set/join (difference (project yrel (set (vals km))))
                       (rename (project xrel (set (keys km))) km)) yrel
     (clojure.set/join xrel yrel km))))



(comment
  (is
   (= (right-join #{{:name "Bob" :movie "Matrix"}
                    {:name "Jim" :movie "Expendibles"}}
                  #{{:name "Bob" :food "pizza"}
                    {:name "Edward" :food "speghetti"}})
      ))
  

  (outer-join #{{:name "Bob" :movie "Matrix"}
                {:name "Jim" :movie "Expendibles"}}
              #{{:name "Bob" :food "pizza"}
                {:name "Edward" :food "speghetti"}}))
