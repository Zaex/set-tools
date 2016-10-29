(ns zaex.set-tools.transient
  (:require [clojure.set :refer :all]))

(def union! (cheat-transient union))
