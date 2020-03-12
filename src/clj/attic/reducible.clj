(ns attic.reducible
  (:require [attic.utils :as utils]
            [clojure.java.io :as jio])
  (:import [clojure.lang IReduceInit]
           [java.io PushbackReader]
           [java.util Iterator]))

(defn chaining-reducible
  "takes a coll of colls. Returns reducible that chains call to reduce over each coll"
  [coll-of-colls]
  (reify IReduceInit
    (reduce [_ f init]
      (let [prf (utils/preserving-reduced f)]
        (reduce (partial reduce prf)
          init
          coll-of-colls)))))

(defn reducible-edn
  "useful for testing from a precanned collection of edns"
  [edn-file]
  (reify IReduceInit
    (reduce [_ f init]
      (with-open [r (PushbackReader. (jio/reader edn-file))]
        (loop [acc init]
          (if (reduced? acc)
            (unreduced acc)
            (let [l (clojure.edn/read {:eof :eof} r)]
              (if (not= l :eof)
                (recur (f acc l))
                acc))))))))

(defn has-next?
  [^Iterator it]
  (.hasNext it))

(defn it-next
  [^Iterator it]
  (when (.hasNext it)
    (.next it)))

(defn weakly-shuffle
  "takes a coll of colls. Preserves the relative ordering of inner coll elements
  whilst shuffling the outer coll elts"
  [coll-of-colls]
  (reify clojure.lang.IReduceInit
    (reduce [_ f init]
      (loop [acc init
             coi (filter has-next?
                   (map #(.iterator ^Iterable %) coll-of-colls))]
        (cond (reduced? acc) (unreduced acc)
              (empty? coi) acc
              :else (recur
                      (f acc (it-next (rand-nth coi)))
                      (filter has-next? coi)))))))
