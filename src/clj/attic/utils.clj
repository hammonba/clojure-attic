(ns attic.utils)

(defn preserving-reduced
  "copied from clojure.core/preserving-reduced, but public"
  [rf]
  #(let [ret (rf %1 %2)]
     (if (reduced? ret)
       (reduced ret)
       ret)))

(defn is-thread-interrupted?
  []
  (.isInterrupted ^Thread (Thread/currentThread)))


(defn select-and-rename-keys
  "equivalent to (comp set/rename-keys select-keys)"
  [m kmap]
  (into {}
    (keep (fn [me]
              (when-let [k2 (get kmap (key me))]
                [k2 (val me)])))
    m))

(defn cons-some
  "only cons a non-nil head"
  [x xs]
  (if (some? x)
    (cons x xs)
    xs))

(defn assoc-absent
  "only assoc the values that are missing"
  [m & kvs]
  (reduce (fn [m [k v]]
              (if (contains? m k)
                m
                (assoc m k v)))
    m
    (partition 2 kvs)))

(def encoding-vec (vec "0123456789abcdef"))
(def decoding-map (zipmap encoding-vec (range)))

(defn change-numberbase
  "expresses n into a base described by the encoding-vec
   ONLY WORKS ON POSITIVE NUMBERS"
  ([n] (change-numberbase encoding-vec n))
  ([encoding-vec large-number]
   (let [radix (count encoding-vec)]
     (transduce
       (comp (take-while pos?)
         (map #(rem % radix))
         (map encoding-vec))
       (completing #(.append ^StringBuilder %1 ^char %2)
         #(str (.reverse ^StringBuilder %)))
       (StringBuilder.)
       (iterate #(quot % radix) large-number)))))

(defn revert-numberbase
  "the reversion operation of changenumberbase"
  ([s] (revert-numberbase decoding-map s))
  ([decoding-map s]
   (when s
     (let [radix (count decoding-map)]
       (transduce
         (map decoding-map)
         (completing (fn [acc v]
                         (+' v (*' acc radix))))
         0
         s)))))
