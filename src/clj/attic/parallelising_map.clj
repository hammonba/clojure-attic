(ns attic.parallelising-map
  (:import [clojure.lang PersistentQueue]))

(defn build-lagging-transducer
  "creates a transducer that will always run n items behind.
   this is convenient if the pipeline contains futures, which you
   want to start deref-ing only when a certain number are in flight"
  [n]
  (fn [rf]
      (let [qv (volatile! PersistentQueue/EMPTY)]
        (fn
         ([] (rf))
         ([acc] (rf (reduce rf acc @qv)))
         ([acc v]
          (vswap! qv conj v)
          (if (< (count @qv) n)
            acc
            (let [h (peek @qv)]
              (vswap! qv pop)
              (rf acc h))))))))

(defn parallelising-map
  ([f]
   (let [n (+ 2 (.. Runtime getRuntime availableProcessors))]
     (parallelising-map n f)))
  ([n f]
   (comp (map #(fn [] (f %)))
     (map future-call)
     (build-lagging-transducer n)
     (map deref))))
