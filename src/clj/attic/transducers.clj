(ns attic.transducers
  (:require [clojure.tools.logging :as log])
  (:import [java.util.concurrent.atomic AtomicBoolean]
           [java.util ArrayList Iterator]
           [clojure.lang IReduceInit]))

(defn bookend
  "prepend and append values to transducer outputs"
  [pre-val post-val]
  (fn [rf]
      (let [started (volatile! false)]
        (fn
         ([] (rf))
         ([acc]
          (cond-> acc

            (and (not @started) (not= ::skip pre-val))
            (rf pre-val)

            (not= ::skip post-val)
            (rf post-val)

            :always
            (rf)))
         ([acc v]
          (if-not @started
            (do (vreset! started true)
                (cond-> acc
                  (not= ::skip pre-val) (rf pre-val)
                  :always (rf v)))
            (rf acc v)))))))

(defn partition-by-atleast
  "transducer that combines partition-all with partition-by
   It will firstly count to n, then it will cut where value of fn changes"
  [^long n f]
  (fn [rf]
      (let [decn (dec n)
            a (java.util.ArrayList. (+ n 16))
            pv (volatile! ::none)]
        (fn
         ([] (rf))
         ([result]
          (let [result (if (.isEmpty a)
                         result
                         (let [v (vec (.toArray a))]
                           ;;clear first!
                           (.clear a)
                           (unreduced (rf result v))))]
            (rf result)))
         ([result input]
          (let [pval @pv
                retain? (or (identical? pval ::none)
                          (= (f input) pval))]
            (when (> (.size a) decn)
              (vreset! pv (f input)))
            (if retain?
              (do (.add a input)
                  result)
              (let [v (vec (.toArray a))]
                (.clear a)
                (vreset! pv ::none)
                (let [ret (rf result v)]
                  (when-not (reduced? ret)
                    (.add a input))
                  ret)))))))))

(defn partition-some
  "similiar to (partition-all; but can also create a new partition
  when it sees enough contiguous nils. Will not create a new partition when
  it only sees nils"
  ([max-vals max-nils] (partition-some max-vals max-nils nil))
  ([max-vals max-nils ^AtomicBoolean killswitch]
   (fn [rf]
       (let [a (ArrayList. ^int max-vals)
             nil-counter (volatile! 0)]
         (fn
          ([] (rf))
          ([result]
           (let [result (if (.isEmpty a)
                          result
                          (let [v (vec (.toArray a))]
                            ;;clear first!
                            (.clear a)
                            (unreduced (rf result v))))]
             (rf result)))
          ([result input]
           (if (and (some? killswitch) (.get killswitch))
             (reduced result)
             (do (if (some? input)
                   (do (.add a input)
                       (vreset! nil-counter 0))
                   (vswap! nil-counter #(min max-nils (inc %))))
                 (if (or (= max-vals (.size a))
                       (and (= max-nils @nil-counter)
                         (not (.isEmpty a))))
                   (let [v (vec (.toArray a))]
                     (.clear a)
                     (rf result v))
                   result)))))))))

(defn iterator-reducible
  "expresses an iterator through the medium of IReduceInit
   if first-val is nil it will be ignored"
  [first-val ^java.util.Iterator it]
  (reify IReduceInit
    (reduce [_ f start]
      (loop [acc (if first-val
                   (f start first-val)
                   start)]
        (if (or (reduced? acc)
              (not (.hasNext it)))
          (unreduced acc)
          (recur (f acc (.next it))))))))


(defn exclude-from-orderedcoll-xform
  "takes two collections OF STRICTLY ASCENDING VALUES according to cmp
  eg Iterables of datomic entity Ids
  and will permit the transducer values that are not present in collR
  cmp will always be called with collR items as first arg, collVal as second arg
  only collVal items are returned.
  The two colls can have different kinds of data as long as cmp can work on them"
  [cmp ^Iterable collR]
  (fn [rf]
      (let [iterR (.iterator collR)
            nextR (volatile! nil)]
        (fn
         ([] (rf))
         ([acc] (rf acc))
         ([acc trans-val]
          (transduce
            (fn [rf]
                (fn
                 ([] (rf))
                 ([acc]
                  ;; if nextR is set, then we are at the end of a sequence of matches
                  ;; and test-val was also a match. Therefore we don't want it.
                  ;; if nextR is NOT set then we have run out of collR values
                  ;; and we want EVERYTHING from NOW ON
                  (if (some? @nextR)
                    acc
                    (rf acc trans-val)))
                 ([acc remove-val]
                  (let [c (cmp remove-val trans-val)]
                    (cond
                      ;; test-val is ahead of remove-val; we have not yet caught up
                      (neg? c) acc

                      ;; test-val equals remove-val; we have caught up
                      ;; we do not want this test-val
                      (zero? c) (do (vreset! nextR remove-val)
                                    (reduced acc))

                      ;; remove-val has raced ahead of test-val.
                      ;; we want this test-val
                      ;; We want to keep this remove-val whilst test-val catches up
                      (pos? c) (do (vreset! nextR remove-val)
                                   (reduced (rf acc trans-val))))))))
            rf
            acc
            (let [r @nextR]
              (vreset! nextR nil)
              (iterator-reducible r iterR))))))))


(defn apply-throttle
  "sleeps thread until desired timestamp, if it is in the future"
  [desired-ts]
  (let [t (- desired-ts (System/currentTimeMillis))]
    (when (pos? t)
      (Thread/sleep t))))

(defn build-throttle-transducer
  "transducer that will attempt to throttle processing to align
  with the period millis"
  [period-millis]
  (fn [rf]
      (let [next-ts (volatile! (System/currentTimeMillis))]
        (fn throttle-transducer*
            ([] (rf))
            ([result] (rf result))
            ([result input]
             (apply-throttle @next-ts)
             (vswap! next-ts #(+ % period-millis))
             (rf result input))))))

(defn build-interval-logging-transducer
  "writes a log statement every now and then"
  [n]
  (fn [rf]
      (let [v (volatile! 0)]
        (fn
         ([] (rf))
         ([result] (rf result))
         ([result input]
          (when (zero? (vswap! v #(mod (inc %) n)))
            (log/warn input))
          (rf result input))))))

(defn create-interleaving-reducible
  "reducible that will interleave the collection values
   WORKS WITH RAGGED DATA; this is its edge over clojure.core/interleave"
  [colls]
  (reify IReduceInit
    (reduce [this f start]
      (let [iters (map #(.iterator ^Iterable %) colls)]
        (loop [acc start]
          (if (not-any? #(.hasNext ^Iterator %) iters)
            acc
            (recur (reduce
                     (fn [acc ^Iterator it]
                         (if (.hasNext it)
                           (f acc (.next it))
                           acc))
                     acc
                     iters))))))))


(defn masking-xform
  "superimposes nil when the predicate call fails"
  [pred]
  (map #(when (pred %) %)))
