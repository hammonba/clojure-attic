(ns attic.trampoline
  (:require [attic.logging]
            [attic.utils :as utils]
            [clojure.tools.logging :as log])
  (:import [java.util.concurrent ScheduledExecutorService TimeUnit Executors]
           [java.util.concurrent.atomic AtomicBoolean]
           [com.google.common.util.concurrent ThreadFactoryBuilder]))


(defn fibish-seq
  "generate a Fibonacci-style sequence between two values"
  [min-val max-val]
  {:pre [(< min-val max-val)]}
  (->> [(rand-int min-val) (rand-int min-val)]
    (iterate (fn [[f0 f1]] [f1 (+ f0 f1)]))
    (map last)
    (drop-while #(< % min-val))
    (take-while #(< % max-val))))

(def TRAMPOLINE-MIN-MILLIS 1000)

(defn next-trampoline-args
  "implements backoff; if were are running earlier than the running hot timestamp
   then we are still running hot and need to continue backing off.
   Otherwise we can reset the backoff time."
  [{:keys [hot-timestamp backoff-seq reset-backoffseq] :as args}]
  (let [running-hot? (if (nil? hot-timestamp)
                       false
                       (< (System/currentTimeMillis) hot-timestamp))
        backoff-seq (if running-hot? backoff-seq reset-backoffseq)
        delay-millis (first backoff-seq)
        next-backoff-seq (or (next backoff-seq) backoff-seq)
        args-out (assoc args
                   :delay-millis delay-millis
                   :backoff-seq next-backoff-seq
                   :hot-timestamp (+ (System/currentTimeMillis) delay-millis TRAMPOLINE-MIN-MILLIS)
                   :current-timestamp (System/currentTimeMillis)
                   :running-hot? running-hot?)]
    (when running-hot?
      (attic.logging/with-mdc args-out (log/warn ::next-trampoline-args)))

    args-out))

(def is-thread-interrupted? utils/is-thread-interrupted?)

(defn loopfn-trampoline
  "provides Runnable task that
   * submits the loopfn for execution
   * reschedules this trampoline function to run a future moment.
   We expect the ExecutorService to be single threaded and therefore this
   trampoline only gets Run AFTER the loopfn has exited; this is important
   because it is how we figure out when we are running hot and need to backoff.

   We shutdown the executor when we leave (because we assume it is single-threaded)

   Eg; we might run hot if a jdbc database is unavailable, causing the loop function
   to exit prematurely. We want to keep trying, but we don't want to make things
   unnecessarily worse by trying too often."
  [{:keys [^ScheduledExecutorService exec
           ^AtomicBoolean killswitch
           loopfn] :as args}]
  (fn []
      (try
        (if-not (or (.get killswitch) (is-thread-interrupted?))
          (do (.submit exec ^Runnable loopfn)

              (let [{:keys [delay-millis] :as args} (next-trampoline-args args)]
                (.schedule exec
                  ^Runnable (loopfn-trampoline args)
                  ^long delay-millis
                  TimeUnit/MILLISECONDS)))

          (do (attic.logging/with-mdc {:action :shutdown-executor
                                       :killswitch (.get killswitch)
                                       :interrupted (is-thread-interrupted?)}
                (log/warn ::loopfn-trampoline))
              (.shutdown exec)))
        (catch Throwable t (log/error t ::loopfn-trampoline)
                           (throw t))
        (finally
          (attic.logging/with-mdc {:action :exiting} (log/warn ::loopfn-trampoline))))))

(defn decorate-loopfn
  "we want a thread interruption to trip the kill switch; since we do not
  control the actual Thread instance"
  [^AtomicBoolean killswitch loopfn]
  (fn []
      (try
        (loopfn)
        (catch InterruptedException ex
          (log/info ex ::decorate-loopfn)
          (.set killswitch true))
        (catch Throwable ex
          (log/error ex ::decorate-loopfn)
          (throw ex)))))

(defn run-in-singlethread-executor
  "create a singlethread executor that will keep running the thunk f
   until thread is interrupted or killswitch is set"
  ([thread-name killswitch maxbackoff-millis f]
   (let [exec (Executors/newSingleThreadScheduledExecutor
                (.build (.setNameFormat (ThreadFactoryBuilder.) thread-name)))]

     (.submit exec (loopfn-trampoline {:reset-backoffseq (cons 10 (fibish-seq TRAMPOLINE-MIN-MILLIS maxbackoff-millis))
                                       :exec exec
                                       :killswitch killswitch
                                       :loopfn (decorate-loopfn killswitch f)}))
     exec)))
