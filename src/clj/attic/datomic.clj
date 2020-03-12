(ns attic.datomic
  (:require [attic.reducible :as reducible]
            [attic.transducers :as transducers]
            [datomic.api :as d])
  (:import [clojure.lang IReduceInit]
           [java.util.concurrent TimeUnit LinkedBlockingQueue]
           [java.sql ResultSet Connection]))

(defn basis-t-from-report-data
  "Handles BOTH
  {:db-before :data :db-after} from datomic tx report queue
  {:id :data :t} from datomic tx-range.
  Second case is more convenient for testing in a REPL"
  [{:keys [db-after t]}]
  (or (when db-after
        (d/basis-t db-after))
    t))

(defn xform-txdata->intervals
  "transducer that expects EITHER
  * maps from d/tx-report-queue
  * maps from d/tx-range
  and returns vectors of [start-t end-t]"
  [chunksize nilsize]
  (comp (map basis-t-from-report-data)
    (transducers/partition-some
      (or chunksize 5000) (or nilsize 5))
    (map last)))

(defn impose-partition-strobe
  "transducer that will impose a partition and return the last element of
  the partition.
  This is useful when we want to split a continuous process into larger chunks for batching"
  [{:keys [datomic-chunksize nilsize killswitch]}]
  (comp (attic.transducers/partition-some (or datomic-chunksize 10240)
          (or nilsize 10)
          killswitch)
    (map last)))

(defn is-thread-interrupted?
  []
  (.isInterrupted ^Thread (Thread/currentThread)))

(defn reportqueue-reducible
  "Reducible object that will play the contents of datomic report queue
   into reducing function.
   Will emit nils when no data arrives during timeout interval.
   NB. It is expected that these nil values will be used as heartbeats"
  [datomic-conn & {:keys [timeout-millis]}]
  (let [timeout-millis (or timeout-millis 1000)]
    (reify IReduceInit
      (reduce [_ f init]
        (let [^LinkedBlockingQueue drq (d/tx-report-queue datomic-conn)]
          (try
            (loop [acc init]
              (if (or (reduced? acc)
                    (is-thread-interrupted?))
                acc
                (let [v (.poll drq timeout-millis TimeUnit/MILLISECONDS)]
                  (recur (f acc v)))))
            (finally (d/remove-tx-report-queue datomic-conn))))))))

(defn select-lasttx
  [{:keys [^Connection jdbc-conn tx-tablename]}]
  (with-open [^ResultSet rs
              (->> (format "SELECT max(last_tx) FROM %s" (name tx-tablename))
                (.executeQuery (.createStatement jdbc-conn)))]
    (when (.next rs)
      (.getLong rs 1))))

(defn reportqueue-timevals-reducible
  "squats on the tx-report queue,
   returns a reducible of successive t values"
  [{:keys [datomic-conn datomic-chunksize nilsize] :as args}]
  (let [init-t  (d/tx->t (select-lasttx args))
        start-t (d/basis-t (d/db datomic-conn))]
    (reducible/chaining-reducible
      (list [init-t start-t]
        (eduction
          (xform-txdata->intervals datomic-chunksize nilsize)
          (reportqueue-reducible datomic-conn))))))
