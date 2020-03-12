(ns attic.csv-utils
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as jio]
            [clojure.tools.logging :as log])
  (:import [java.io LineNumberReader BufferedReader]))

(defn flatten-map
  "flatten a map into a simple vector, suitable for apply"
  [m]
  (if (map? m)
    (reduce into [] m)
    (flatten m)))

(defn read-csvline
  "read single line from string,
  interpret as single csv row"
  ([s flat-opts]
   (first (apply csv/read-csv s flat-opts)))
  ([s int-sep int-quot]
   (first (csv/read-csv-from s int-sep int-quot))))

(defn read-csvline-from-reader
  "read single line from buffered reader,
  interpret as single csv row"
  [^BufferedReader br flat-opts]
  (read-csvline (.readLine br) flat-opts))

(defn csv-maps
  "generic low overhead way to turn (massive) csv file
   into lazily evaluated stream of maps"
  ([ff] (csv-maps ff nil))
  ([ff {:keys [header-fn pre-rowfn post-rowfn]
        :or {header-fn identity
             pre-rowfn identity
             post-rowfn identity}
        :as opts}]
   (let [flat-opts (flatten-map opts)]
     (reify clojure.lang.IReduceInit
       (reduce [_ f init]
         (with-open [^BufferedReader r (apply jio/reader ff flat-opts)]
           (let [hdr (header-fn (read-csvline-from-reader r flat-opts))]
             (transduce
               (comp
                 (keep (juxt identity pre-rowfn))
                 (map (fn [[row prerow]] (-> (zipmap hdr prerow)
                                           (assoc :row row))))
                 (keep post-rowfn))
               (completing f)
               init
               (apply csv/read-csv r flat-opts)))))))))

(defn csv-vals
  "generic low overhead way to turn (massive) csv file
   into lazily evaluated stream of csv vals, line numbers and raw inputs"
  ([ff] (csv-maps ff nil))

  ([ff {:keys [header-fn separator quote]
        :or {header-fn identity separator \, quote \"}
        :as opts}]
   (let [flat-opts (flatten-map opts)
         int-sep (int separator)
         int-quot (int quote)]
     (reify clojure.lang.IReduceInit
       (reduce [_ f init]
         (try
           (with-open [^LineNumberReader lnr (LineNumberReader. (apply jio/reader ff flat-opts))]
             (let [hdr (header-fn (read-csvline (.readLine lnr) int-sep int-quot))]
               (loop [acc init]
                 (if (reduced? acc)
                   (unreduced acc)
                   (if-let [row (.readLine lnr)]
                     (recur (f acc {:line-number (.getLineNumber lnr)
                                    :raw-row row
                                    :hdr hdr
                                    :vals (read-csvline row int-sep int-quot)}))
                     acc)))))
           (catch Throwable t
             (log/warn t "caught in csv vals")
             (throw t))))))))
