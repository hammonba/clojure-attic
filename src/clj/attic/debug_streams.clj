(ns attic.debug-streams
  (:require [clojure.tools.logging :as log])
  (:import [java.io InputStream OutputStream]))

(defn dbg-inputstream
  [^InputStream is]
  (proxy [InputStream] []
    (read
      ([] (.read is))
      ([b]
       (let [n (.read is b)]
         (log/infof "read (b) => %d" n)
         n))
      ([b off len]
       (let [n (.read is b off len)]
         (log/infof "read (b off len) => %d" n)
         n)))
    (skip [n] (.skip is n))
    (available [] (.available is))
    (close []
      (log/info "close")
      (.close is))
    (mark [readlimit] (.mark is readlimit))
    (reset [] (.reset is))
    (markSupported [] (.markSupported is))))

(defn dbg-outputstream
  [^OutputStream os]
  (proxy [OutputStream] []
    (close []
      (log/info "OutputStream/close")
      (.close os))
    (flush []
      (log/info "OutputStream/flush")
      (.flush os))
    (write
      ([int b]
       (.write os b))
      ([^bytes b]
       (log/infof "OutputStream/write %d" (count b))
       (.write os b))
      ([b off len]
       (log/infof "OutputStream/write %d %d" off (count b))
       (.write os b off len)))))
