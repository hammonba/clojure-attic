(ns attic.logging
  (:require [cheshire.core :as json])
  (:import [org.slf4j MDC]))

(defmacro with-mdc [m & body]
  `(let [old# (or (MDC/getCopyOfContextMap) (new java.util.HashMap))
         new# (.clone old#)]
     (doseq [[k# v#] ~m]
       (.put new# (name k#) (str (cond-> v# (coll? v#) json/encode))))
     (MDC/setContextMap new#)
     (try
       ~@body
       (finally (MDC/setContextMap old#)))))
