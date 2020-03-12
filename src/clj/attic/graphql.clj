(ns attic.graphql
  (:require [graphql-builder.parser]))

(defn compile-graphql
  [graphql-files]
  (transduce
    (comp (map slurp)
      (map graphql-builder.parser/parse))
    (partial merge-with into)
    {}
    graphql-files))

(defn post-graphql
  [{:keys [url cookie]} graphql-op]
  (-> {:headers (medley/assoc-some
                  {"Content-Type" "application/json"}
                  "Cookie" cookie)
       :body (cheshire.core/generate-string graphql-op)
       :method :post
       :url url}
    http-client/request
    :body
    cheshire.core/parse-string))
