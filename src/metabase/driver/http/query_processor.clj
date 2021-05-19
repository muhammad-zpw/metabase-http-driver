(ns metabase.driver.http.query-processor
  (:refer-clojure :exclude [==])
  (:require [cheshire.core :as json]
            [clojure.tools.logging :as log]
            [clojure.walk :as walk]
            [clj-http.client :as client])
  (:import [com.jayway.jsonpath JsonPath Predicate]))


(declare compile-expression compile-function)

(defn json-path
  [query body]
  (JsonPath/read body query (into-array Predicate [])))

(defn compile-function
  [[operator & arguments]]
  (case (keyword operator)
    :count count
    :sum   #(reduce + (map (compile-expression (first arguments)) %))
    :float #(Float/parseFloat ((compile-expression (first arguments)) %))
           (throw (Exception. (str "Unknown operator: " operator)))))

(defn compile-expression
  [expr]
  (cond
    (string? expr)  (partial json-path expr)
    (number? expr)  (constantly expr)
    (vector? expr)  (compile-function expr)
    :else           (throw (Exception. (str "Unknown expression: " expr)))))

(defn aggregate
  [rows metrics breakouts]
  (let [breakouts-fns (map compile-expression breakouts)
        breakout-fn   (fn [row] (for [breakout breakouts-fns] (breakout row)))
        metrics-fns   (map compile-expression metrics)]
    (for [[breakout-key breakout-rows] (group-by breakout-fn rows)]
      (concat breakout-key (for [metrics-fn metrics-fns]
                             (metrics-fn breakout-rows))))))

(defn extract-fields
  [rows fields]
  (let [fields-fns (map compile-expression fields)]
    (for [row rows]
      (for [field-fn fields-fns]
        (field-fn row)))))


(defn add-column-metadata
  [fields]
  (for [field fields]
    {:name field :display_name field}))


(defn execute-http-request-reducible [respond native-query]
  (let [query         (if (string? (:query native-query))
                        (json/parse-string (:query native-query) keyword)
                        (:query native-query))
        result        (client/request {:method  (or (:method query) :get)
                                       :url     (:url query)
                                       :headers (:headers query)
                                       :body    (if (:body query) (json/generate-string (:body query)))
                                       :accept  :json
                                       :as      :json})
        rows-path     (or (:path (:result query)) "$")
        rows          (json-path rows-path (walk/stringify-keys (:body result)))
        fields        (or (:fields (:result query)) (keys (first rows)))
        aggregations  (or (:aggregation (:result query)) [])
        breakouts     (or (:breakout (:result query)) [])
        raw           (and (= (count breakouts) 0) (= (count aggregations) 0))
        columns_metadata (if raw (add-column-metadata fields) (add-column-metadata (concat breakouts aggregations)))]
    (log/info "Row results: " rows)
    (log/info "Columns metadata: " columns_metadata)
    (respond
     {:cols columns_metadata}
     (if raw
       (extract-fields rows fields)
       (aggregate rows aggregations breakouts)))))