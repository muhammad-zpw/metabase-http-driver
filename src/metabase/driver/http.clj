(ns metabase.driver.http
  "HTTP API driver."
  (:require [cheshire.core :as json]
            [clojure.tools.logging :as log]
            [metabase.driver :as driver]
            [metabase.driver.http.query-processor :as http.qp]
            [metabase.query-processor.store :as qp.store]
            [metabase.util :as u]))

(defn find-first
  [f coll]
  (first (filter f coll)))

(defn- database->definitions
  [database]
  (json/parse-string (:definitions (:details database)) keyword))

(defn- database->table-defs
  [database]
  (or (:tables (database->definitions database)) []))

(defn- database->table-def
  [database name]
  (log/info "database->table-def database:" database ", name: " name)
  (first (filter #(= (:name %) name) (database->table-defs database))))

(defn table-def->field
  [table-def name]
  (log/info "table-def->field table-def: " table-def "name" name)
  (find-first #(= (:name %) name) (:fields table-def)))

(defn mbql-field->expression
  [table-def expr]
  (log/info "mbql-field->expression table-def: " table-def "expr" expr)
  (let [field-store (qp.store/field (get expr 1))]
    (:name field-store)))

(defn mbql-aggregation->aggregation
  [table-def mbql-aggregation]
  (log/info "mbql-aggregation->aggregation" mbql-aggregation)
  [(:name (get mbql-aggregation 2))])

(def json-type->base-type
  {:string  :type/Text
   :number  :type/Float
   :boolean :type/Boolean})

(driver/register! :http)

(defmethod driver/supports? [:http :basic-aggregations] [_ _] true)

(defmethod driver/can-connect? :http [_ _]
  true)

(defmethod driver/describe-database :http [_ database]
  (let [table-defs (database->table-defs database)]
    (log/info "Describe database...")
    {:tables (set (for [table-def table-defs]
                    {:name   (:name table-def)
                     :schema (:schema table-def)}))}))

(defmethod driver/describe-table :http [_ database table]
  (let [table-def  (database->table-def database (:name table))]
    (log/info "Describe table...")
    {:name   (:name table-def)
     :schema (:schema table-def)
     :fields (set (for [field (:fields table-def)]
                    {:name          (:name field)
                     :database-type (:type field)
                     :database-position (:id database)
                     :base-type     (or (:base_type field)
                                        (json-type->base-type (keyword (:type field))))}))}))

(defmethod driver/mbql->native :http [_ query]
  (let [database    (qp.store/database)
        table       (qp.store/table (:source-table (:query query)))
        table-def   (database->table-def database (:name table))
        breakout    (map (partial mbql-field->expression table-def) (:breakout (:query query)))
        aggregation (map (partial mbql-aggregation->aggregation table-def) (:aggregation (:query query)))]
    (log/info "driver/mbql->native Query:" query)
    (log/info "table" table)
    (log/info "breakout" breakout)
    (log/info "aggregation" aggregation)
    {:query (merge (select-keys table-def [:method :url :headers])
                   {:result (merge (:result table-def)
                                   {:breakout     breakout
                                    :aggregation  aggregation})})
     :mbql? true}))

;; (defmethod driver/execute-query :http [_ {native-query :native}]
;;   (http.qp/execute-http-request native-query)) 

(defmethod driver/execute-reducible-query :http
  [_ {query :native} _ respond]
  (log/info "Query: " query)
  (http.qp/execute-http-request-reducible respond query))