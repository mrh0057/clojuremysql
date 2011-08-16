(ns clojuremysql.record-helpers
  (:use clojure.contrib.sql
        clojuremysql.utils)
  (:require [ clojure.contrib.str-utils2 :as str-utils2]))

(defprotocol DatabaseRecord
  (convert-db [record])
  (insert [record])
  (update [record]))

(defn update-date? [key]
  (or (= key :updated-on)
      (= key :last-modified)))

(defn insert-date? [key]
  (or (update-date? key)
      (= key :created-on)))

(defn insert-vec-builder [db-fields-keys record-fields field-values]
  (loop [db-fields db-fields-keys
         record-fields record-fields
         vals '()]
    (if (empty? db-fields)
      (list (vec db-fields-keys)
            (vec (reverse vals)))
      (let [field (first record-fields)]
        (recur (rest db-fields)
               (rest record-fields)
               (cons (if (insert-date? field)
                       `(new java.util.Date)
                       `(~field ~field-values)) vals))))))

(defn- update-update-function [update-function field vals]
  (if (empty? update-function)
    `(assoc ~vals ~field (new java.util.Date))
    `(assoc ~update-function ~field (new java.util.Date))))

(defn update-record-builder [record-fields vals]
  (let [update-function (reduce (fn [update-function field]
                                  (if (update-date? field)
                                    (update-update-function update-function field vals)
                                    update-function)) '() record-fields)]
    (if (empty? update-function)
      vals
      update-function)))

(defn emmit-extend-record-converter [record db-fields record-fields table id-field]
  (let [convert-db-this (gensym)
        insert-this (gensym)
        update-this (gensym)]
    `(extend ~record
       DatabaseRecord
       {:convert-db (fn [~convert-db-this]
                      (hash-map
                       ~@(loop [db-fields db-fields
                                record-fields record-fields
                                fields '()]
                           (if (empty? db-fields)
                             fields
                             (recur (rest db-fields)
                                    (rest record-fields)
                                    (cons (first db-fields) (cons `(~(first record-fields) ~convert-db-this) fields)))))))
        :insert (fn [~insert-this]
                  ~(let [insert-vec (insert-vec-builder db-fields record-fields insert-this)]
                     `(do
                        (clojure.contrib.sql/insert-values ~table
                                                           ~(first insert-vec)
                                                           ~(second insert-vec))
                        (clojuremysql.utils/last-insert-id))))
        :update (fn [~update-this]
                  (clojure.contrib.sql/update-values ~table
                                                     [~(str id-field " = ?") (:id ~update-this)]
                                                     (convert-db ~(update-record-builder record-fields update-this))))})))

(defn emmit-record-functions [name record db-fields record-fields table id-field]
  (let [convert-db-this (gensym)
        insert-this (gensym)
        update-this (gensym)
        convert-db-func (symbol (str "convert-" name "-db"))]
    `(do
       (defn ~convert-db-func [~convert-db-this]
         (hash-map
          ~@(loop [db-fields db-fields
                   record-fields record-fields
                   fields '()]
              (if (empty? db-fields)
                fields
                (recur (rest db-fields)
                       (rest record-fields)
                       (cons (first db-fields) (cons `(~(first record-fields) ~convert-db-this) fields)))))))
       (defn ~(symbol (str "insert-" name)) [~insert-this]
         ~(let [insert-vec (insert-vec-builder db-fields record-fields insert-this)]
                     `(do
                        (clojure.contrib.sql/insert-values ~table
                                                           ~(first insert-vec)
                                                           ~(second insert-vec))
                        (clojuremysql.utils/last-insert-id))))
       (defn ~(symbol (str "update-" name)) [~update-this]
         (clojure.contrib.sql/update-values ~table
                                            [~(str id-field " = ?") (:id ~update-this)]
                                            (~convert-db-func ~(update-record-builder record-fields update-this)))))))

(defn- keyword-to-lower-case [val]
  (keyword (str-utils2/lower-case (str-utils2/drop (str val) 1))))

(defn- keyword-to-string [val]
  (str-utils2/drop (str val) 1))

(defn def-convert-db [name record db-fields]
  (let [db-record (gensym)]
    `(defn ~name [~db-record]
       (new ~record ~@(map (fn [field] `(~(keyword-to-lower-case field) ~db-record)) db-fields)))))

(defn def-get-db [name table convert-function id-field]
  `(defn ~name [id#]
     (clojure.contrib.sql/with-query-results rs#
       [~(str "SELECT * FROM " (keyword-to-string table) " WHERE " id-field " = ?") id#]
       (~convert-function (first rs#)))))

(defn- option-builder [options]
  (loop [options options
         opt-map {}]
    (if (empty? options)
      opt-map
      (recur (drop 2 options)
             (assoc opt-map (first options) (second options))))))

(defn- keyword-to-symbol [val]
  (symbol (str-utils2/drop (str val) 1)))

(defmacro def-database-record [name record-fields]
  "Used to define a database record. Expects record fields to be keywords.

name - The name of the record
record-fields - The record fields.  Expects them to be keywords."
  `(defrecord ~name
       ~(vec (map (fn [field] (keyword-to-symbol field)) record-fields))))

(defmacro def-database-table [name table-name db-fields record record-fields]
  "Returns a function to setup the database and record converting functions.

name - The postfix name for the functions generated.
table-name - The string name of the table
record - The record that represents the table
db-fields - The database fields
record - The matching record fields.
first keyword in db-fields is assumed to be the id

get-<name> [id]
convert-<name> [db-table]
implements the DatabaseRecord"
  (if (not= (count db-fields) (count record-fields))
    (throw (new Exception "Db fields and record fields must match")))
  (let [convert-function (symbol (str "convert-" name))
        id-field (str-utils2/drop (str (first db-fields)) 1)]
    `(do
       ~(emmit-extend-record-converter record db-fields record-fields table-name id-field)   
       ~(def-convert-db convert-function record db-fields)
       ~(def-get-db (symbol (str "get-" name)) table-name convert-function id-field))))

(defmacro def-database-table-with-func [name table-name db-fields record record-fields]
  "Creates functions for muniplating a database table.

name - The posfix name
table-name - The naem of the table.
db-fields - The fields
record - The record
record-fields - The fields for the reocrd

get-<name> [id]
convert-<name> [db-table]
convert-<name>-db [record]
insert-<name> [record]
update-<name> [record]"
  (if (not= (count db-fields) (count record-fields))
    (throw (new Exception "Db fields and record fields must match")))
  (let [convert-function (symbol (str "convert-" name))
        id-field (str-utils2/drop (str (first db-fields)) 1)]
    `(do
       ~(emmit-record-functions name record db-fields record-fields table-name id-field)   
       ~(def-convert-db convert-function record db-fields)
       ~(def-get-db (symbol (str "get-" name)) table-name convert-function id-field))))
