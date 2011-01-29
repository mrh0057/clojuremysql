(ns clojuremysql.utils
  (:use clojure.contrib.sql))

(defn last-insert-id []
  (with-query-results rs [(str "SELECT LAST_INSERT_ID() as id")] (:id (first rs))))

(defn- exists-already? [records record key1 key2]
  "Checks to see if the records already exists in the function.

records - The reocds the check in the database.
record - The record in the database.
key1 - The key in the database.
key2 - The key second key in the database."
  (loop [records records]
    (if (empty? records)
      false
      (if (and (= (key1 record) (key1 (first records)))
               (= (key2 record) (key2 (first records))))
        true
        (recur (rest records))))))

(defn- delete-old-records [old-records new-records id-field key1 key2 delete-fn]
  "Used to delete the records from the database.

old-records - The old records in the database.
new-roecrds - The new records in the database.
key1 - The key in the field.
key2 - The second key in the field.
delete-fn - The delete function."
  (loop [old-records old-records]
    (if (empty? old-records)
      nil
      (do
        (if (not (exists-already? new-records (first old-records) key1 key2))
          (delete-fn (id-field (first old-records))))
        (recur (rest old-records))))))

(defn- insert-new-records [old-records new-records key1 key2 insert-fn]
  "Used to insert new records into the database.
old-records - the records that are in the database
new-records - the new records to insert into the database.
key1 - The first key in the many to many relationship.
key2 - The first key in the many to many relationship."
  (loop [new-records new-records]
    (if (empty? new-records)
      nil
      (do
        (if (not (exists-already? old-records (first new-records) key1 key2))
          (insert-fn (first new-records)))
        (recur (rest new-records))))))

(defn many-to-many-update [old-records new-records
                           id-field key1 key2
                           delete-fn insert-fn]
  "Used to update a many-to-many relation.
old-records - The old records in the database.
new-records - The new records in the database.
id-field - The id of the filed to insert.
key1 - The first key to check.
key2 - The second key to check.
delete-fn - The function to delete
insert-fn - The function to insert a record."
  (delete-old-records old-records new-records id-field key1 key2 delete-fn)
  (insert-new-records old-records new-records key1 key2 insert-fn))
