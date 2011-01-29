(ns clojuremysql.utils-test
  (:use clojure.contrib.sql
        clojuremysql.utils
        clojure.test
        midje.sweet))

(defn delete-record [user-role-id]
  (println "deleted record")
  (is (= user-role-id 10)))

(defn insert-record [user-role]
  (println "inserted record " user-role)
  (is (and (= (:key1 user-role) 6) (= (:key2 user-role) 7))))

(deftest many-to-many-update-test
  (let [old-records [{:key1 2 :key2 3} {:id 10 :key1 2 :key2 5} {:id 10 :key1 4 :key2 3}]
        new-records [{:key1 2 :key2 3} {:key1 6 :key2 7}]
        key1 :key1
        key2 :key2]
    (fact
     (many-to-many-update old-records new-records
                          :id key1 key2
                          delete-record insert-record))))

(run-tests)
