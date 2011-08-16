(ns clojuremysql.test.record-helpers
  (:use clojure.test
        clojuremysql.record-helpers))

(def-database-record TestRecord [:field-1 :field-2 :field-3])

(def-database-record FuncTable [:field-1 :field-2])

(def-database-table-with-func name-func :func_table [:field_1 :field_2]
  FuncTable
  [:field-1 :field-2])

(deftest table-with-func-test
  (is insert-name-func)
  (is update-name-func)
  (is convert-name-func-db))

(defn setup-functions []
  (def-database-table test-record :test_record [:field_1 :field_2 :field_3]
    TestRecord
    [:field-1 :field-2 :field-3]))


(deftest insert-vec-builder-test
  (let [db-fields [:1 :2 :3]
        record-fields [:1 :created-on :last-modified]
        vecs (insert-vec-builder db-fields record-fields "values")
        vals-vec (second vecs)]
    (is (= db-fields (first vecs)))
    (is (= (first vals-vec) '(:1 "values")))
    (is (= (second vals-vec) '(new java.util.Date)))
    (is (= (nth vals-vec 2) '(new java.util.Date) ))))

(deftest update-record-builder-test
  (let [record-fields [:1 :last-modified :updated-on]
        vals "values"
        result (update-record-builder record-fields vals)]
    (is (= result '(clojure.core/assoc (clojure.core/assoc "values" :last-modified (new java.util.Date)) :updated-on (new java.util.Date))))))

(deftest record-convert-test
  (setup-functions)
  (let [record (convert-db (TestRecord. 1 2 3))
        db-convert (convert-test-record {:field_1 4 :field_2 5 :field_3 6})]
    (println record)
    (is (= (:field_1 record) 1))
    (is (= (:field_2 record) 2))
    (is (= (:field_3 record) 3))

    (is (= (:field-1 db-convert) 4))
    (is (= (:field-2 db-convert) 5))
    (is (= (:field-3 db-convert) 6))))
