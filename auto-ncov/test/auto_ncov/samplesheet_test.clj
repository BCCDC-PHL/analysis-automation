(ns auto-ncov.samplesheet-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.clojure-test :refer [defspec]]
            [auto-ncov.test-generators :as tgen]
            [auto-ncov.samplesheet :as samplesheet]))


(t/deftest remove-from-end-unit
  (t/testing "Remove empty string from empty string returns empty string."
    (t/is (= "" (samplesheet/remove-from-end "" ""))))
  (t/testing "Remove non-empty string from empty string returns empty string."
    (t/is (= "" (samplesheet/remove-from-end "" ","))))
  (t/testing "Remove empty string from non-empty string returns the original non-empty string."
    (t/is (= "foo" (samplesheet/remove-from-end "foo" ""))))
  (t/testing "Remove non-empty string from non-empty string."
    (t/is (= "foo" (samplesheet/remove-from-end "foo,,," ",")))))


(t/deftest parse-project-library-ids-unit
  (t/testing ""
    (let [lines ["[Data]"
                 "sample-01,"
                 "sample-02,project"]
          section-header "[Data]"
          project-id "project"
          project-id-column 1
          library-id-column 0]
      (t/is (= ["sample-02"] (samplesheet/parse-project-library-ids lines section-header project-id project-id-column library-id-column))))))
