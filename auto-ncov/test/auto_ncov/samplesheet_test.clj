(ns auto-ncov.samplesheet-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.clojure-test :refer [defspec]]
            [auto-ncov.test-generators :as tgen]
            [auto-ncov.samplesheet :as samplesheet]))


#_(t/deftest remove-from-end-unit
  (t/testing ""
    (t/is (= "" (samplesheet/remove-from-end "" ""))))
  (t/testing ""
    (t/is (= "" (samplesheet/remove-from-end "" ","))))
  (t/testing ""
    (t/is (= "foo" (samplesheet/remove-from-end "foo" ""))))
  (t/testing ""
    (t/is (= "foo" (samplesheet/remove-from-end "foo,,," ","))))
  )
