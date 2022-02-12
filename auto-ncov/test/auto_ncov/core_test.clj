(ns auto-ncov.core-test
  (:require [clojure.test :as t]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.clojure-test :refer [defspec]]
            [auto-ncov.test-generators :as tgen]
            [auto-ncov.core :as core]))

(gen/sample tgen/miseq-run-id)


(t/deftest illumina-directory-regex-unit
  (t/testing "MiSeq-style directory name matches regex"
    (t/is (core/matches-run-directory-regex? "220207_M00123_0123_000000000-A7TRG")))
  (t/testing "NextSeq-style directory name matches regex"
    (t/is (core/matches-run-directory-regex? "220207_VH00123_123_AAATV7TM5")))
  (t/testing "Non-run directory name matches regex"
    (t/is (not (core/matches-run-directory-regex? "data")))))

(defspec miseq-directory-regex-property 100
  (prop/for-all [r tgen/miseq-run-id]
                (t/is (core/matches-run-directory-regex? r))))

(defspec nextseq-directory-regex-property 100
  (prop/for-all [r tgen/nextseq-run-id]
    (t/is (core/matches-run-directory-regex? r))))
