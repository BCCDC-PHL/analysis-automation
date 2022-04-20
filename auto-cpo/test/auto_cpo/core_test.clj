(ns auto-cpo.core-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.clojure-test :refer [defspec]]
            [auto-cpo.test-generators :as tgen]
            [auto-cpo.core :as core]))


(t/deftest illumina-directory-regex-unit
  (t/testing "Illumina directory name matches regex"
    (t/is (core/matches-run-directory-regex? "220207_M00123_0123_000000000-A7TRG")))
  (t/testing "NextSeq-style directory name matches regex"
    (t/is (core/matches-run-directory-regex? "220207_VH00123_123_AAATV7TM5")))
  (t/testing "Non-run directory name does not match regex"
    (t/is (not (core/matches-run-directory-regex? "data")))))


(defspec miseq-directory-regex-property 100
  (prop/for-all [r tgen/miseq-run-id]
                (t/is (core/matches-run-directory-regex? r))))


(defspec nextseq-directory-regex-property 100
  (prop/for-all [r tgen/nextseq-run-id]
                (t/is (core/matches-run-directory-regex? r))))


(t/deftest determine-sequencer-type-unit
  (t/testing "Test that we can correctly determine the sequencer type based on the run ID."
    (let [run-id ""]
      (t/is (= :unknown (core/determine-sequencer-type run-id))))
    (let [run-id "220207_M00123_0123_000000000-A7TRG"]
      (t/is (= :miseq (core/determine-sequencer-type run-id))))
    (let [run-id "220207_VH00123_23_A7TY6AG73"]
      (t/is (= :nextseq (core/determine-sequencer-type run-id))))))


(defspec determine-sequencer-type-property 100
  (prop/for-all [r (gen/one-of [tgen/miseq-run-id tgen/nextseq-run-id])]
                (t/is (or (= :miseq (core/determine-sequencer-type r))
                          (= :nextseq (core/determine-sequencer-type r))))))


(t/deftest symlink-one!-unit
  (t/testing "Symlinking with nil src and dest returns nil."
    (let [src nil
          dest nil]
      (t/is (nil? (core/symlink-one! src dest)))))
  (t/testing "Symlinking with non-nil src and nil dest returns nil."
    (let [src "src"
          dest nil]
      (t/is (nil? (core/symlink-one! src dest)))))
  (t/testing "Symlinking with non-nil dest and nil dest returns nil."
    (let [src nil
          dest "dest"]
      (t/is (nil? (core/symlink-one! src dest))))))
