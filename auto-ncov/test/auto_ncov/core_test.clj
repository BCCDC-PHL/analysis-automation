(ns auto-ncov.core-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.clojure-test :refer [defspec]]
            [auto-ncov.test-generators :as tgen]
            [auto-ncov.core :as core]))


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


(t/deftest currently-analyzing-unit
  (t/testing "Test that we don't determine that a run is analyzing with an empty `db`."
    (let [run-id "220207_M00123_0123_000000000-A7TRG"
          db (atom {})]
      (t/is (not (core/currently-analyzing? run-id db)))))
  (t/testing "Test that we don't determine that a run is analyzing when
              value for `:currently-analyzing` in `db` is `nil`."
    (let [run-id "220207_M00123_0123_000000000-A7TRG"
          db (atom {:currently-analyzing nil})]
      (t/is (not (core/currently-analyzing? run-id db)))))
  (t/testing "Test that we do determine that we are analyzing a run when its run ID is
              the value for `:currently-analyzing` in `db`."
    (let [run-id "220207_M00123_0123_000000000-A7TRG"
          db (atom {:currently-analyzing "220207_M00123_0123_000000000-A7TRG"})]
      (t/is (core/currently-analyzing? run-id db)))))


(defspec currently-analyzing-property 100
  (prop/for-all [r (gen/one-of [tgen/miseq-run-id tgen/nextseq-run-id])]
                (let [db (atom {:currently-analyzing r})]
                  (t/is (core/currently-analyzing? r db)))))


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
