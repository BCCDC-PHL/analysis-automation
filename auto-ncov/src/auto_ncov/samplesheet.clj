(ns auto-ncov.samplesheet
  (:require [clojure.java.io :as io]
            [clojure.string :as str]))

(defn remove-from-end
  ""
  [s end]
  (if (.endsWith s end)
    (remove-from-end (.substring s 0 (- (count s)
                                        (count end))) end)
    s))

(defn parse-project-library-ids
  ""
  [lines section-header project-id project-id-column sample-id-column]
  (->> lines
       (split-with #(not= section-header (remove-from-end % ",")))
       second
       (drop 2)
       (map #(str/split % #","))
       (filter #(= project-id (nth % project-id-column)))
       (map #(nth % sample-id-column))))
  

(defn parse-project-library-ids-miseq
  ""
  [lines project-id]
  (parse-project-library-ids lines "[Data]" project-id 9 1))


(defn parse-project-library-ids-nextseq
  ""
  [lines project-id]
  (parse-project-library-ids lines "[Cloud_Data]" project-id 1 0))


(comment

  
  (def nextseq-samplesheet-path "test_input/SampleSheet_nextseq.csv")
  (def miseq-samplesheet-path "test_input/SampleSheet_miseq.csv")
  
  
  (with-open [rdr (io/reader miseq-samplesheet-path)]
    (-> (doall (line-seq rdr))
        (parse-covid-library-ids "[Data]" 9 1)))

  (with-open [rdr (io/reader nextseq-samplesheet-path)]
    (-> (doall (line-seq rdr))
        (parse-samplesheet "[Cloud_Data]" 1 0)))
  )
