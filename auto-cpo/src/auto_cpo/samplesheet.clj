(ns auto-cpo.samplesheet
  (:require [clojure.java.io :as io]
            [clojure.string :as str]))

(defn remove-from-end
  "Remove all instances of `end` from `s`.
  eg:
  ```clojure
  (remove-from-end \"foo,,,\" \",\")
  => \"foo\"  
  ```
  takes:
     `s`: (`String`)
     `end`: (`String`)
   returns:
     The String `s`, with all `end` removed from the end. (`String`)
  "
  [s end]
  (cond (empty? end) s
        (.endsWith s end) (remove-from-end (.substring s 0 (- (count s)
                                                              (count end))) end)
        (empty? s) s
        
        :else s))


(defn parse-project-library-ids
  "Parse the library IDs for a specific project from a SampleSheet.
   takes:
     `lines`: Lines of SampleSheet file. (`[String]`)
     `section-header`: Header of section that we want to parse. `[Data]` for MiSeq, or `[Cloud_Data]` for NextSeq (`String`)
     `project-id`: Project ID used to identify samples for this project in the SampleSheet (`String`)
     `project-id-column`: Column (zero-indexed) of the SampleSheet that project ID is in. 1 for NextSeq, 9 for MiSeq (`int`)
     `library-id-column`: Column (zero-indexed) of the SampleSheet that library ID is in. 0 for NextSeq, 1 for MiSeq (`int`)
   returns:
     Sequence of library IDs belonging to project indicated by `project-id` (`[String]`)
  "
  [lines section-header project-id project-id-column library-id-column]
  (->> lines
       (split-with #(not= section-header (remove-from-end % ",")))
       second
       (drop 2)
       (map #(str/split % #","))
       (filter #(= project-id (nth % project-id-column "")))
       (map #(nth % library-id-column))))
  

(defn parse-project-library-ids-miseq
  "Parse the library IDs for a specific project from a MiSeq SampleSheet.
   takes:
     `lines`: Lines of SampleSheet file (`[String]`)
     `project-id`: Project ID used to identify samples for this project in the SampleSheet (`String`)
   returns:
    Sequence of library IDs belonging to project indicated by `project-id` (`[String]`)
  "
  [lines project-id]
  (parse-project-library-ids lines "[Data]" project-id 9 1))


(defn parse-project-library-ids-nextseq
  "Parse the library IDs for a specific project from a NextSeq SampleSheet.
   takes:
     `lines`: Lines of SampleSheet file (`[String]`)
     `project-id`: Project ID used to identify samples for this project in the SampleSheet (`String`)
   returns:
     Sequence of library IDs belonging to project indicated by `project-id` (`[String]`)
  "
  [lines project-id]
  (parse-project-library-ids lines "[Cloud_Data]" project-id 1 0))


(comment

  
  (def nextseq-samplesheet-path "test_input/SampleSheet_nextseq.csv")
  (def miseq-samplesheet-path "test_input/SampleSheet_miseq.csv")
  
  
  (with-open [rdr (io/reader miseq-samplesheet-path)]
    (-> (doall (line-seq rdr))
        (parse-project-library-ids "[Data]" 9 1)))

  (with-open [rdr (io/reader nextseq-samplesheet-path)]
    (-> (doall (line-seq rdr))
        (parse-project-library-ids "[Cloud_Data]" 1 0)))
  )
