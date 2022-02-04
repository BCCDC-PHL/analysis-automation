(ns auto-qc.core
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [clojure.pprint :refer [pprint]]
            [clojure.tools.cli :refer [parse-opts]]
            [clojure.string :as str]
            [clojure.java.shell :refer [sh]]
            [clojure.core.async :as async :refer [go go-loop chan onto-chan! <! <!! >! >!! timeout]]
            [auto-qc.cli :as cli])
  (:gen-class))


(defn upload-complete?
  "Check for presence of upload_complete.json file. Returns true if present, false otherwise"
  [run-dir]
  (let [upload-complete-path (str/join \/ [run-dir "upload_complete.json"])]
    (.exists (io/file upload-complete-path))))


(defn analyzed?
  "Check for presence of RoutineQC sub-directory. Returns true if present, false otherwise"
  [run-dir]
  (let [analysis-dir-path (str/join \/ [run-dir "RoutineQC"])]
    (.exists (io/file analysis-dir-path))))


(defn run-nextflow!
  "Run the BCCDC-PHL/routine-sequence-qc pipeline on a run directory.
   When the analysis completes, delete the 'work' directory."
  [{:keys [run-dir revision]}]
  (let [work-dir (str/join \/ [run-dir (str "work-" (java.util.UUID/randomUUID))])
        outdir (str/join \/ [run-dir "RoutineQC"])
        log-file (str/join \/ [outdir "nextflow.log"])]
    (do
      (sh "mkdir" "-p" outdir)
      (apply sh ["nextflow"
                 "-q"
                 "-log" log-file
                 "run" "BCCDC-PHL/routine-sequence-qc"
                 "-profile" "conda"
                 "--cache" (str/join \/ [(System/getProperty "user.home") ".conda/envs"])
                 "-r" revision
                 "--run_dir" run-dir
                 "-work-dir" work-dir
                 "--outdir" outdir])
      (sh "rm" "-r" work-dir))))


(defn -main
  "Main entry point"
  [& args]

  ;;
  ;; Command-line argument parsing
  (def opts (parse-opts args cli/options))

  ;;
  ;; Handle -h and --help flags
  (if (get-in opts [:options :help])
    (let [options-summary (:summary opts)]
      (cli/exit 0 (cli/usage options-summary))))

  ;;
  ;; Handle -v and --version flags
  (if (get-in opts [:options :version])
    (cli/exit 0 cli/version))

  ;;
  ;; In-memory db for co-ordinated state
  (def db (atom {}))

  ;;
  ;; Load list of excluded runs from the --exclude file
  ;; Store set of run IDs under :excluded-run-ids in db
  ;; Refresh every 10 seconds
  (let [exclude-file-path (get-in opts [:options :exclude])]
    (if exclude-file-path
      (go-loop []
        (if (.exists (io/file exclude-file-path))
          (with-open [rdr (io/reader exclude-file-path)]
            (swap! db (fn [x] (assoc x :excluded-run-ids (into #{} (line-seq rdr)))))))
        (log/debug (str "Excluded runs: " (:excluded-run-ids @db)))
        (<! (timeout 10000))
        (recur))))

  
  (def runs-to-analyze-chan (chan))

  ;;
  ;; Scan through sub-directories of --runs-dir
  ;; Exclude those without 'upload_complete.json' file
  ;; Exclude those in the --exclude file
  ;; Add the remaining directories to the runs-to-analyze channel
  ;; Park for 10 seconds
  ;; Recur
  (go-loop []
    (->> (get-in opts [:options :runs-dir])
         io/file
         .listFiles
         (map (memfn getCanonicalPath))
         (filter upload-complete?)
         (filter #(not (analyzed? %)))
         (filter #(not (contains? (:excluded-run-ids @db) (.getName (io/file %)))))
         (#(doseq [run %] (go (>! runs-to-analyze-chan run)))))
    (<! (timeout 10000))
    (recur))


  ;;
  ;; Main loop
  ;; Take directory path from runs-to-analyze channel
  ;; Run nextflow pipeline
  ;; Recur with the next directory path on the channel
  (loop [run (<!! runs-to-analyze-chan)]
    (if (not (nil? run))
      (do
        (log/info (str "Analysis started: " run))
        (run-nextflow! {:run-dir run
                        :revision "main"})
        (log/info (str "Analysis complete: " run))))
    (recur (<!! runs-to-analyze-chan))))




(comment
  ;;
  ;; Useful forms for REPL-driven development

  (def opts {:options {:runs-dir ""
                       :exclude ""}})
  
  (defn mock-analyze!
    ""
    [{:keys [run-dir]}]
    (do
      (log/info (str "Analyzing: " run-dir))
      (Thread/sleep 10000)
      (log/info (str "Analysis complete: " run-dir))))
  )
