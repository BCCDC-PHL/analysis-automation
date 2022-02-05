(ns auto-qc.core
  (:require [clojure.java.io :as io]
            [clojure.edn :as edn]
            [clojure.set :as set]
            [clojure.tools.logging :as log]
            [clojure.pprint :refer [pprint]]
            [clojure.tools.cli :refer [parse-opts]]
            [clojure.string :as str]
            [clojure.java.shell :as shell :refer [sh]]
            [clojure.core.async :as async :refer [go go-loop chan onto-chan! <! <!! >! >!! timeout]]
            [auto-qc.cli :as cli])
  (:gen-class))


(defn load-edn
  "Load edn from an io/reader source (filename or io/resource).
   https://clojuredocs.org/clojure.edn/read#example-5a68f384e4b09621d9f53a79"
  [source]
  (try
    (with-open [r (io/reader source)]
      (edn/read (java.io.PushbackReader. r)))

    (catch java.io.IOException e
      (printf "Couldn't open '%s': %s\n" source (.getMessage e)))
    (catch RuntimeException e
      (printf "Error parsing edn file '%s': %s\n" source (.getMessage e)))))


(defn matches-run-directory-regex?
  "Check that the directory name matches one of the standard illumina directory name formats."
  [run-dir]
  (->> run-dir
       io/file
       .getName
       (re-matches #"\d{6}_[A-Z0-9]+_\d+_[A-Z0-9\-]+")
       some?))


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
      (sh "chmod" "750" outdir)
      (sh "mkdir" "-p" work-dir)
      (sh "chmod" "750" work-dir)
      (shell/with-sh-dir outdir
        (apply sh ["nextflow"
                   "-q"
                   "-log" log-file
                   "run" "BCCDC-PHL/routine-sequence-qc"
                   "-profile" "conda"
                   "--cache" (str/join \/ [(System/getProperty "user.home") ".conda/envs"])
                   "-r" revision
                   "--run_dir" run-dir
                   "-work-dir" work-dir
                   "--outdir" "."]))
      (sh "rm" "-r" work-dir)
      (sh "find" outdir "-type" "d" "-exec" "chmod" "750" "{}" "+")
      (sh "find" outdir "-type" "f" "-exec" "chmod" "640" "{}" "+"))))


(defn -main
  "Main entry point"
  [& args]

  ;;
  ;; Command-line argument parsing
  (def opts (parse-opts args cli/options))
  (when (not (empty? (:errors opts)))
    (cli/exit 1 (str/join \newline (:errors opts))))

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


  (defn update-config!
    "Read config from file and insert into db under key :config"
    [config-file-path db]
    (if (.exists (io/file config-file-path))
      (let [config (load-edn config-file-path)]
        (swap! db (fn [x] (assoc x :config config))))))
  
  ;;
  ;; Load config to db
  ;; Loads once synchronously, then asynchronously refresh every 60 seconds
  (let [config-file-path (get-in opts [:options :config])]
    (when (some? config-file-path)
      (do
        (update-config! config-file-path db)
        (go-loop []
          (update-config! config-file-path db)
          (log/debug (str "Current config: " (:config @db)))
          (<! (timeout 60000))
          (recur)))))
  
  ;;
  ;; Load list of excluded runs from all exclude files
  ;; Store set of run IDs under :excluded-run-ids in db
  ;; Refresh every 10 seconds
  (go-loop []
    (let [exclude-file-paths (get-in @db [:config :exclude-files])]
      (when (some? exclude-file-paths)
        (dosync
         (swap! db (fn [x] (assoc x :excluded-run-ids #{})))
         (doseq [exclude-file-path exclude-file-paths]
           (if (.exists (io/file exclude-file-path))
             (with-open [rdr (io/reader exclude-file-path)]
               (swap! db (fn [x] (assoc x :excluded-run-ids (set/union (:excluded-run-ids x) (into #{} (line-seq rdr)))))))))))
      (log/debug (str "Excluded runs: " (:excluded-run-ids @db)))
      (<! (timeout 10000))
      (recur)))

  
  (def runs-to-analyze-chan (chan))

  ;;
  ;; Scan through contents of run-dirs
  ;; Include only directories (not files)
  ;; Exclude directories that don't match the illumina run directory naming scheme
  ;; Exclude those without 'upload_complete.json' file
  ;; Exclude those runs whose run ID is listed in an exclude file
  ;; Add the remaining directories to the runs-to-analyze channel
  ;; Park for 10 seconds
  ;; Recur
  (go-loop []
    (let [run-dirs (get-in @db [:config :run-dirs])]
      (doseq [run-dir run-dirs]
        (->> run-dir
             io/file
             .listFiles
             (map (memfn getCanonicalPath))
             (filter #(.isDirectory (io/file %)))
             (filter matches-run-directory-regex?)
             (filter upload-complete?)
             (filter #(not (analyzed? %)))
             (filter #(not (contains? (:excluded-run-ids @db) (.getName (io/file %)))))
             (#(doseq [run %] (go (>! runs-to-analyze-chan run)))))))
    (<! (timeout 10000))
    (recur))


  ;;
  ;; Main loop
  ;; Take directory path from runs-to-analyze channel
  ;; Run nextflow pipeline
  ;; Recur with the next directory path on the channel
  (loop [run (<!! runs-to-analyze-chan)]
    (if (some? run)
      (do
        (log/info (str "Analysis started: " run))
        (run-nextflow! {:run-dir run
                        :revision "main"})
        (log/info (str "Analysis complete: " run))))
    (recur (<!! runs-to-analyze-chan))))




(comment
  ;;
  ;; Useful forms for REPL-driven development

  (def opts {:options {:config "config.edn"}})
  
  (defn mock-analyze!
    ""
    [{:keys [run-dir]}]
    (do
      (log/info (str "Analyzing: " run-dir))
      (Thread/sleep 10000)
      (log/info (str "Analysis complete: " run-dir))))
  )
