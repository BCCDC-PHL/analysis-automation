(ns auto-ncov.core
  (:require [clojure.java.io :as io]
            [clojure.edn :as edn]
            [clojure.set :as set]
            [clojure.tools.logging :as log]
            [clojure.pprint :refer [pprint]]
            [clojure.tools.cli :refer [parse-opts]]
            [clojure.string :as str]
            [clojure.java.shell :as shell :refer [sh]]
            [clojure.core.async :as async :refer [go go-loop chan onto-chan! <! <!! >! >!! timeout]]
            [clojure.data.json :as json]
            [nrepl.server :refer [start-server stop-server]]
            [auto-ncov.cli :as cli]
            [auto-ncov.samplesheet :as samplesheet])
  (:gen-class))


;;
;; Remove me
(defn mock-analyze!
    ""
    [symlink-dir db]
    (do
      (swap! db assoc :currently-analyzing symlink-dir)
      (log/debug (str "Analysis started: " symlink-dir))
      (go (<! (timeout 10000))
          (swap! db assoc :currently-analyzing nil))
      (log/debug (str "Analysis complete: " symlink-dir))))
;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(defn load-edn!
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


(defn update-config!
  "Read config from file and insert into db under key :config"
  [config-file-path db]
  (if (.exists (io/file config-file-path))
    (let [config (load-edn! config-file-path)]
      (swap! db (fn [x] (assoc x :config config))))))


(defn update-excluded-runs!
  ""
  [db]
  (let [exclude-file-paths (get-in @db [:config :exclude-files])]
    (when (some? exclude-file-paths)
      (dosync
       (swap! db (fn [x] (assoc x :excluded-run-ids #{})))
       (doseq [exclude-file-path exclude-file-paths]
         (if (.exists (io/file exclude-file-path))
           (with-open [rdr (io/reader exclude-file-path)]
             (swap! db (fn [x] (assoc x :excluded-run-ids (set/union (:excluded-run-ids x) (into #{} (line-seq rdr)))))))))))))


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


(defn symlink-dir-exists?
  ""
  [run-dir symlinks-dir]
  (let [run-id (.getName (io/file run-dir))
        symlinks-dir-path (str/join \/ [symlinks-dir run-id])]
    (.exists (io/file symlinks-dir-path))))


(defn symlinks-complete?
  "Given a run "
  [symlinks-dir]
  (let [symlinks-complete-file (io/file symlinks-dir "symlinks_complete.json")]
    (.exists symlinks-complete-file)))


(defn analysis-dir-exists?
  "Given a fastq symlinks directory, check if the corresponding
   analysis output directory exists.

   takes:
     symlinks-dir: path (String)
     analysis-output-dir path (String)

   returns:
     boolean
  "
  [symlinks-dir analysis-output-dir]
  (let [run-id (.getName (io/file symlinks-dir))
        analysis-dir (io/file analysis-output-dir run-id)]
    (.exists analysis-dir)))


(defn currently-analyzing?
  "Check if a run directory is currently being analyzed."
  [run-dir db]
  (= (:currently-analyzing @db) run-dir))


(defn scan-directory!
  "Scan a directory and return a list of all files in the directory"
  [dir]
  (->> dir
       io/file
       .listFiles
       (map (memfn getCanonicalPath))))


(defn filter-for-runs-to-symlink
  "Directories must match standard illumina naming scheme
   and contain an 'upload_complete.json' file indicating that
   they've  been completely uploaded to the server.
   Excludes any directories that have already been analyzed,
   or are included in excluded-run-ids."
  [paths symlinks-dir excluded-run-ids]
  (->> paths
       (filter #(.isDirectory (io/file %)))
       (filter matches-run-directory-regex?)
       (filter upload-complete?)
       (filter #(not (symlink-dir-exists? % symlinks-dir)))
       (filter #(not (contains? excluded-run-ids (.getName (io/file %)))))))


(defn scan-for-runs-to-symlink!
  "Scan through multiple run directories for runs to analyze"
  [run-dirs symlinks-dir excluded-run-ids]
  (-> (map scan-directory! run-dirs)
      flatten
      (filter-for-runs-to-symlink symlinks-dir excluded-run-ids)))


(defn filter-for-runs-to-analyze
  "Directories must match standard illumina naming scheme
   and contain an 'symlinks_complete.json' file indicating that
   fastq symlinks have been created.
   Excludes any directories that have already been analyzed,
   or are included in excluded-run-ids.

   takes:
     paths: sequence of fastq symlinks directories ([String])
     db: global state database (Atom)

   returns:
     filtered sequence of fastq symlinks directories ([String])
  "
  [paths db]
  (let [excluded-run-ids    (get @db :excluded-run-ids)
        analysis-output-dir (get-in @db [:config :analysis-output-dir])]
    (->> paths
         (filter #(.isDirectory (io/file %)))
         (filter matches-run-directory-regex?)
         (filter symlinks-complete?)
         (filter #(not (analysis-dir-exists? % analysis-output-dir)))
         (filter #(not (contains? excluded-run-ids (.getName (io/file %))))))))


(defn scan-for-runs-to-analyze!
  "Scan through symlinks directory for runs to analyze"
  [symlinks-dir db]
  (-> (scan-directory! symlinks-dir)
      (filter-for-runs-to-analyze db))) 


(defn find-samplesheet!
  ""
  [run-dir]
  (->> (io/file run-dir)
      .listFiles
      (filter #(re-find #"SampleSheet[a-zA-Z0-9\-_]*.csv" (.getName %)))
      first
      str))


(defn determine-sequencer-type
  ""
  [run-id]
  (cond
    (re-matches #"\d{6}_M[0-9]{5}_\d{4}_[0]{9}-[A-Z0-9]{5}" run-id) :miseq
    (re-matches #"\d{6}_VH[0-9]{5}_\d+_[A-Z0-9]{9}" run-id) :nextseq
    :else :unknown))


(defn get-project-library-ids-from-samplesheet!
  ""
  [samplesheet-path project-id]
  (let [run-id (.getName (.getParentFile (io/file samplesheet-path)))
        samplesheet-lines (with-open [rdr (io/reader samplesheet-path)]
                            (-> (doall (line-seq rdr))))
        sequencer-type (determine-sequencer-type run-id)]
    (cond (= (determine-sequencer-type run-id) :miseq)
          (samplesheet/parse-project-library-ids-miseq samplesheet-lines project-id)
          (= (determine-sequencer-type run-id) :nextseq)
          (samplesheet/parse-project-library-ids-nextseq samplesheet-lines project-id)
          :else '())))


(defn find-fastq-directory!
  "Given a run directory, find the directory containing fastq files.
   MiSeqs store fastq files under Data/Intensities/BaseCalls.
   NextSeqs store fastq files under Analysis/N/Data/fastq, where N is
   the number of times that demultiplexing has been performed.
   In most cases, N is 1, but in cases where we have repeated demultiplexing,
   we want to take the fastq files from the most recent (highest numbered) Analysis directory."
  [run-dir]
  (let [sequencer-type (determine-sequencer-type (.getName (io/file run-dir)))]
    (cond (= sequencer-type :miseq) (.getCanonicalPath (io/file run-dir "Data" "Intensities" "BaseCalls"))
          (= sequencer-type :nextseq) (-> (io/file run-dir "Analysis")
                                          .listFiles
                                          last
                                          (io/file "Data" "fastq")
                                          .getCanonicalPath)
          :else (throw (Exception. "Unknown sequencer type. Cannot find fastq files.")))))


(defn symlink-one!
  "Create one symlink from src to dest. Ignores exceptions
   thrown when dest already exists."
  [src dest]
  (let [src-path (java.nio.file.Paths/get src (into-array String []))
        dest-path (java.nio.file.Paths/get dest (into-array String []))]
    (try
      (do
        (java.nio.file.Files/deleteIfExists dest-path)
        (java.nio.file.Files/createSymbolicLink dest-path src-path
                                              (into-array java.nio.file.attribute.FileAttribute [])))
      (catch java.io.IOException e)
      (catch java.nio.file.FileAlreadyExistsException e))))


(defn symlink!
  "Create symlinks from run-dir to symlinks-dir, for samples belonging to project-id."
  [run-dir symlinks-dir project-id]
  (let [run-id              (.getName (io/file run-dir))
        fastq-src-dir       (find-fastq-directory! run-dir)
        fastq-src-paths     (filter #(re-find #".fastq.gz$" %)
                                    (map (memfn getCanonicalPath)
                                         (.listFiles (io/file fastq-src-dir))))
        symlinks-dest-dir   (.getCanonicalPath (io/file symlinks-dir run-id))
        samplesheet-path    (find-samplesheet! run-dir)
        project-library-ids (get-project-library-ids-from-samplesheet! samplesheet-path project-id)
        symlinks-complete   {:destination_directory symlinks-dest-dir
                             :num_symlinks_created (count project-library-ids)
                             :source_directory fastq-src-dir}]
    (do
      (.mkdir (io/file symlinks-dest-dir))
      (doseq [library-id project-library-ids]
        (let [r1-regex (re-pattern (str library-id "_S\\d+_L\\d+_R1_\\d+.fastq.gz"))
              src-r1   (first (filter #(re-find r1-regex %) fastq-src-paths))
              dest-r1  (.getPath (io/file symlinks-dest-dir (str library-id "_R1.fastq.gz")))
              r2-regex (re-pattern (str library-id "_S\\d+_L\\d+_R2_\\d+.fastq.gz"))
              src-r2   (first (filter #(re-find r2-regex %) fastq-src-paths))
              dest-r2  (.getPath (io/file symlinks-dest-dir (str library-id "_R2.fastq.gz")))]
          (symlink-one! src-r1 dest-r1)
          (symlink-one! src-r2 dest-r2)))
      (spit (str (io/file symlinks-dest-dir "symlinks_complete.json"))
            (with-out-str (json/pprint symlinks-complete :escape-slash false)))
      (log/debug (str "Symlinks complete: " symlinks-dest-dir)))))


(defn run-ncov2019-artic-nf!
  "Run the BCCDC-PHL/ncov2019-artic-nf pipeline on a run directory.
   When the analysis completes, delete the 'work' directory."
  [db]
  (let [symlinks-dir        (get-in @db [:config :symlinks-dir])
        analysis-output-dir (get-in @db [:config :analysis-output-dir])
        pipeline-version    (get-in @db [:config :ncov2019-artic-nf-config :version])
        run-id              (.getName (io/file symlinks-dir))
        work-dir            (str (io/file analysis-output-dir run-id (str "work-" (java.util.UUID/randomUUID))))
        outdir              (str (io/file analysis-output-dir run-id (str "ncov2019-artic-nf-" pipeline-version "-output")))
        log-file            (str (io/file outdir "nextflow.log"))
        ref                 (get-in @db [:config :ncov2019-artic-nf-config :ref])
        bed                 (get-in @db [:config :ncov2019-artic-nf-config :bed])
        primer-pairs-tsv    (get-in @db [:config :ncov2019-artic-nf-config :primer-pairs-tsv])
        gff                 (get-in @db [:config :ncov2019-artic-nf-config :gff])
        composite-ref       (get-in @db [:config :ncov2019-artic-nf-config :composite-ref])]
    (do
      (swap! db assoc :currently-analyzing run-id)
      (sh "mkdir" "-p" outdir)
      (sh "chmod" "750" outdir)
      (sh "mkdir" "-p" work-dir)
      (sh "chmod" "750" work-dir)
      (shell/with-sh-dir outdir
        (apply sh ["nextflow"
                   "-q"
                   "-log" log-file
                   "run" "BCCDC-PHL/ncov2019-artic-nf"
                   "-profile" "conda"
                   "--cache" (str (io/file (System/getProperty "user.home") ".conda/envs"))
                   "-r" pipeline-version
                   "--ref" ref
                   "--bed" bed
                   "--primer_pairs_tsv" primer-pairs-tsv
                   "--gff" gff
                   "--composite_ref" composite-ref
                   "-work-dir" work-dir
                   "--outdir" "."]))
      (sh "rm" "-r" work-dir)
      (sh "find" outdir "-type" "d" "-exec" "chmod" "750" "{}" "+")
      (sh "find" outdir "-type" "f" "-exec" "chmod" "640" "{}" "+")
      (swap! db assoc :currently-analyzing nil))))


(defn run-ncov-tools-nf!
  "Run the BCCDC-PHL/ncov-tools-nf pipeline on a run directory.
   When the analysis completes, delete the 'work' directory."
  [{:keys [artic-analysis-dir revision]} db]
  (let [run-id (.getName (.getParentFile (io/file artic-analysis-dir)))
        work-dir (str/join \/ [artic-analysis-dir (str "work-" (java.util.UUID/randomUUID))])
        outdir (str/join \/ [artic-analysis-dir (str "ncov-tools-" revision "-output")])
        log-file (str/join \/ [outdir "nextflow.log"])]
    (do
      (swap! db assoc-in [:currently-analyzing :ncov-tools-nf] run-id)
      (sh "mkdir" "-p" outdir)
      (sh "chmod" "750" outdir)
      (sh "mkdir" "-p" work-dir)
      (sh "chmod" "750" work-dir)
      (shell/with-sh-dir outdir
        (apply sh ["nextflow"
                   "-q"
                   "-log" log-file
                   "run" "BCCDC-PHL/ncov-tools-nf"
                   "-profile" "conda"
                   "--cache" (str/join \/ [(System/getProperty "user.home") ".conda/envs"])
                   "-r" revision
                   ;; ...TODO...
                   "-work-dir" work-dir
                   "--outdir" "."]))
      (sh "rm" "-r" work-dir)
      (sh "find" outdir "-type" "d" "-exec" "chmod" "750" "{}" "+")
      (sh "find" outdir "-type" "f" "-exec" "chmod" "640" "{}" "+")
      (swap! db assoc-in [:currently-analyzing :ncov-tools-nf nil]))))


(defn analyze!
  ""
  [run db]
  (mock-analyze! run db))


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
  (defonce db (atom {}))
  
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
  ;; Start up REPL when configured to do so
  (when (get-in @db [:config :repl])
    (let [uuid (java.util.UUID/randomUUID)
          socket-dir (str "/tmp/auto-ncov-" (System/getProperty "user.home"))]
      (do
        (sh "mkdir" "-p" socket-dir)
        (sh "chmod" "700" socket-dir)
        (defonce server (start-server :socket (str socket-dir "/auto-ncov-" uuid ".sock"))))))

  ;;
  ;; Load list of excluded runs from all exclude files
  ;; Store set of run IDs under :excluded-run-ids in db
  ;; Updates asynchronously every 10 seconds
  (go-loop []
    (update-excluded-runs! db)
    (<! (timeout 10000))
    (recur))

  
  (def runs-to-symlink-chan (chan))

  (def runs-to-analyze-chan (chan))

  ;;
  ;; Scan through contents of run-dirs
  ;; Include only directories (not files)
  ;; Exclude directories that don't match the illumina run directory naming scheme
  ;; Exclude those without 'upload_complete.json' file
  ;; Exclude those runs whose run ID is listed in an exclude file
  ;; Exclude a run if it is currently being analyzed
  ;; Take the first run from the remaining list
  ;; Put it on the runs-to-analyze channel
  ;; Park for 10 seconds
  ;; Recur
  (go-loop []
    (log/debug "Scanning for runs to symlink...")
    (let [run-dirs (get-in @db [:config :run-dirs])
          symlinks-dir (get-in @db [:config :symlinks-dir])
          _ (update-excluded-runs! db)
          excluded-run-ids (get-in @db [:excluded-run-ids])]
      (log/debug (str "Excluded runs: " excluded-run-ids))
      (->> (scan-for-runs-to-symlink! run-dirs symlinks-dir excluded-run-ids)
           first
           (#(when-some [run %]
               (do
                 (log/debug (str "Putting on symlink channel: " run))
                 (go (>! runs-to-symlink-chan run)))))))
    (<! (timeout 10000))
    (recur))


  ;;
  ;; Take directory path from runs-to-symlink channel
  ;; Create symlinks
  ;; Recur with the next directory path on the channel
  (go-loop [run (<! runs-to-symlink-chan)]
    (log/debug (str "Took from symlink channel: " run))
    (when-some [r run]
      (let [symlinks-dir (get-in @db [:config :symlinks-dir])
            project-id (get-in @db [:config :samplesheet-project-id])]
        (symlink! run symlinks-dir project-id)))
    (recur (<! runs-to-symlink-chan)))


  ;;
  ;; Scan through contents of run-dirs
  ;; Include only directories (not files)
  ;; Exclude directories that don't match the illumina run directory naming scheme
  ;; Exclude those without 'upload_complete.json' file
  ;; Exclude those runs whose run ID is listed in an exclude file
  ;; Exclude a run if it is currently being analyzed
  ;; Take the first run from the remaining list
  ;; Put it on the runs-to-analyze channel
  ;; Park for 10 seconds
  ;; Recur
  (go-loop []
    (log/debug "Scanning for runs to analyze...")
    (let [run-dirs (get-in @db [:config :run-dirs])
          symlinks-dir (get-in @db [:config :symlinks-dir])
          _ (update-excluded-runs! db)
          excluded-run-ids (get-in @db [:excluded-run-ids])]
      (log/debug (str "Excluded runs: " excluded-run-ids))
      (->> (scan-for-runs-to-analyze! symlinks-dir db)
           (filter #(not= (get @db :currently-analyzing) %)) 
           first
           (#(when-some [run %]
               (do
                 (log/debug (str "Putting on analysis channel: " run))
                 (go (>! runs-to-analyze-chan run)))))))
    (<! (timeout 10000))
    (recur))


  ;;
  ;; Take directory path from runs-to-analyze channel
  ;; Analyze runs
  ;; Recur with the next directory path on the channel
  (go-loop [run (<! runs-to-analyze-chan)]
    (log/debug (str "Took from analysis channel: " run))
    (when-some [r run]
        (analyze! run db))
    (recur (<! runs-to-analyze-chan)))

  ;;
  ;; Main loop
  (loop []
    (recur)))


(comment
  ;;
  ;; Useful forms for REPL-driven development

  (def opts {:options {:config "dev-config.edn"}})

  ;; Reload config
  (update-config! (get-in opts [:options :config]) db)

  (update-excluded-runs! db)

  ;; Clear excluded run list
  (swap! db (fn [x] (assoc x :excluded-run-ids #{})))

  ;; Reload excluded run list from exclude files
  (doseq [exclude-file-path (get-in @db [:config :exclude-files])]
    (if (.exists (io/file exclude-file-path))
      (with-open [rdr (io/reader exclude-file-path)]
        (swap! db (fn [x] (assoc x :excluded-run-ids (set/union (:excluded-run-ids x) (into #{} (line-seq rdr)))))))))
  
  (defn mock-analyze!
    ""
    [{:keys [run-dir]} db]
    (do
      (swap! db assoc :currently-analyzing run-dir)
      (go (<! (timeout 10000))
          (swap! db assoc :currently-analyzing nil))))
  
  )
