(ns auto-ncov.core
  (:require [clojure.java.io :as io]
            [clojure.edn :as edn]
            [clojure.set :as set]
            [clojure.tools.logging :as log]
            [clojure.pprint :refer [pprint]]
            [clojure.tools.cli :refer [parse-opts]]
            [clojure.string :as str]
            [clojure.java.shell :as shell :refer [sh]]
            [clojure.core.async :as async :refer [go go-loop chan onto-chan! put! take! <! <!! >! >!! timeout]]
            [clojure.data.json :as json]
            [nrepl.server :refer [start-server stop-server]]
            [auto-ncov.cli :as cli]
            [auto-ncov.samplesheet :as samplesheet])
  (:import [java.time.format DateTimeFormatter]
           [java.time ZonedDateTime])
  (:gen-class))


(defn now!
  "Generate an ISO-8601 timestamp (`YYYY-MM-DDTHH:mm:ss.xxxx-OFFSET`).
   eg: `2022-02-11T14:13:49.9335-08:00`
  
   takes:
   returns:
     ISO-8601 timestamp for the current time (`String`)
  "
  []
  (.. (ZonedDateTime/now) (format DateTimeFormatter/ISO_OFFSET_DATE_TIME)))


(defn load-edn!
  "Load edn from an io/reader source (filename or io/resource).
   https://clojuredocs.org/clojure.edn/read#example-5a68f384e4b09621d9f53a79

   takes:
     `source`: Path to edn file to load (`String`)

   returns:
     Data structure as determined by `source`.
  "
  [source]
  (try
    (with-open [r (io/reader source)]
      (edn/read (java.io.PushbackReader. r)))

    (catch java.io.IOException e
      (printf "Couldn't open '%s': %s\n" source (.getMessage e)))
    (catch RuntimeException e
      (printf "Error parsing edn file '%s': %s\n" source (.getMessage e)))))


(defn update-config!
  "Read config from file and insert into `db` under key `:config`.

   takes:
     `config-file-path`: Path to config file (`String`)
     `db`: Global app-state db (`Atom`)

   returns:
     `nil`
  "
  [config-file-path db]
  (if (.exists (io/file config-file-path))
    (let [config (load-edn! config-file-path)]
      (swap! db (fn [x] (assoc x :config config))))))


(defn update-excluded-runs!
  "Update `db` by loading excluded run IDs from files and inserting under
   key `:excluded-run-ids`.

   takes:
     `db`: Global app-state db (`Atom`)

   returns:
     `nil`
  "
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
  "Check that the directory name matches one of the standard illumina directory name formats.

   takes:
     `dir`: Path to directory to be checked. (`String`)

   returns:
     `Boolean`
  "
  [dir]
  (->> dir
       io/file
       .getName
       (re-matches #"\d{6}_[A-Z0-9]+_\d+_[A-Z0-9\-]+")
       some?))


(defn upload-complete?
  "Check for presence of upload_complete.json file.

   takes:
     `run-dir`: Illumina sequencer output directory. (`String`)

   returns:
     `Boolean`
  "
  [run-dir]
  (let [upload-complete-path (io/file run-dir "upload_complete.json")]
    (.exists upload-complete-path)))


(defn symlink-dir-exists?
  "Check if the corresponding symlinks dir for `run-dir` exists.

   takes:
     `run-dir`: Path to illumina sequencer output dir. (`String`)
     `symlinks-dir`: Path to top-level fastq symlinks dir. (`String`)

   returns:
     `Boolean`
  "
  [run-dir symlinks-dir]
  (let [run-id (.getName (io/file run-dir))
        symlinks-dir (io/file symlinks-dir run-id)]
    (.exists symlinks-dir)))


(defn symlinks-complete?
  "Check if symlinking is complete, based on the presence of a `symlinks_complete.json` file.
  
   takes:
     `symlinks-dir`: Path to fastq symlinks dir. (`String`)

   returns:
     `Boolean`
  "
  [symlinks-dir]
  (let [symlinks-complete-file (io/file symlinks-dir "symlinks_complete.json")]
    (.exists symlinks-complete-file)))


(defn analysis-dir-exists?
  "Given a fastq symlinks directory, check if the corresponding
   analysis output directory exists.

   takes:
     symlinks-dir: path (`String`)
     analysis-output-dir path (`String`)

   returns:
     `Boolean`
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
  "Scan a directory and return a list of all files in the directory.

   takes:
     `dir`: Path to directory to scan (`String`)

   returns:
     Sequence of (absolute) paths to contents of `dir`. (`[String]`)
  "
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
   or are included in excluded-run-ids.

   takes:
     `paths`: Sequence of paths to illumina sequencer output dirs. (`[String]`)
     `symlinks-dir`: Top-level fastq symlinks dir. (`String`)
     `excluded-run-ids`: Set of run IDs to exclude from symlinking (`#{String}`)

   returns:
     Filtered sequence of paths to illumina sequencer output dirs. (`[String]`)
  "
  [paths symlinks-dir excluded-run-ids]
  (->> paths
       (filter #(.isDirectory (io/file %)))
       (filter matches-run-directory-regex?)
       (filter upload-complete?)
       (filter #(not (symlink-dir-exists? % symlinks-dir)))
       (filter #(not (contains? excluded-run-ids (.getName (io/file %)))))))


(defn scan-for-runs-to-symlink!
  "Scan through multiple run directories for runs to symlink.

   takes:
     `db`: Global app-state db (Atom)

   returns:
     Sequence of illumina run directory paths available for symlinking ([String])
  "
  [db]
  (let [run-dirs              (get-in @db [:config :run-dirs])
        symlinks-dir          (get-in @db [:config :symlinks-dir])
        excluded-run-ids      (get @db :excluded-run-ids)]
    (-> (map scan-directory! run-dirs)
        flatten
        (filter-for-runs-to-symlink symlinks-dir excluded-run-ids))))


(defn filter-for-runs-to-analyze
  "Directories must match standard illumina naming scheme
   and contain an 'symlinks_complete.json' file indicating that
   fastq symlinks have been created.
   Excludes any directories that have already been analyzed,
   or are included in `excluded-run-ids`.

   takes:
     `paths`: sequence of fastq symlinks directory paths ([String])
     `db`: global state database (Atom)

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
  "Scan through symlinks directory for runs to analyze.

   takes:
     `db`: Global app-state db (Atom)

   returns:
     Sequence of fastq symlinks directories ([String])
  "
  [db]
  (let [symlinks-dir (get-in @db [:config :symlinks-dir])]
    (-> (scan-directory! symlinks-dir)
        (filter-for-runs-to-analyze db))))


(defn find-samplesheet!
  "Find the SampleSheet file within an illumina sequencer output directory.

   takes:
     `run-dir`: Path to illumina sequencer output directory (String)

   returns:
     Path to SampleSheet file. (String)
  "
  [run-dir]
  (->> (io/file run-dir)
      .listFiles
      (filter #(re-find #"SampleSheet[a-zA-Z0-9\-_]*.csv" (.getName %)))
      first
      str))


(defn determine-sequencer-type
  "Determine the type of sequencer, based on the run ID.

   takes:
     `run-id`: The run ID (`String`)

   returns:
     The sequencer type, one of: `:miseq`, `:nextseq`, or `:unknown`. (`Keyword`)  
  "
  [run-id]
  (cond
    (re-matches #"\d{6}_M[0-9]{5}_\d{4}_[0]{9}-[A-Z0-9]{5}" run-id) :miseq
    (re-matches #"\d{6}_VH[0-9]{5}_\d+_[A-Z0-9]{9}" run-id) :nextseq
    :else :unknown))


(defn get-project-library-ids-from-samplesheet!
  "Get a list of library IDs for libraries belonging to this project in the SampleSheet.

   takes:
     `samplesheet-path`: Path to SampleSheet file (`String`)
     `project-id`: Project ID used to identify samples for this project in the SampleSheet (`String`)

   returns:
     Sequence of library IDs. (`[String]`)
  "
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
   MiSeqs store fastq files under `Data/Intensities/BaseCalls`.
   NextSeqs store fastq files under `Analysis/N/Data/fastq`, where `N` is
   the number of times that demultiplexing has been performed.
   In most cases, `N` is 1, but in cases where we have repeated demultiplexing,
   we want to take the fastq files from the most recent (highest numbered) Analysis directory.

   takes:
     `run-dir`: Path to an illumina sequencer output directory. (`String`)

   returns:
      Path to fastq directory within `run-dir`. (`String`)

   throws:
     `Exception`: If sequencer type is not `:miseq` or `:nextseq`, cannot determine fastq directory.
  "
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
  "Create one symlink from `src` to `dest`. Ignores exceptions
   thrown when dest already exists.

   takes:
     `src`: Path to source file. (`String`)
     `dest`: Path to symlink destination. (`String`)

   returns:
     `nil`
  "
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
  "Create symlinks from run-dir to `symlinks-dir`, for samples belonging to `project-id`.

   takes:
     `run-dir`: Path to illumina sequencer output directory to create symlinks for. (`String`) 
     `symlinks-dir`: Path to top-level fastq symlinks directory for the project. (`String`)
     `project-id`: Identifier used to mark libraries as belonging to this project in the SampleSheet file. (`String`)

   returns:
     `nil`
  "
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
                             :source_directory run-dir}]
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
            (with-out-str (json/pprint (assoc symlinks-complete :timestamp (now!)) :escape-slash false)))
      (log/info (str "Symlinks complete: " symlinks-dest-dir)))))


(defn start-scanning-for-runs-to-symlink!
  "Starts the (asynchronous) process of scanning for runs to symlink.

   takes:
     `runs-to-symlink-chan`: Channel to put run directories on for symlinking. (`Channel`)
     `kill-chan`: Channel for killing the scanning process. (`Channel`)
     `db`: Global app-state db (`Atom`)

   returns:
     (`Channel`)
  "
  [runs-to-symlink-chan kill-chan db]
  (go-loop []
    (do
      (log/debug "Scanning for runs to symlink...")
      (let [run (first (scan-for-runs-to-symlink! db))]
        (when-some [r run]
          (do
            (log/info "Found directory to symlink: " run)
            (>! runs-to-symlink-chan r))))
      (let [symlinks-scanning-interval (get-in @db [:config :symlinks-scanning-interval-ms] 10000)
            [v ch] (async/alts! [(timeout symlinks-scanning-interval) kill-chan])]
        (if (= ch kill-chan)
          (log/info "Stopped scanning for runs to symlink.")
          (recur))))))


(defn start-symlinker!
  "Starts the (asynchronous) process of symlinking any directories put on `runs-to-symlink-chan`.

   takes:
     `runs-to-symlink-chan`: Channel to take run directories from for symlinking (`Channel`)
     `db`: Global app-state db (`Atom`)

   returns:
     (`Channel`)
  "
  [runs-to-symlink-chan db]
  (go-loop [run (<! runs-to-symlink-chan)]
    (log/debug (str "Took from symlink channel: " run))
    (when-some [r run]
      (let [symlinks-dir (get-in @db [:config :symlinks-dir])
            project-id   (get-in @db [:config :samplesheet-project-id])]
        (symlink! run symlinks-dir project-id)))
    (recur (<! runs-to-symlink-chan))))


(defn start-scanning-for-runs-to-analyze!
  "Starts the (asynchronous) process of scanning for runs to analyze.

   takes:
     `runs-to-analyze-chan`: Channel to put fastq symlink directories on for analysis. (`Channel`)
     `kill-chan`: Channel for killing the scanning process. (`Channel`)
     `db`: Global app-state db (`Atom`)

   returns:
     (`Channel`)
  "
  [runs-to-analyze-chan kill-chan db]
  (go-loop []
    (do
      (log/debug "Scanning for runs to analyze...")
      (let [run (first (scan-for-runs-to-analyze! db))]
        (when-some [r run]
          (do
            (log/info "Found directory to analyze: " run)
            (>! runs-to-analyze-chan r))))
      (let [analysis-scanning-interval (get-in @db [:config :analysis-scanning-interval-ms] 10000)
            [v ch] (async/alts! [(timeout analysis-scanning-interval) kill-chan])]
        (if (= ch kill-chan)
          (log/info "Stopped scanning for runs to analyze.")
          (recur))))))


(defn put-stop!
  "Stop a process by putting a value on the `kill-chan`.

   takes:
     `kill-chan`: Channel to put a value on to kill an async process. (`Channel`)

   returns:
     `boolean`
  "
  [kill-chan]
  (put! kill-chan :stop))


(defn run-ncov2019-artic-nf!
  "Run the BCCDC-PHL/ncov2019-artic-nf pipeline on a run directory.
   When the analysis completes, delete the 'work' directory.

   takes:
     `symlinks-dir`: Path to symlinks directory to analyze. (`String`)
     `db`: Global app-state db (`Atom`)

   returns:
     Path to analysis output directory. (`String`)
  "
  [symlinks-dir db]
  (let [analysis-output-dir    (get-in @db [:config :analysis-output-dir])
        pipeline-full-name     "BCCDC-PHL/ncov2019-artic-nf"
        pipeline-short-name    (second (str/split pipeline-full-name #"/"))
        pipeline-full-version  (get-in @db [:config :ncov2019-artic-nf-config :version])
        pipeline-minor-version (str/join "." (take 2 (str/split pipeline-full-version #"\.")))
        run-id                 (.getName (io/file symlinks-dir))
        work-dir               (str (io/file analysis-output-dir run-id (str "work-" pipeline-short-name "-" (java.util.UUID/randomUUID))))
        outdir                 (str (io/file analysis-output-dir run-id (str pipeline-short-name "-" pipeline-minor-version "-output")))
        log-file               (str (io/file outdir "nextflow.log"))
        ref                    (get-in @db [:config :ncov2019-artic-nf-config :ref])
        bed                    (get-in @db [:config :ncov2019-artic-nf-config :bed])
        primer-pairs-tsv       (get-in @db [:config :ncov2019-artic-nf-config :primer-pairs-tsv])
        gff                    (get-in @db [:config :ncov2019-artic-nf-config :gff])
        composite-ref          (get-in @db [:config :ncov2019-artic-nf-config :composite-ref])]
    (do
      (swap! db assoc :currently-analyzing run-id)
      (log/info (str "Started " pipeline-short-name " analysis: " symlinks-dir))
      (sh "mkdir" "-p" outdir)
      (sh "chmod" "750" outdir)
      (sh "mkdir" "-p" work-dir)
      (sh "chmod" "750" work-dir)
      (shell/with-sh-dir outdir
        (apply sh ["nextflow"
                   "-log" log-file
                   "run" pipeline-full-name
                   "-profile" "conda"
                   "--cache" (str (io/file (System/getProperty "user.home") ".conda/envs"))
                   "-r" pipeline-full-version
                   "--illumina"
                   "--directory" symlinks-dir
                   "--prefix" run-id
                   "--ref" ref
                   "--bed" bed
                   "--primer_pairs_tsv" primer-pairs-tsv
                   "--gff" gff
                   "--composite_ref" composite-ref
                   "-work-dir" work-dir
                   "--outdir" "."]))
      
      (sh "find" outdir "-type" "d" "-exec" "chmod" "750" "{}" "+")
      (sh "find" outdir "-type" "f" "-exec" "chmod" "640" "{}" "+")
      (log/info (str "Finished " pipeline-short-name " analysis: " symlinks-dir))
      (go (sh "rm" "-r" work-dir)))
    outdir))


(defn run-ncov-tools-nf!
  "Run the BCCDC-PHL/ncov-tools-nf pipeline on an artic analysis directory.
   When the analysis completes, delete the 'work' directory.

   takes:
     `artic-analysis-dir`: Path to artic analysis output directory. (`String`)
     `db`: Global app-state db (`Atom`)
   returns:
     `nil`
  "
  [artic-analysis-dir db]
  (let [run-id                  (.getName (.getParentFile (io/file artic-analysis-dir)))
        pipeline-full-name      "BCCDC-PHL/ncov-tools-nf"
        pipeline-short-name     (second (str/split pipeline-full-name #"/"))
        pipeline-full-version   (get-in @db [:config :ncov-tools-nf-config :version])
        pipeline-minor-version  (str/join "." (take 2 (str/split pipeline-full-version #"\.")))
        ncov-watchlists-version (get-in @db [:config :ncov-tools-nf-config :ncov-watchlists-version])
        work-dir                (str (io/file (.getParentFile (io/file artic-analysis-dir)) (str "work-" pipeline-short-name "-" (java.util.UUID/randomUUID))))
        outdir                  (str (io/file artic-analysis-dir (str "ncov-tools-" pipeline-minor-version "-output")))
        log-file                (str (io/file outdir "nextflow.log"))]
    (do
      (log/info (str "Started " pipeline-short-name " analysis: " artic-analysis-dir))
      (sh "mkdir" "-p" outdir)
      (sh "chmod" "750" outdir)
      (sh "mkdir" "-p" work-dir)
      (sh "chmod" "750" work-dir)
      (shell/with-sh-dir outdir
        (apply sh ["nextflow"
                   "-log" log-file
                   "run" pipeline-full-name
                   "-profile" "conda"
                   "--cache" (str (io/file (System/getProperty "user.home") ".conda/envs"))
                   "-r" pipeline-full-version
                   "--artic_analysis_dir" artic-analysis-dir
                   "--ncov_watchlists_version" ncov-watchlists-version
                   "--run_name" run-id
                   "--downsampled"
                   "--split_by_plate"
                   "--freebayes_consensus"
                   "--freebayes_variants"
                   "-work-dir" work-dir
                   "--outdir" "."]))

      (sh "find" outdir "-type" "d" "-exec" "chmod" "750" "{}" "+")
      (sh "find" outdir "-type" "f" "-exec" "chmod" "640" "{}" "+")
      (swap! db assoc :currently-analyzing nil)
      (log/info (str "Finished " pipeline-short-name " analysis: " artic-analysis-dir))
      (go (sh "rm" "-r" work-dir)))))


(defn analyze!
  ""
  [run db]
  (-> (run-ncov2019-artic-nf! run db)
      (run-ncov-tools-nf! db)))


(defn start-analyzer!
  ""
  [runs-to-analyze-chan db]
  (go-loop [run (<! runs-to-analyze-chan)]
    (log/debug (str "Took from analysis channel: " run))
    (when-some [r run]
      (analyze! run db))
    (recur (<! runs-to-analyze-chan))))


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
          (let [config-reload-interval (get-in opts [:config :config-reload-interval-ms] 60000)]
            (update-config! config-file-path db)
            (log/debug (str "Current config: " (:config @db)))
            (<! (timeout config-reload-interval))
            (recur))))))

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
    (let [exclude-files-reload-interval (get-in opts [:config :exclude-files-reload-interval-ms] 60000)]
      (update-excluded-runs! db)
      (<! (timeout exclude-files-reload-interval))
      (recur)))

  
  (defonce runs-to-symlink-chan (chan))
  (defonce kill-symlinking-chan (chan))

  (defonce runs-to-analyze-chan (chan))
  (defonce kill-analysis-chan (chan))

  (start-symlinker! runs-to-symlink-chan db)
  (start-scanning-for-runs-to-symlink! runs-to-symlink-chan kill-symlinking-chan db)

  (start-analyzer! runs-to-analyze-chan db)
  (start-scanning-for-runs-to-analyze! runs-to-analyze-chan kill-analysis-chan db)

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
  
  (defn mock-analyze!
    ""
    [{:keys [run-dir]} db]
    (do
      (swap! db assoc :currently-analyzing run-dir)
      (go (<! (timeout 10000))
          (swap! db assoc :currently-analyzing nil))))
  
  )
