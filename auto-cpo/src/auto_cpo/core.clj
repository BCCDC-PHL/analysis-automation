(ns auto-cpo.core
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
            [auto-cpo.cli :as cli]
            [auto-cpo.samplesheet :as samplesheet])
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
  (let [exclude-file-paths (get-in @db [:config :run-exclude-files])]
    (when (some? exclude-file-paths)
      (dosync
       (swap! db (fn [x] (assoc x :excluded-run-ids #{})))
       (doseq [exclude-file-path exclude-file-paths]
         (if (.exists (io/file exclude-file-path))
           (with-open [rdr (io/reader exclude-file-path)]
             (swap! db (fn [x] (assoc x :excluded-run-ids (set/union (:excluded-run-ids x) (into #{} (line-seq rdr)))))))))))))


(defn update-excluded-libraries!
  "Update `db` by loading excluded run IDs from files and inserting under
   key `:excluded-library-ids`.

   takes:
     `db`: Global app-state db (`Atom`)

   returns:
     `nil`
  "
  [db]
  (let [exclude-file-paths (get-in @db [:config :library-exclude-files])]
    (when (some? exclude-file-paths)
      (dosync
       (swap! db (fn [x] (assoc x :excluded-library-ids #{})))
       (doseq [exclude-file-path exclude-file-paths]
         (if (.exists (io/file exclude-file-path))
           (with-open [rdr (io/reader exclude-file-path)]
             (swap! db (fn [x] (assoc x :excluded-library-ids (set/union (:excluded-library-ids x) (into #{} (line-seq rdr)))))))))))))


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
        symlinks-dir          (get-in @db [:config :fastq-symlinks-dir])
        excluded-run-ids      (get @db :excluded-run-ids)]
    (-> (map scan-directory! run-dirs)
        flatten
        (filter-for-runs-to-symlink symlinks-dir excluded-run-ids))))


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


(defn find-fastqs-by-library-id
  "Look for fastq files for a specific library ID within an illumina sequencer output directory.

   takes:
     `run-dir`:
     `library-id`:

   returns:
     Vector of paths to fastq files for the library matching `library-id`. (`[String]`)
  "
  [run-dir library-id]
  (let [fastq-dir (find-fastq-directory! run-dir)
        fastq-paths (filter #(re-find #".fastq.gz$" %)
                            (map (memfn getCanonicalPath)
                                 (.listFiles (io/file fastq-dir))))
        r1-regex  (re-pattern (str library-id "_S\\d+_L\\d+_R1_\\d+.fastq.gz"))
        fastq-r1  (first (filter #(re-find r1-regex %) fastq-paths))
        r2-regex  (re-pattern (str library-id "_S\\d+_L\\d+_R2_\\d+.fastq.gz"))
        fastq-r2  (first (filter #(re-find r2-regex %) fastq-paths))]
    [fastq-r1 fastq-r2]))


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


(defn scan-for-libraries-to-symlink!
  "Scan through a run directory for libraries to symlink.

   takes:
     `run-dir`: Path to illumina sequencer output directory (`String`)
     `db`: Global app-state db (`Atom`)

   returns:
     Sequence of pairs of fastq paths (`[[String]]`)
     eg:
       ```
       ([\"/path/to/sample-01_R1.fastq.gz\"
         \"/path/to/sample-01_R2.fastq.gz\"]
        [\"/path/to/sample-02_R1.fastq.gz\"
         \"/path/to/sample-02_R2.fastq.gz\"]
        ...)
       ```
  "
  [run-dir db]
  (let [symlinks-dir        (get-in @db [:config :fastq-symlinks-dir])
        excluded-run-ids    (get @db :excluded-run-ids)
        project-id          (get-in @db [:config :samplesheet-project-id])
        samplesheet-path    (find-samplesheet! run-dir)
        project-library-ids (get-project-library-ids-from-samplesheet! samplesheet-path project-id)]
    (map #(find-fastqs-by-library-id run-dir %) project-library-ids)))


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
  (let [symlinks-dir (get-in @db [:config :fastq-symlinks-dir])]
    (-> (scan-directory! symlinks-dir)
        (filter-for-runs-to-analyze db))))


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
  (try
    (let [src-path (java.nio.file.Paths/get src (into-array String []))
          dest-path (java.nio.file.Paths/get dest (into-array String []))
          dest-parent-dir (.getParentFile (io/file (str dest-path)))]
      (do
        (when (not (.exists dest-parent-dir))
          (do
            (log/info "Creating directory: " (str dest-parent-dir))
            (io/make-parents (str dest-path))))
        (java.nio.file.Files/deleteIfExists dest-path)
        (java.nio.file.Files/createSymbolicLink dest-path src-path
                                                (into-array java.nio.file.attribute.FileAttribute []))))
    (catch java.lang.NullPointerException e)
    (catch java.io.IOException e)
    (catch java.nio.file.FileAlreadyExistsException e)))


(defn get-fastq-paths!
  "Get paths for any libraries in `run-dir` that 

   takes:
     `project-id`: Identifier used to mark libraries as belonging to this project in the SampleSheet file. (`String`)
     `run-dir`: Path to illumina sequencer output directory to create symlinks for. (`String`)
     

   returns:
     Sequence of maps, each with keys:
       `:library-id` Library ID (`String`)
       `:r1-path`    Path to R1 fastq file (`String`)
       `:r2-path`    Path to R2 fastq file (`String`)
  "
  [project-id run-dir]
  (let [run-id              (.getName (io/file run-dir))
        fastq-src-dir       (find-fastq-directory! run-dir)
        fastq-src-paths     (filter #(re-find #".fastq.gz$" %)
                                    (map (memfn getCanonicalPath)
                                         (.listFiles (io/file fastq-src-dir))))
        samplesheet-path    (find-samplesheet! run-dir)
        project-library-ids (get-project-library-ids-from-samplesheet! samplesheet-path project-id)]
    (->> project-library-ids
         (map (fn [library-id]
                (let [r1-regex (re-pattern (str library-id "_S\\d+_L\\d+_R1_\\d+.fastq.gz"))
                      r1-path  (first (filter #(re-find r1-regex %) fastq-src-paths))
                      r2-regex (re-pattern (str library-id "_S\\d+_L\\d+_R2_\\d+.fastq.gz"))
                      r2-path  (first (filter #(re-find r2-regex %) fastq-src-paths))]
                      {:library-id   library-id
                       :r1-path  r1-path
                       :r2-path  r2-path}))))))


(defn fastq-symlinks-subdir-by-year
  "Given a map with library ID and fastq paths, determine the sub-directory
   under the `:fastq-symlinks-dir` config value to create fastq symlinks,
   based on the sample-collection year that is embedded in the library ID for
   CPO samples.

   If the year cannot be determined from the ID, the current year at the time of
   symlinking will be used.

   takes:
     `library-fastq-paths`:
       `Map` with keys:
         `:library-id` Library ID (`String`)
         `:r1-path`    Path to R1 fastq file (`String`)
         `:r2-path`    Path to R2 fastq file (`String`)
      `fastq-symlinks-dir`: Path to directory to create fastq symlinks in (`String`)

   returns:
     Sub-directory name (`String`)
  "
  [library-fastq-paths]
  (let [library-id (:library-id library-fastq-paths)
        current-year (first (str/split (now!) #"-"))
        current-two-digit-year (apply str (drop 2 current-year))
        library-year-regex #"BC(\d{2})[A-Z]"
        library-two-digit-year-matches (re-find library-year-regex library-id)]
    (if (> (count library-two-digit-year-matches) 1)
      (second library-two-digit-year-matches)
      current-two-digit-year)))


(defn add-symlink-dest-path
  "Given a map of fastq file paths, add paths to symlink destinations.

   takes:
     `library-fastq-paths`:
       `Map` with keys:
         `:library-id` Library ID (`String`)
         `:r1-path`    Path to R1 fastq file (`String`)
         `:r2-path`    Path to R2 fastq file (`String`)
      `fastq-symlinks-dir`: Path to directory to create fastq symlinks in (`String`)

   returns:
     `Map` with keys:
       `:library-id`   Library ID (`String`)
       `:r1-src-path`  Path to R1 fastq file (`String`)
       `:r1-dest-path` Path to R1 fastq file (`String`)
       `:r2-src-path`  Path to R2 fastq file (`String`)
       `:r2-dest-path` Path to R2 fastq file (`String`)
  "
  [library-fastq-paths fastq-symlinks-dir]
  (let [{:keys [library-id r1-path r2-path]} library-fastq-paths
        fastq-symlinks-subdir (fastq-symlinks-subdir-by-year library-fastq-paths)
        r1-src-path  r1-path
        r1-dest-path (str (io/file fastq-symlinks-dir fastq-symlinks-subdir (str library-id "_R1.fastq.gz")))
        r2-src-path  r2-path
        r2-dest-path (str (io/file fastq-symlinks-dir fastq-symlinks-subdir (str library-id "_R2.fastq.gz")))]
    {:library-id   library-id
     :r1-src-path  r1-src-path
     :r1-dest-path r1-dest-path
     :r2-src-path  r2-src-path
     :r2-dest-path r2-dest-path}))


(defn symlink!
  "Create symlinks for each library in `fastq-paths`, under `fastq-symlinks-dir`.

   takes:
     `fastq-paths`: `Map` with keys:
       `:library-id`   Library ID (`String`)
       `:r1-src-path`  Path to source R1 fastq file (`String`)
       `:r1-dest-path` Path to destination R1 symlink (`String`)
       `:r2-src-path`  Path to source R2 fastq file (`String`)
       `:r2-dest-path` Path to destination R2 symlink (`String`)
  
      `libraries-to-analyze-chan`: (`Channel`)

   returns:
     `nil`
  "
  [fastq-paths libraries-to-analyze-chan db]
  (let [{:keys [library-id r1-src-path r1-dest-path r2-src-path r2-dest-path]} fastq-paths
        r1-symlink-exists (.exists (io/file r1-dest-path))
        r2-symlink-exists (.exists (io/file r1-dest-path))
        both-symlinks-exist (and r1-symlink-exists r2-symlink-exists)]
    (when (and (not both-symlinks-exist) (not (contains? (:excluded-library-ids @db) library-id)))
      (do
        (symlink-one! r1-src-path r1-dest-path)
        (symlink-one! r2-src-path r2-dest-path)
        (let [library-to-analyze {:library-id library-id
                                  :analysis-stage :assembly
                                  :r1-path r1-dest-path
                                  :r2-path r2-dest-path}]
          (put! libraries-to-analyze-chan library-to-analyze)
          (log/info "Put on analysis channel: " library-to-analyze))))))


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
      (let [_ (update-excluded-runs! db)
            run (first (scan-for-runs-to-symlink! db))]
        (when-some [r run]
          (do
            (log/info "Found directory to symlink: " run)
            (>! runs-to-symlink-chan r))))
      (let [symlinks-scanning-interval (get-in @db [:config :symlinking-scanning-interval-ms] 10000)
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
  [runs-to-symlink-chan libraries-to-analyze-chan db]
  (go-loop [run (<! runs-to-symlink-chan)]
    (log/info (str "Took from symlink channel: " run))
    (when-some [r run]
      (let [symlinks-dir (get-in @db [:config :fastq-symlinks-dir])
            project-id   (get-in @db [:config :samplesheet-project-id])]
        (->> r
             (get-fastq-paths! project-id)
             (map #(add-symlink-dest-path % symlinks-dir))
             (#(doseq [library %] (symlink! library libraries-to-analyze-chan db))))))

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
      (let [_ (update-excluded-runs! db)
            run (first (scan-for-runs-to-analyze! db))]
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


(defn run-routine-assembly!
  "Run the BCCDC-PHL/routine-assembly pipeline on a run directory.
   When the analysis completes, delete the 'work' directory.

   takes:
     `db`: Global app-state db (`Atom`)

   returns:
     Path to analysis output directory. (`String`)
  "
  [libraries db]
  (let [samplesheet            ""
        assembly-output-dir    (get-in @db [:config :assembly-output-dir])
        pipeline-full-name     "BCCDC-PHL/routine-assembly"
        pipeline-short-name    (second (str/split pipeline-full-name #"/"))
        pipeline-full-version  (get-in @db [:config :routine-assembly-config :version])
        pipeline-minor-version (str/join "." (take 2 (str/split pipeline-full-version #"\.")))
        work-dir               (str (io/file assembly-output-dir (str "work-" pipeline-short-name "-" (java.util.UUID/randomUUID))))
        outdir                 (str (io/file assembly-output-dir))
        log-file               (str (io/file outdir "nextflow.log"))
        assembly-tool          (get-in @db [:config :routine-assembly-config :assembly-tool])
        annotation-tool        (get-in @db [:config :routine-assembly-config :annotation-tool])]
    (do
      (swap! db assoc :currently-analyzing libraries)
      (log/info (str "Started " pipeline-short-name " analysis."))
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
                   "--samplesheet_input" samplesheet
                   (str "--" assembly-tool)
                   (str "--" annotation-tool)
                   "-work-dir" work-dir
                   "--outdir" "."]))
      
      (sh "find" outdir "-type" "d" "-exec" "chmod" "750" "{}" "+")
      (sh "find" outdir "-type" "f" "-exec" "chmod" "640" "{}" "+")
      (log/info (str "Finished " pipeline-short-name " analysis."))
      (go (sh "rm" "-r" work-dir)))
    outdir))


(defn run-mlst-nf!
  "Run the BCCDC-PHL/mlst-nf pipeline on an artic analysis directory.
   When the analysis completes, delete the 'work' directory.

   takes:
     `assembly-dir`: Path to artic analysis output directory. (`String`)
     `db`: Global app-state db (`Atom`)
   returns:
     `nil`
  "
  [assemblies db]
  (let [pipeline-full-name      "BCCDC-PHL/mlst-nf"
        pipeline-short-name     (second (str/split pipeline-full-name #"/"))
        pipeline-full-version   (get-in @db [:config :mlst-nf-config :version])
        pipeline-minor-version  (str/join "." (take 2 (str/split pipeline-full-version #"\.")))
        work-dir                (str (io/file (str "work-" pipeline-short-name "-" (java.util.UUID/randomUUID))))
        outdir                  (get-in @db [:config :analysis-output-dir])
        log-file                (str (io/file outdir "nextflow.log"))]
    (do
      (log/info (str "Started " pipeline-short-name " analysis."))
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
                   ;; TODO
                   "-work-dir" work-dir
                   "--outdir" "."]))

      (sh "find" outdir "-type" "d" "-exec" "chmod" "750" "{}" "+")
      (sh "find" outdir "-type" "f" "-exec" "chmod" "640" "{}" "+")
      #_(swap! db assoc :currently-analyzing nil)
      (log/info (str "Finished " pipeline-short-name " analysis."))
      (go (sh "rm" "-r" work-dir)))))


(defn analyze!
  "Run the two nextflow analysis pipelines on `run`.

   takes:
     `libraries`: _
     `db`: Global app-state db. (`Atom`)

   returns:
    `nil`
  "
  [libraries db]
  (-> (run-routine-assembly! libraries db)
      (run-mlst-nf! db)))


(defn start-analyzer!
  "Starts the (asynchronous) process of analyzing any directories put on `runs-to-analyze-chan`.

   takes:
     `runs-to-analyse-chan`: Channel to take run directories from for analysis (`Channel`)
     `db`: Global app-state db (`Atom`)

   returns:
     (`Channel`)
  "
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
          socket-dir (str "/tmp/auto-cpo-" (System/getProperty "user.name"))]
      (do
        (sh "mkdir" "-p" socket-dir)
        (sh "chmod" "700" socket-dir)
        (defonce server (start-server :socket (str socket-dir "/auto-cpo-" uuid ".sock"))))))

  ;;
  ;; Load list of excluded runs from all exclude files
  ;; Store set of run IDs under :excluded-run-ids in db
  ;; Updates asynchronously every 10 seconds
  (go-loop []
    (let [exclude-files-reload-interval (get-in opts [:config :exclude-files-reload-interval-ms] 60000)]
      (update-excluded-runs! db)
      (update-excluded-libraries! db)
      (<! (timeout exclude-files-reload-interval))
      (recur)))

  
  (defonce runs-to-symlink-chan (chan))
  (defonce kill-symlinking-chan (chan))

  (defonce libraries-to-analyze-chan (chan))
  (defonce kill-analysis-chan (chan))

  (start-symlinker! runs-to-symlink-chan libraries-to-analyze-chan db)
  (start-scanning-for-runs-to-symlink! runs-to-symlink-chan kill-symlinking-chan db)

  #_(start-analyzer! runs-to-analyze-chan db)
  #_(start-scanning-for-runs-to-analyze! runs-to-analyze-chan kill-analysis-chan db)

  ;;
  ;; Main loop
  (loop []
    (recur)))


(comment
  ;;
  ;; Useful forms for REPL-driven development

  (def opts {:options {:config "dev-config.edn"}})

  ;; create app db
  (defonce db (atom {}))

  ;; Reload config
  (update-config! (get-in opts [:options :config]) db)

  (update-excluded-runs! db)
  (update-excluded-libraries! db)

  ;; Clear excluded run list
  (swap! db (fn [x] (assoc x :excluded-run-ids #{})))


  (defonce runs-to-symlink-chan (chan))
  (defonce kill-symlinking-chan (chan))

  (defonce libraries-to-analyze-chan (chan))
  (defonce kill-analysis-chan (chan))

  ;; Control symlinking process
  (start-symlinker! runs-to-symlink-chan libraries-to-analyze-chan db)
  (start-scanning-for-runs-to-symlink! runs-to-symlink-chan kill-symlinking-chan db)
  (put-stop! kill-symlinking-chan)
 
  
  (start-analyzer! runs-to-analyze-chan db)
  (start-scanning-for-runs-to-analyze! runs-to-analyze-chan kill-analysis-chan db)
  (put-stop! kill-analysis-chan)
  
  (defn mock-analyze!
    "Mock analysis by sleeping for 10s"
    [run-dir db]
    (do
      (swap! db assoc :currently-analyzing run-dir)
      (go (<! (timeout 10000))
          (swap! db assoc :currently-analyzing nil))))
  
  )
