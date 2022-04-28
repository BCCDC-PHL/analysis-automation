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
            [clojure.data.csv :as csv]
            [nrepl.server :refer [start-server stop-server]]
            [auto-cpo.cli :as cli]
            [auto-cpo.samplesheet :as samplesheet])
  (:import [java.time.format DateTimeFormatter]
           [java.time ZonedDateTime]
           [java.io File])
  (:gen-class))


;;
;; Helpers
(def spy #(do (log/info (str "spy: " %)) %))

(defn update-vals
  "Simplified version of update-vals, taken from:
   https://github.com/clojure/clojure/blob/2b3ba822815981e7f76907a3b75e9ecc428f7656/src/clj/clojure/core.clj#L8008-L8022
  "
  [m f]
  (reduce-kv (fn [acc k v] (assoc acc k (f v))) {} m))


(defn timed-take!
  "
  "
  ([batch-size timeout-time-ms ch]
   (timed-take! batch-size timeout-time-ms ch nil))
  ([batch-size timeout-time-ms ch buf-or-n]
   (let [out (chan buf-or-n)]
     (go (loop [x 0]
           (when (< x batch-size)
             (let [[v _] (async/alts! [ch (timeout timeout-time-ms)])]
               (when (not (nil? v))
                 (>! out v)
                 (recur (inc x))))))
         (async/close! out))
     out)))


(defn batch
  ""
  [in batch-size timeout-time-ms]
  (let [out (chan)]
    (go-loop []
      (let [v (<! (->> (timed-take! batch-size timeout-time-ms in)
                       (async/reduce conj nil)))]
        (when (not (nil? v))
          (>! out v))
        (recur)))
    out))


(defn now!
  "Generate an ISO-8601 timestamp (`YYYY-MM-DDTHH:mm:ss.xxxx-OFFSET`).
   eg: `2022-02-11T14:13:49.9335-08:00`
  
   takes:
   returns:
     ISO-8601 timestamp for the current time (`String`)
  "
  []
  (.. (ZonedDateTime/now) (format DateTimeFormatter/ISO_OFFSET_DATE_TIME)))


(defn now-by-second-only-digits!
  "Generate a timestamp at second resolution with format (`YYYYMMDDHHmmss`).
   Excludes any special characters like `-` and `:`, so useful for filenames.
   eg: `20220211141349`
  
   takes:
   returns:
     Timestamp for the current time at second resolution, including only digits (`String`)"
  []
  (apply str (filter #(Character/isDigit %) (first (str/split (now!) #"\.")))))


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


(defn maps->csv-data 
  "Takes a collection of maps and returns csv-data 
   (vector of vectors with all values)."       
   [maps]
   (let [columns (-> maps first keys)
         headers (mapv name columns)
         rows (mapv #(mapv % columns) maps)]
     (into [headers] rows)))


;;
;; Config management
(defn update-config!
  "Read config from file and insert into `db` under key `:config`.

   takes:
     `config-file-path`: Path to config file (`String`)
     `db`: Global app-state db (`Atom`)

   returns:
     `nil`
  "
  [config-file-path db]
  (when (.exists (io/file config-file-path))
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
         (when (.exists (io/file exclude-file-path))
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
         (when (.exists (io/file exclude-file-path))
           (with-open [rdr (io/reader exclude-file-path)]
             (swap! db (fn [x] (assoc x :excluded-library-ids (set/union (:excluded-library-ids x) (into #{} (line-seq rdr)))))))))))))


;;
;;
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
     `db`: Global app-state db (`Atom`)

   returns:
     Filtered sequence of paths to illumina sequencer output dirs. (`[String]`)
  "
  [paths db]
  (let [symlinks-dir              (get-in @db [:config :fastq-symlinks-dir])
        excluded-run-ids          (get @db :excluded-run-ids)
        previously-symlinked-runs (get @db :symlinked-runs)]
      (->> paths
       (filter #(.isDirectory (io/file %)))
       (filter matches-run-directory-regex?)
       (filter upload-complete?)
       (filter #(not (contains? excluded-run-ids (.getName (io/file %)))))
       (filter #(not (contains? previously-symlinked-runs (.getName (io/file %))))))))


(defn scan-for-runs-to-symlink!
  "Scan through multiple run directories for runs to symlink.

   takes:
     `db`: Global app-state db (Atom)

   returns:
     Sequence of illumina run directory paths available for symlinking ([String])
  "
  [db]
  (let [run-dirs (get-in @db [:config :run-dirs])]
    (-> (map scan-directory! run-dirs)
        flatten
        (filter-for-runs-to-symlink db))))


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
    (cond (= sequencer-type :miseq)
          (samplesheet/parse-project-library-ids-miseq samplesheet-lines project-id)
          (= sequencer-type :nextseq)
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
  (let [project-id          (get-in @db [:config :samplesheet-project-id])
        samplesheet-path    (find-samplesheet! run-dir)
        project-library-ids (get-project-library-ids-from-samplesheet! samplesheet-path project-id)]
    (map #(find-fastqs-by-library-id run-dir %) project-library-ids)))



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
      (when (not (.exists dest-parent-dir))
        (io/make-parents (str dest-path)))
      (java.nio.file.Files/deleteIfExists dest-path)
      (java.nio.file.Files/createSymbolicLink dest-path src-path (into-array java.nio.file.attribute.FileAttribute [])))
                                            
    (catch java.lang.NullPointerException e)
    (catch java.io.IOException e)
    (catch java.nio.file.FileAlreadyExistsException e)))


(defn get-fastq-paths!
  "Get paths for any libraries in `run-dir` that are labeled with `project-id` in the run's SampleSheet.csv file. 

   takes:
     `project-id`: Identifier used to mark libraries as belonging to this project in the SampleSheet file. (`String`)
     `run-dir`: Path to illumina sequencer output directory to create symlinks for. (`String`)
     

   returns:
     Sequence of maps, each with keys:
       `:id`      Library ID (`String`)
       `:r1-path` Path to R1 fastq file (`String`)
       `:r2-path` Path to R2 fastq file (`String`)
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
                      {:id   library-id
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
         `:id` Library ID (`String`)
         `:r1-path`    Path to R1 fastq file (`String`)
         `:r2-path`    Path to R2 fastq file (`String`)
      `fastq-symlinks-dir`: Path to directory to create fastq symlinks in (`String`)

   returns:
     Sub-directory name (`String`)
  "
  [library-fastq-paths]
  (let [library-id (:id library-fastq-paths)
        current-year (first (str/split (now!) #"-"))
        current-two-digit-year (apply str (drop 2 current-year))
        library-year-regex #"BC(\d{2})[A-Z]"
        library-two-digit-year-matches (re-find library-year-regex library-id)]
    (if (> (count library-two-digit-year-matches) 1)
      (second library-two-digit-year-matches)
      current-two-digit-year)))


(defn library-id->year
  "Extract the year from a library ID. Assumes that `library-id` starts with \"BC\", followed by a
   two-digit year, followed by a letter A-Z.
   takes:
     `library-id`: Library ID (`String`)

   returns:
     Two-digit year (`String`)

   eg: (library-id->year \"BC21A001A\") => \"21\"
       (library-id->year \"BC17B135A\") => \"17\"
       (library-id->year \"A\") => nil
  "
  [library-id]
  (let [library-year-regex #"BC(\d{2})[A-Z]"
        library-two-digit-year-matches (re-find library-year-regex library-id)]
    (when (> (count library-two-digit-year-matches) 1)
      (second library-two-digit-year-matches))))


(defn group-samples-by-year
  "Given a collection of samples, identify the year of the sample based on the sample ID, and group
   samples together by year.
   takes:
     `samples`: collection of samples. Each sample must include an `:id` key.
  
   returns:
     Map of `String` to vector, with keys that are two-digit years and values that are collections
     of samples. 
  "
  [samples]
  (group-by #(library-id->year (:id %)) samples))


(defn add-symlink-dest-path
  "Given a map of fastq file paths, add paths to symlink destinations.

   takes:
     `library-fastq-paths`:
       `Map` with keys:
         `:id`      Library ID (`String`)
         `:r1-path` Path to R1 fastq file (`String`)
         `:r2-path` Path to R2 fastq file (`String`)
      `fastq-symlinks-dir`: Path to directory to create fastq symlinks in (`String`)

   returns:
     `Map` with keys:
       `:id`           Library ID (`String`)
       `:r1-src-path`  Path to R1 fastq file (`String`)
       `:r1-dest-path` Path to R1 fastq file (`String`)
       `:r2-src-path`  Path to R2 fastq file (`String`)
       `:r2-dest-path` Path to R2 fastq file (`String`)
  "
  [library-fastq-paths fastq-symlinks-dir]
  (let [{:keys [r1-path r2-path]} library-fastq-paths
        library-id (:id library-fastq-paths)
        library-id-year (library-id->year library-id)
        fastq-symlinks-subdir (if (nil? library-id-year) (apply str (drop 2 (first (str/split (now!) #"-")))) library-id-year)
        r1-src-path  r1-path
        r1-dest-path (str (io/file fastq-symlinks-dir fastq-symlinks-subdir (str library-id "_R1.fastq.gz")))
        r2-src-path  r2-path
        r2-dest-path (str (io/file fastq-symlinks-dir fastq-symlinks-subdir (str library-id "_R2.fastq.gz")))]
    {:id           library-id
     :r1-src-path  r1-src-path
     :r1-dest-path r1-dest-path
     :r2-src-path  r2-src-path
     :r2-dest-path r2-dest-path}))


(defn both-symlinks-exist?
  "Check if symlinks for a library already exist.

   takes:
     `fastq-paths`: `Map` with keys:
       `:r1-dest-path` Path to destination R1 symlink (`String`)
       `:r2-dest-path` Path to destination R2 symlink (`String`)

   returns:
     `bool`, indicating whether or not both fastq symlinks already exist for that sample.
  "
  [fastq-paths]
  (let [{:keys [r1-dest-path r2-dest-path]} fastq-paths
        r1-symlink-exists (.exists (io/file r1-dest-path))
        r2-symlink-exists (.exists (io/file r2-dest-path))]
    (and r1-symlink-exists r2-symlink-exists)))


(defn library-excluded?
  "Check if the library is excluded from analysis, based on its ID
   takes:
     `library-id`: Identifier to check against list of excluded libraries (`String`)
     `db`: Global app-state db
  "
  [library-id db]
  (contains? (:excluded-library-ids @db) library-id))


(defn symlink!
  "Create symlinks for the library in `fastq-paths`, under the directory associated with the key `:fastq-symlinks-dir` in `db`.

   takes:
     `fastq-paths`: `Map` with keys:
       `:id`   Library ID (`String`)
       `:r1-src-path`  Path to source R1 fastq file (`String`)
       `:r1-dest-path` Path to destination R1 symlink (`String`)
       `:r2-src-path`  Path to source R2 fastq file (`String`)
       `:r2-dest-path` Path to destination R2 symlink (`String`)

   returns:
     `Map` with keys:
       `:id`     Library ID
       `:r1-path`        Path to R1 fastq file (`String`)
       `:r2-path`        Path to R2 fastq file (`String`)
  "
  [message-chan fastq-paths]
  (let [library-id (:id fastq-paths)
        {:keys [r1-src-path r1-dest-path r2-src-path r2-dest-path]} fastq-paths]
    (go 
      (symlink-one! r1-src-path r1-dest-path)
      (symlink-one! r2-src-path r2-dest-path)
      (let [message {:event :symlinks-created
                     :timestamp (now!)
                     :id library-id
                     :library-id library-id
                     :symlink-paths {:r1 r1-dest-path
                                     :r2 r2-dest-path}}]
        (>! message-chan {:topic :symlinking
                          :message message})
        (>! message-chan {:topic :analysis
                          :message message})
        (>! message-chan {:topic :logging
                          :message {:log/level :info
                                    :log/message message}})))))


(defn start-scanning-for-runs-to-symlink!
  "Starts the (asynchronous) process of scanning for runs to symlink.

   takes:
     `message-chan`: Channel to put run directories on for symlinking. (`Channel`)
     `kill-chan`: Channel for killing the scanning process. (`Channel`)
     `db`: Global app-state db (`Atom`)

   returns:
     (`Channel`)
  "
  [message-chan kill-chan db]
  (go-loop []
    (let [_ (update-excluded-runs! db)
          run (first (scan-for-runs-to-symlink! db))]
      (when-some [r run]
        (let [message {:timestamp (now!)
                       :event :run-directory-found
                       :run-dir r}]
          (>! message-chan {:topic :symlinking
                            :message message})
          (>! message-chan {:topic :logging
                            :message {:log/level :info
                                      :log/message message}}))))
    (let [symlinks-scanning-interval (get-in @db [:config :symlinking-scanning-interval-ms] 2000)
          [v ch] (async/alts! [(timeout symlinks-scanning-interval) kill-chan])]
      (if (= ch kill-chan)
        (log/info "Stopped scanning for runs to symlink.")
        (recur)))))


(defn mark-as-symlinked
  "Add the run ID to `db` under the key `:symlinked-runs`.

   takes:
     `db`: Global app-state db (`Atom`)
     `run-path` Path to illumina output directory for the run (`String`)

   returns:
     `Map`, the updated state of the global app-state db.
  "
  [db run-path]
  (let [run-id (.getName (io/file run-path))]
    (if (not (contains? @db :symlinked-runs))
      (swap! db assoc :symlinked-runs #{run-id})
      (swap! db update :symlinked-runs conj run-id))))


(defn start-symlinker!
  "Starts the (asynchronous) process of symlinking any directories put on `symlink-chan`.

   takes:
     `message-chan`:
     `symlink-chan`: 
     `db`: Global app-state db (`Atom`)

   returns:
     (`Channel`)
  "
  [message-chan symlink-chan db]
  (go-loop [took (<! symlink-chan)]
    (when-some [message (if (= :run-directory-found (get-in took [:message :event])) (:message took) nil)]
      (let [symlinks-dir (get-in @db [:config :fastq-symlinks-dir])
            project-id   (get-in @db [:config :samplesheet-project-id])
            run-dir      (io/file (get-in message [:run-dir]))]
        (->> (str run-dir)
             (get-fastq-paths! project-id)
             (map #(add-symlink-dest-path % symlinks-dir))
             (filter #(not (library-excluded? (:id %) db)))
             (filter #(not (both-symlinks-exist? %)))
             (map (partial symlink! message-chan))
             (doall))
        (mark-as-symlinked db run-dir)
        ))
      (recur (<! symlink-chan))))


(defn put-stop!
  "Stop a process by putting a value on the `kill-chan`.

   takes:
     `kill-chan`: Channel to put a value on to kill an async process. (`Channel`)

    returns:
     `boolean`
  "
  [kill-chan]
  (put! kill-chan :stop))


(defn find-files!
  "Find files below `dir`, matching filename-glob

   takes:
     `dir`: Directory to search under. (`String`)
     `filename-glob`: Filename to search for, possibly including glob characters like `*`. (`String`)

   returns:
     Seq of paths to files matching `filename-glob`. (`[String]`)
  "
  [dir filename-glob]
  (-> (apply sh ["find" dir "-type" "f" "-name" filename-glob])
      :out
      (str/split #"\n")))


(defn run-taxon-abundance!
  "Run the BCCDC-PHL/taxon-abundance pipeline on a set of libraries.
   When the analysis completes, delete the 'work' directory.

   takes:
     `libraries`: Sequence of maps, each with keys:
       `:id`: Library ID (`String`)
       `:r1-path`: Path to R1 fastq file (`String`)
       `:r2-path`: Path to R2 fastq file (`String`)
     `output-subdir`: Directory name for output sub-directory (below main output dir from config)
     `db`: Global app-state db (`Atom`)
     `message-chan`: Channel to put values on for next analyses (`Channel`)

   returns:
     
  "
  [libraries output-subdir db message-chan]
  (let [output-dir             (io/file (get-in @db [:config :analysis-output-dir]) output-subdir)
        pipeline-full-name     "BCCDC-PHL/taxon-abundance"
        pipeline-short-name    (second (str/split pipeline-full-name #"/"))
        pipeline-full-version  (get-in @db [:config :taxon-abundance-config :version])
        pipeline-minor-version (str/join "." (take 2 (str/split pipeline-full-version #"\.")))
        analysis-uuid          (java.util.UUID/randomUUID)
        work-dir               (str (io/file output-dir (str "work-" pipeline-short-name "-" analysis-uuid)))
        samplesheet-path       (str (File/createTempFile (str pipeline-short-name "-input-" analysis-uuid) ".csv"))
        samplesheet-data       (maps->csv-data (map #(set/rename-keys (select-keys % [:id :r1-path :r2-path]) {:id :ID :r1-path :R1 :r2-path :R2}) libraries))
        outdir                 (str output-dir)
        log-file               (str (io/file (get-in @db [:config :nextflow-logs-dir]) (str (now-by-second-only-digits!) "-" pipeline-short-name "-nextflow.log")))
        kraken-db-path         (get-in @db [:config :taxon-abundance-config :kraken-db])
        bracken-db-path        (get-in @db [:config :taxon-abundance-config :bracken-db])]
    (do
      
      (doseq [library-id (map :id libraries)]
        (let [message {:event :analysis-started
                       :timestamp (now!)
                       :pipeline-name pipeline-full-name
                       :id library-id
                       :library-id library-id}]
          (go
            (>! message-chan {:topic :analysis
                              :message message})
            (>! message-chan {:topic :logging
                              :message {:log/level :info
                                        :log/message message}}))))

      (sh "mkdir" "-p" outdir)
      (sh "chmod" "750" outdir)
      (with-open [writer (io/writer samplesheet-path)]
        (csv/write-csv writer samplesheet-data))
      (sh "mkdir" "-p" work-dir)
      (sh "chmod" "750" work-dir)

      (shell/with-sh-dir outdir
        (apply sh ["nextflow"
                   "-log" log-file
                   "run" pipeline-full-name
                   "-profile" "conda"
                   "--cache" (str (io/file (System/getProperty "user.home") ".conda/envs"))
                   "-r" pipeline-full-version
                   "--samplesheet_input" samplesheet-path
                   "--kraken_db" kraken-db-path
                   "--bracken_db" bracken-db-path
                   "-work-dir" work-dir
                   "--versioned_outdir"
                   "--outdir" "."]))

      (sh "find" outdir "-type" "d" "-exec" "chmod" "750" "{}" "+")
      (sh "find" outdir "-type" "f" "-exec" "chmod" "640" "{}" "+")

      (sh "rm" "-r" work-dir)
      (sh "rm" samplesheet-path)

      (doseq [library-id (map :id libraries)]
        (let [library-analysis-outdir (io/file outdir library-id (str/join "-" [pipeline-short-name pipeline-minor-version "output"]))
              message {:event :analysis-completed
                       :timestamp (now!)
                       :pipeline-name pipeline-full-name
                       :pipeline-version pipeline-full-version
                       :id library-id
                       :library-id library-id
                       :outdir (str library-analysis-outdir)}]
          (spit (io/file library-analysis-outdir "analysis_complete.json")
                {:timestamp (now!)})
          (go (>! message-chan {:topic :analysis
                                :message message})
              (>! message-chan {:topic :logging
                                :message {:log/level :info
                                          :log/message message}})))))))


(defn run-routine-assembly!
  "Run the BCCDC-PHL/routine-assembly pipeline on a run directory.
   When the analysis completes, delete the 'work' directory.

   takes:
     `libraries`: Sequence of maps, each with keys:
       `:id`: Library ID (`String`)
       `:r1-path`: Path to R1 fastq file (`String`)
       `:r2-path`: Path to R2 fastq file (`String`)
     `output-subdir`: Directory name for output sub-directory (below main output dir from config)
     `db`: Global app-state db (`Atom`)
     `message-chan`: Channel to put values on for next analyses (`Channel`)

   returns:
     
  "
  [libraries output-subdir db message-chan]
  (let [output-dir             (io/file (get-in @db [:config :analysis-output-dir]) output-subdir)
        pipeline-full-name     "BCCDC-PHL/routine-assembly"
        pipeline-short-name    (second (str/split pipeline-full-name #"/"))
        pipeline-full-version  (get-in @db [:config :routine-assembly-config :version])
        pipeline-minor-version (str/join "." (take 2 (str/split pipeline-full-version #"\.")))
        analysis-uuid          (java.util.UUID/randomUUID)
        work-dir               (str (io/file output-dir (str "work-" pipeline-short-name "-" analysis-uuid)))
        samplesheet-path       (str (File/createTempFile (str pipeline-short-name "-input-" analysis-uuid) ".csv"))
        samplesheet-data       (maps->csv-data (map #(set/rename-keys (select-keys % [:id :r1-path :r2-path]) {:id :ID :r1-path :R1 :r2-path :R2}) libraries))
        outdir                 (str output-dir)
        log-file               (str (io/file (get-in @db [:config :nextflow-logs-dir]) (str (now-by-second-only-digits!) "-" pipeline-short-name "-nextflow.log")))
        assembly-tool          (get-in @db [:config :routine-assembly-config :assembly-tool])
        annotation-tool        (get-in @db [:config :routine-assembly-config :annotation-tool])]

    (doseq [library-id (map :id libraries)]
      (let [message {:event :analysis-started
                     :timestamp (now!)
                     :pipeline-name pipeline-full-name
                     :id library-id
                     :library-id library-id}]
        (go
          (>! message-chan {:topic :analysis
                            :message message})
          (>! message-chan {:topic :logging
                            :message {:log/level :info
                                      :log/message message}}))))
    
    (sh "mkdir" "-p" outdir)
    (sh "chmod" "750" outdir)
    (with-open [writer (io/writer samplesheet-path)]
      (csv/write-csv writer samplesheet-data))
    (sh "mkdir" "-p" work-dir)
    (sh "chmod" "750" work-dir)

    (shell/with-sh-dir outdir
      (apply sh ["nextflow"
                 "-log" log-file
                 "run" pipeline-full-name
                 "-profile" "conda"
                 "--cache" (str (io/file (System/getProperty "user.home") ".conda/envs"))
                 "-r" pipeline-full-version
                 "--samplesheet_input" samplesheet-path
                 (str "--" assembly-tool)
                 (str "--" annotation-tool)
                 "-work-dir" work-dir
                 "--versioned_outdir"
                 "--outdir" "."]))

    (doseq [library libraries]
      (let [library-id (:id library)
            r1-path (:r1-path library)
            r2-path (:r2-path library)
            library-analysis-outdir (io/file outdir library-id (str/join "-" [pipeline-short-name pipeline-minor-version "output"]))
            now (now!)
            analysis-completed-message {:event :analysis-completed
                                        :timestamp now
                                        :pipeline-name pipeline-full-name
                                        :pipeline-version pipeline-full-version
                                        :id library-id
                                        :library-id library-id
                                        :outdir (str library-analysis-outdir)}
            assembly-completed-message {:event :assembly-completed
                                        :timestamp now
                                        :assembly-tool assembly-tool
                                        :annotation-tool annotation-tool
                                        :id library-id
                                        :library-id library-id
                                        :r1-path r1-path
                                        :r2-path r2-path
                                        :assembly-path (str (io/file library-analysis-outdir (str library-id "_" assembly-tool ".fa")))}]
        (spit (io/file library-analysis-outdir "analysis_complete.json")
              {:timestamp (now!)})
        (go (>! message-chan {:topic :analysis
                              :message assembly-completed-message})
            (>! message-chan {:topic :logging
                              :message {:log/level :info
                                        :log/message analysis-completed-message}})
            (>! message-chan {:topic :logging
                              :message {:log/level :info
                                        :log/message assembly-completed-message}}))))
      
    (sh "find" outdir "-type" "d" "-exec" "chmod" "750" "{}" "+")
    (sh "find" outdir "-type" "f" "-exec" "chmod" "640" "{}" "+")

    (sh "rm" "-r" work-dir)
    (sh "rm" samplesheet-path)))


(defn run-mlst-nf!
  "Run the BCCDC-PHL/mlst-nf pipeline on a collection of assemblies.
   When the analysis completes, delete the 'work' directory.

   takes:
     `assemblies`: Sequence of maps, each with keys:
       `:id`:            Library ID (`String`)
       `:assembly-path`: Path to assembly fasta file (`String`)
     `output-subdir`: Directory name for output sub-directory (below main output dir from config) (`String`)
     `db`: Global app-state db (`Atom`)
     `message-chan`: Channel to put downstream analysis inputs into
   returns:
     `nil`
  "
  [assemblies output-subdir db message-chan]
  (let [pipeline-full-name     "BCCDC-PHL/mlst-nf"
        pipeline-short-name    (second (str/split pipeline-full-name #"/"))
        pipeline-full-version  (get-in @db [:config :mlst-nf-config :version])
        pipeline-minor-version (str/join "." (take 2 (str/split pipeline-full-version #"\.")))
        analysis-uuid          (java.util.UUID/randomUUID)
        work-dir               (str (io/file (str "work-" pipeline-short-name "-" analysis-uuid)))
        samplesheet-path       (str (File/createTempFile (str pipeline-short-name "-input-" analysis-uuid) ".csv"))
        samplesheet-data       (maps->csv-data (map #(set/rename-keys (select-keys % [:id :assembly-path]) {:id :ID :assembly-path :ASSEMBLY}) assemblies))
        outdir                 (str (io/file (get-in @db [:config :analysis-output-dir]) output-subdir))
        log-file               (str (io/file (get-in @db [:config :nextflow-logs-dir]) (str (now-by-second-only-digits!) "-" pipeline-short-name "-nextflow.log")))
        ]

    (doseq [library-id (map :id assemblies)]
      (let [message {:event :analysis-started
                     :timestamp (now!)
                     :pipeline-name pipeline-full-name
                     :id library-id
                     :library-id library-id}]
        (go
          (>! message-chan {:topic :analysis
                            :message message})
          (>! message-chan {:topic :logging
                            :message {:log/level :info
                                      :log/message message}}))))

    (sh "mkdir" "-p" outdir)
    (sh "chmod" "750" outdir)
    (with-open [writer (io/writer samplesheet-path)]
      (csv/write-csv writer samplesheet-data))
    (sh "mkdir" "-p" work-dir)
    (sh "chmod" "750" work-dir)

    (shell/with-sh-dir outdir
      (apply sh ["nextflow"
                 "-log" log-file
                   "run" pipeline-full-name
                 "-profile" "conda"
                 "--cache" (str (io/file (System/getProperty "user.home") ".conda/envs"))
                 "-r" pipeline-full-version
                 "--samplesheet_input" samplesheet-path
                 "-work-dir" work-dir
                 "--versioned_outdir"
                 "--outdir" "."]))

    (doseq [assembly assemblies]
      (let [library-id (:library-id assembly)
            library-analysis-outdir (io/file outdir library-id (str/join "-" [pipeline-short-name pipeline-minor-version "output"]))
            now (now!)
            analysis-completed-message {:event :analysis-completed
                                        :timestamp now
                                        :pipeline-name pipeline-full-name
                                        :pipeline-version pipeline-full-version
                                        :id library-id
                                        :library-id library-id
                                        :outdir (str library-analysis-outdir)}
            mlst-completed-message {:event :mlst-completed
                                        :timestamp now
                                        :id library-id
                                        :library-id library-id
                                        :mlst-sequence-type-path (str (io/file library-analysis-outdir (str library-id "_sequence_type.csv")))}]
        (spit (io/file library-analysis-outdir "analysis_complete.json")
              {:timestamp (now!)})
        (go (>! message-chan {:topic :analysis
                              :message mlst-completed-message})
            (>! message-chan {:topic :logging
                              :message {:log/level :info
                                        :log/message analysis-completed-message}})
            ;; remove this one after debugging
            (>! message-chan {:topic :logging
                              :message {:log/level :info
                                        :log/message mlst-completed-message}}))))
 
    (sh "find" outdir "-type" "d" "-exec" "chmod" "750" "{}" "+")
    (sh "find" outdir "-type" "f" "-exec" "chmod" "640" "{}" "+")

    (sh "rm" "-r" work-dir)
    (sh "rm" samplesheet-path)))


(defn run-plasmid-screen!
  "Run the BCCDC-PHL/plasmid-screen pipeline on a collection of assemblies.
   When the analysis completes, delete the 'work' directory.

   takes:
     `assemblies`: Sequence of maps, each with keys:
       `:id`: Library ID (`String`)
       `:assembly-path`: Path to assembly fasta file (`String`)
       `:r1-path`: Path to R1 fastq file (`String`)
       `:r2-path`: Path to R2 fastq file (`String`)
     `output-subdir`: Directory name for output sub-directory (below main output dir from config) (`String`)
     `db`: Global app-state db (`Atom`)
     `message-chan`:
   returns:
     `nil`
  "
  [assemblies output-subdir db message-chan]
  (let [pipeline-full-name     "BCCDC-PHL/plasmid-screen"
        pipeline-short-name    (second (str/split pipeline-full-name #"/"))
        pipeline-full-version  (get-in @db [:config :plasmid-screen-config :version])
        pipeline-minor-version (str/join "." (take 2 (str/split pipeline-full-version #"\.")))
        mob-suite-db           (get-in @db [:config :plasmid-screen-config :mob-suite-db])
        analysis-uuid          (java.util.UUID/randomUUID)
        work-dir               (str (io/file (str "work-" pipeline-short-name "-" analysis-uuid)))
        samplesheet-path       (str (File/createTempFile (str pipeline-short-name "-input-" analysis-uuid) ".csv"))
        samplesheet-data       (maps->csv-data (map #(set/rename-keys (select-keys % [:id :assembly-path :r1-path :r2-path]) {:id :ID :r1-path :R1 :r2-path :R2 :assembly-path :ASSEMBLY}) assemblies))
        outdir                 (str (io/file (get-in @db [:config :analysis-output-dir]) output-subdir))
        log-file               (str (io/file (get-in @db [:config :nextflow-logs-dir]) (str (now-by-second-only-digits!) "-" pipeline-short-name "-nextflow.log")))
        ]

    (doseq [library-id (map :id assemblies)]
      (let [message {:event :analysis-started
                     :timestamp (now!)
                     :pipeline-name pipeline-full-name
                     :id library-id
                     :library-id library-id}]
        (go
          (>! message-chan {:topic :analysis
                            :message message})
          (>! message-chan {:topic :logging
                            :message {:log/level :info
                                      :log/message message}}))))
    
    (sh "mkdir" "-p" outdir)
    (sh "chmod" "750" outdir)
    (with-open [writer (io/writer samplesheet-path)]
      (csv/write-csv writer samplesheet-data))
    (sh "mkdir" "-p" work-dir)
    (sh "chmod" "750" work-dir)

    (shell/with-sh-dir outdir
      (apply sh ["nextflow"
                 "-log" log-file
                 "run" pipeline-full-name
                 "-profile" "conda"
                 "--cache" (str (io/file (System/getProperty "user.home") ".conda/envs"))
                 "-r" pipeline-full-version
                 "--pre_assembled"
                 "--samplesheet_input" samplesheet-path
                 "--mob_db" mob-suite-db
                 "-work-dir" work-dir
                 "--versioned_outdir"
                 "--outdir" "."]))

    (doseq [assembly assemblies]
      (let [library-id (:library-id assembly)
            library-analysis-outdir (io/file outdir library-id (str/join "-" [pipeline-short-name pipeline-minor-version "output"]))
            now (now!)
            analysis-completed-message {:event :analysis-completed
                                        :timestamp now
                                        :pipeline-name pipeline-full-name
                                        :pipeline-version pipeline-full-version
                                        :id library-id
                                        :library-id library-id
                                        :outdir (str library-analysis-outdir)}
            plasmid-screen-completed-message {:event :plasmid-screen-completed
                                              :timestamp now
                                              :id library-id
                                              :library-id library-id
                                              :resistance-gene-report-path (str (io/file library-analysis-outdir (str library-id "_resistance_gene_report.tsv")))}]
        (spit (io/file library-analysis-outdir "analysis_complete.json")
              {:timestamp (now!)})
        (go (>! message-chan {:topic :analysis
                              :message plasmid-screen-completed-message})
            (>! message-chan {:topic :logging
                              :message {:log/level :info
                                        :log/message analysis-completed-message}})
            ;; remove this one after debugging
            (>! message-chan {:topic :logging
                              :message {:log/level :info
                                        :log/message plasmid-screen-completed-message}}))))

    (sh "find" outdir "-type" "d" "-exec" "chmod" "750" "{}" "+")
    (sh "find" outdir "-type" "f" "-exec" "chmod" "640" "{}" "+")
    
    (sh "rm" "-r" work-dir)
    (sh "rm" samplesheet-path)))


(defn group-analysis-events
  "Takes a sequence of events, and groups them first by :event, then by the year (as indicated by the :id)
  takes:
    Sequence of maps, each with tags:
     `:event`:
     `:id`:

  returns:
    Map, with structure:
      { `event-key` { `id-year` [`events`]}}
    eg:
      `{:symlinks-created {\"21\" [{:event :symlinks-created :id \"BC21AXYZA\"}...]
                           \"22\" [{:event :symlinks-created :id \"BC22AXYZA\"}...]}}`
  "
  [events]
  (->> events
       (group-by :event)
       (#(update-vals % group-samples-by-year))))


(defn symlinks-created-event->library
  "

  returns:
    map with keys:
      `:id`:
      `:r1-path`:
      `:r2-path`:
  "
  [event]
  (let [id (:id event)
        r1-path (get-in event [:symlink-paths :r1])
        r2-path (get-in event [:symlink-paths :r2])]
    {:id id
     :r1-path r1-path
     :r2-path r2-path}))


(defn demultiplex-analysis
  "Takes a map of grouped
  "
  [[event-type events-by-year] message-chan db]
  (case event-type
    :symlinks-created (dorun (for [year (keys events-by-year)]
                               (let [output-subdir year
                                     libraries (map symlinks-created-event->library (get events-by-year year))]
                                 (go (run-routine-assembly! libraries output-subdir db message-chan))
                                 (go (run-taxon-abundance! libraries output-subdir db message-chan)))))
    :assembly-completed (dorun (for [year (keys events-by-year)]
                                 (let [output-subdir year
                                       assemblies (get events-by-year year)]
                                   (go (run-mlst-nf! assemblies output-subdir db message-chan))
                                   (go (run-plasmid-screen! assemblies output-subdir db message-chan)))))
    nil
    )
  )


(defn start-analyzer!
  "Starts the (asynchronous) process of analyzing any directories put on `libraries-to-analyze-chan`.

   takes:
     `message-chan`: (`Channel`)
     `analysis-chan`: Channel to take run directories from for analysis (`Channel`)
     `db`: Global app-state db (`Atom`)

   returns:
     (`Channel`)
  "
  [message-chan analysis-chan batched-analysis-chan db]
  (go-loop [took (<! batched-analysis-chan)]
    (when-some [messages (into [] (map :message took))]
      (->> messages
           (group-analysis-events)
           (map #(demultiplex-analysis % message-chan db))
           (dorun)))
    (recur (<! batched-analysis-chan))))


(defn analysis-output-dir-name->pipeline-data
  "Parse a pipeline output directory to get data about the pipeline
  
   takes:
     `dir-name`: The directory name where the pipeline output was written to.
     eg: `plasmid-screen-v0.2-output`

    returns:
      Map with keys:
        `:pipeline-name`
        `:pipeline-version`
  "
  [dir-name]
  (let [pipeline-short-name (first (str/split dir-name #"-v\d+\.\d+"))
        pipeline-version (str/replace (re-find #"-v\d+\.\d+-" "plasmid-screen-v0.2-output") "-" "")]
    {:pipeline-name (str "BCCDC-PHL/" pipeline-short-name)
     :pipeline-version pipeline-version}))


(defn scan-completed-analyses!
  "
  takes:
    `db`: Global app-stated db (`Atom`)

  returns:
    Sequence of maps, each with keys:
      `:library-id`:
      `:pipeline-name`:
      `:pipeline-version`:
      `:output-subdir`:
  "
  [db]
  (let [analysis-outdir (get-in @db [:config :analysis-output-dir])
        analysis-outdirs-by-year (scan-directory! analysis-outdir)
        analysis-outdirs-by-sample (filter #(not (or (re-find #"^\." (.getName (io/file %)))
                                                     (re-find #"^work" (.getName (io/file %)))))
                                           (mapcat scan-directory! analysis-outdirs-by-year))
        analysis-outdirs-by-pipeline (mapcat scan-directory! analysis-outdirs-by-sample)]
    (->> analysis-outdirs-by-pipeline
         (filter #(.exists (io/file % "analysis_complete.json")))
         (map #(str/split % #"/"))
         
         (map #(let [library-id (last (butlast %))
                     analysis-output-dir (last %)
                     pipeline-data (analysis-output-dir-name->pipeline-data analysis-output-dir)
                     year (library-id->year library-id)
                     output-subdir (str (io/file year library-id analysis-output-dir))]
                 (merge pipeline-data 
                        {:id library-id
                         :library-id library-id
                         :output-subdir output-subdir}))))))


(defn scan-symlinked-libraries!
  "
  takes:
    `db`: Global app-stated db (`Atom`)
  
  returns:
    IDs for Libraries that have been symlinked (`#{String}`)
  "
  [db]
  (let [fastq-symlinks-dir (get-in @db [:config :fastq-symlinks-dir])
        fastq-symlinks-dirs-by-year (scan-directory! fastq-symlinks-dir)]
    (->> fastq-symlinks-dirs-by-year
         (mapcat scan-directory!)
         (map #(.getName (io/file %)))
         (map #(first (str/split % #"_")))
         (into #{}))))


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

  ;; Channels, pubs & subs
  (defonce message-chan (chan))
  (defonce kill-message-chan (chan))

  (defonce logging-chan (chan))
  (defonce kill-logging-chan (chan))
  
  (defonce symlinking-chan (chan))
  (defonce kill-symlinking-chan (chan))

  (defonce analysis-chan (chan))
  (let [analysis-batch-max-size (get-in @db [:config :analysis-batch-max-size])
        analysis-batch-timeout-ms (get-in @db [:config :analysis-batch-timeout-ms])]
    (defonce batched-analysis-chan (batch analysis-chan analysis-batch-max-size analysis-batch-timeout-ms)))
  
  (defonce kill-analysis-chan (chan))
  
  (defonce message-pub (async/pub message-chan :topic))

  (async/sub message-pub :logging logging-chan)
  (async/sub message-pub :symlinking symlinking-chan)
  (async/sub message-pub :analysis analysis-chan)

  ;;
  ;; 
  (start-symlinker! message-chan symlinking-chan db)
  (start-scanning-for-runs-to-symlink! message-chan kill-symlinking-chan db)

  (start-analyzer! message-chan analysis-chan batched-analysis-chan db)

  ;;
  ;; Log any messages from the logging-chan
  (go-loop [took (<! logging-chan)]
    (when-some [msg (:message took)]
      (log/info (:log/message msg )))
      (recur (<! logging-chan)))

  ;;
  ;; Main loop
  (loop []
    #_(>!! message-chan {:topic :logging
                         :message {:log/level :info
                                   :log/message {:event :main-loop
                                                 :timestamp (now!)}}})
    (Thread/sleep 10000)
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


  (defonce symlinking-chan (chan))
  (defonce kill-symlinking-chan (chan))

  (defonce analysis-chan (chan))
  (defonce kill-analysis-chan (chan))

  ;; Control symlinking process
  (start-symlinker! message-chan symlinking-chan db)
  (start-scanning-for-runs-to-symlink! message-chan symlinking-chan db)
  (put-stop! kill-symlinking-chan)
 
  
  (start-analyzer! libraries-to-analyze-chan db)
  (start-scanning-for-libraries-to-analyze! libraries-to-analyze-chan kill-analysis-chan db)
  (put-stop! kill-analysis-chan)

  (let [libraries [{:id "BC21A763A"
                    :r1-path "/home/dfornika/code/analysis-automation/auto-cpo/test_output/fastq_symlinks/by_year/21/BC21A763A_R1.fastq.gz"
                    :r2-path "/home/dfornika/code/analysis-automation/auto-cpo/test_output/fastq_symlinks/by_year/21/BC21A763A_R2.fastq.gz"}]]
    (run-routine-assembly! libraries db))
  
  (with-open [writer (io/writer "test.csv")]
    (csv/write-csv writer (maps->csv-data [{:hello "world"
                                            :goodbye "earth"}])))
  )
