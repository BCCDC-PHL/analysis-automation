(ns auto-qc.cli
  (:require [clojure.string :as str]))

(def version "v0.1.0")

(defn usage
  ""
  [options-summary]
  (->> [(str/join " " ["auto-qc" version])
        ""
        "Usage: java -jar auto-qc.jar OPTIONS"
        ""
        "Options:"
        options-summary]
       (str/join \newline)))


(def options
  [["-r" "--runs-dir RUN_DIR"]
   ["-e" "--exclude EXCLUDE_FILE"]
   ["-h" "--help"]
   ["-v" "--version"]])

(defn exit
  "Exit the program with status code and message"
  [status msg]
  (println msg)
  (System/exit status))
