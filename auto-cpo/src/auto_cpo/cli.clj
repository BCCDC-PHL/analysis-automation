(ns auto-cpo.cli
  (:require [clojure.java.io :as io]
            [clojure.string :as str]))

(def version "v0.1.0")

(defn usage
  ""
  [options-summary]
  (->> [(str/join " " ["auto-cpo" version])
        ""
        "Usage: java -jar auto-cpo.jar OPTIONS"
        ""
        "Options:"
        options-summary]
       (str/join \newline)))


(def options
  [["-c" "--config CONFIG_FILE" "Config file"
    :validate [#(.exists (io/as-file %)) #(str "Config file '" % "' does not exist.")]]
   ["-h" "--help"]
   ["-v" "--version"]])

(defn exit
  "Exit the program with status code and message"
  [status msg]
  (println msg)
  (System/exit status))
