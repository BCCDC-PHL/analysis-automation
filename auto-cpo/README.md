# Auto CPO
Run pipelines.

### Building
A build script (`build.sh`) is included in this repo. The `clj` command-line tool is required for building. Follow [this guide](https://clojure.org/guides/getting_started) to install. A standalone `.jar` file will be compiled under the `target` directory.

### Usage
```
Usage: java -jar auto-cpo.jar OPTIONS

Options:
  -c, --config CONFIG_FILE Config file.
  -h, --help
  -v, --version
```

### Config
The config file is expected to be in [edn](https://github.com/edn-format/edn) format. 

Example config:
```edn
{:run-dirs ["/path/to/sequencer/outputs"]
 :run-exclude-files ["/path/to/excluded_runs.csv"]
 :library-exclude-files ["/path/to/excluded_libraries.csv"] 
 :fastq-symlinks-dir "/path/to/fastq_symlinks_by_run"
 :analysis-output-dir "/path/to/analysis_by_run"
 :nextflow-logs-dir "/path/to/nextflow_logs"
 :samplesheet-project-id "cpo"
 :taxon-abundance-config {:version ""}
 :routine-assembly-config {:version ""}
 :mlst-nf-config {:version ""}
 :plasmid-screen-config {:version ""}
 :config-reload-interval-ms 60000
 :exclude-files-reload-interval-ms 10000
 :symlinking-scanning-interval-ms 10000
 :analysis-scanning-interval-ms 10000
 :repl false}
```

### Exclude files
The files listed under the `:run-exclude-files` and `:library-exclude-files` key in the config are lists of runs or libraries that should be excluded from analysis.
They should simply contain one identifier name per line.

Example run exclude file:
```
210230_M00123_0123_000000000-AAB12
210412_M00123_0138_000000000-AD623
210623_M00426_0165_000000000-B3A52
```

Example libary exclude file:
```
S123456
S123457
S123458
```

