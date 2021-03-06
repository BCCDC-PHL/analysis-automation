# Auto nCoV
Run the [BCCDC-PHL/ncov2019-artic-nf](https://github.com/BCCDC-PHL/ncov2019-artic-nf) and [BCCDC-PHL/ncov-tools-nf](https://github.com/BCCDC-PHL/ncov-tools-nf) pipelines.

### Building
A build script (`build.sh`) is included in this repo. The `clj` command-line tool is required for building. Follow [this guide](https://clojure.org/guides/getting_started) to install. A standalone `.jar` file will be compiled under the `target` directory.

### Usage
```
Usage: java -jar auto-ncov.jar OPTIONS

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
 :exclude-files ["/path/to/exclude.csv"]
 :symlinks-dir "/path/to/fastq_symlinks_by_run"
 :analysis-output-dir "/path/to/analysis_by_run"
 :samplesheet-project-id "covid-19_production"
 :ncov2019-artic-nf-config {:version "v1.3.3"
                            :ref "/path/to/nCoV-2019.reference.fasta"
                            :bed "/path/to/nCoV-2019.primer.bed"
                            :primer-pairs-tsv "/path/to/primer_pairs.tsv"
                            :gff "/path/to/nCoV-2019.gff"
                            :composite-ref "/path/to/composite_GRCh38_SARS-CoV-2.fna"}
 :ncov-tools-nf-config {:version "v1.5.7"
                        :ncov-watchlists-version "1.3"}
 :config-reload-interval-ms 60000
 :exclude-files-reload-interval-ms 10000
 :symlinking-scanning-interval-ms 10000
 :analysis-scanning-interval-ms 10000
 :repl false}
```

### Exclude files
The files listed under the `:exclude-files` key in the config are lists of directories that should be excluded from analysis.
They should simply contain one directory name per line (not a full path).

Example exclude file:
```
210230_M00123_0123_000000000-AAB12
210412_M00123_0138_000000000-AD623
210623_M00426_0165_000000000-B3A52
```
