# Auto QC
Run the [BCCDC-PHL/routine-sequence-qc](https://github.com/BCCDC-PHL/routine-sequence-qc) pipeline.

### Usage
```
Usage: java -jar auto-qc.jar OPTIONS

Options:
  -c, --config CONFIG_FILE Config file.
  -h, --help
  -v, --version
```

### Config
The config file is expected to be in [edn](https://github.com/edn-format/edn) format. 

Example config:
```edn
{:run-dirs ["/path/to/run/dir/1"
	    "/path/to/run/dir/2"
	    "/path/to/run/dir/3"]
 :exclude-files ["/path/to/exclude-file-1"
                 "/path/to/exclude-file-2"]}
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
