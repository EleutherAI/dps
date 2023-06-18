# DataFrame processing framework

The dps DataFrame framework allows processing of documents using Spark
DataFrames as the wrapping infrastructure, and Pandas DataFrames as the
underlying preprocessing modules.

It does so by using a `UdfPreprocessor` module that is executed as a [Pandas
UDF] in Spark, and is governed by a [configuration file]

## 1. Installation

### 1.1 Main installation

The package needs a Python virtualenv, with at least Python 3.8. To install
it, decide the place in which to create the Python virtualenv (the instructions
below use `<VENVDIR>` as that place). Then perform installation there by
executing, on a cloned `dps` repository:

     VENV=<VENVDIR> make install

This will:
  * create the virtualenv if it does not exist (if it exists it will just be
    reused)
  * build the `dps-N.M.P-tar.gz` package file
  * install the package in the virtualenv together with its dependencies

Note 1: by default the virtualenv will be created with the `python3` executable;
to use a more specific version add it as an additional environment variable, i.e.

     PYTHON=python3.10 VENV=<VENVDIR> make install

Note 2: if the source code is in a different server than the one it will run
on, it is possible to first build the package file by executing

	  make pkg

and then copy the package file (in the `dist` folder) to the destination server
and replicate the install commands from the Makefile:

	export VENV=<VENVDIR>
	mkdir -p $VENV
	python3 -m venv $VENV
	$VENV/bin/pip install --upgrade pip wheel
	$VENV/bin/pip install dps-N.M.P.tar.gz


### 1.2 Spark dependency

In order for the processing to work, we need a working Spark environment.
There are two ways of achieving this: via a separate Spark installation, or
through the [pyspark] package. Both should work if we will use a local Spark
master (but for a Spark distributed service we will need the first option _and_
configuration of the Spark master server and workers).


#### 1.2.1 Separate Spark installation

If there is no local Spark distribution available, install it:
* Download a Spark distribution from
  [https://spark.apache.org/downloads.html]. Use the latest release, pre-built
  for Hadoop
* Uncompress the package in a local folder.

Once Spark is locally available, define the environment variable `SPARK_HOME`
to point to the root path of the local Spark

	export SPARK_HOME=<spark_homedir>


#### 1.2.2 Pyspark installation

Just install [pyspark] in the same virtualenv where the dps package was
installed:

    <VENVDIR>/bin/pip install pyspark


## 2. Operation

When installed, the `dps` package adds a command-line entry point,
`dps-run-spark`, which launches this process. It requires a configuration file
as its main argument.


### 2.1 Configuration

The configuration is a YAML file with the following sections:
 * `general`: generic options
 * `io`: defines the data source & destination. It can be a single dict, with
   elements `source` and `dest`, or a list of dicts (which will be processed
   sequentially).
 * `spark`: configuration to apply to create the Spark session
 * `preprocess`: defines the preprocessing blocks to apply
 * `logging`: defines configuration for loggers

There is an [example configuration](../configs/preproc_df.yaml) available.


### 2.2 Source data

The source data is specified in the configuration file, in the `source` subfield.
There can be a list of them; each one will be paired with its corresponding
`dest` subfield.

A `source` field contains:
 - `base`: base path to prepend to all file paths
 - `paths`: list of filenames to read (each one can also point to a directory;
   in that case all files in the directory will be read)
 - `options`: options to pass to the Spark file loader. For instance:
     - `pathGlobFilter`: pattern to filter input filenames with
     - `encoding`: charset encoding

Note that the Spark DataFrame reader has native capabilities:
 * it can read compressed files (e.g. `bz2` or `gz`) and uncompress them on the fly
 * it can also read files stored in S3 buckets (use an `s3a://` URL to activate
   the S3 provider). In this case a special configuration for S3 will be
   defined; it is possible to modify that default configuration by using the
   `spark/s3_config` section in the configuration (all fields in the section
   will be prefixed with a `spark.hadoop.fs.s3a.` prefix)


## 3. Processing

Implemented preprocessing steps are:
 * [langfilter]: language detection and filtering
 
## 4. Implementtation of prrocessing steps

Each step to be applied to the data must be implemented as a file inside the
[udf](../dps/spark_df/udf) directory.

  * The implementation should be a class
  * The constructor of the class receives a configuration dictionary: the
    subfields for this preprocessor in the main config. It should then initialize
	any processing objects it needs (e.g. models)
  * The class should be callable (i.e. have a `__call__` method). This is
    the entry point
	  - The entry point recceives a  *Pandas* DataFrame
	  - Each Dataframe row is a document; there will be at least a `text`column
	  - The method must process the DataFrame and return the result, also
	    as a FataFrame
  * Once the preprocessor is implemented, add its initialization code to the
    `_init_obj` method in the [preproc] object.


You can check the [langfilter](../dps/spark_df/udf/langfilter.py) preprocessor
as an example.


[pyspark]: https://pypi.org/project/pyspark
[Pandas UDF]: https://spark.apache.org/docs/3.1.2/api/python/user_guide/arrow_pandas.html#pandas-function-apis
[S3 provider]: https://spark.apache.org/docs/latest/cloud-integration.html#installation
[configuration file]: ../configs/preproc_df.yaml
[langfilter]: udf/langfilter.md
[preproc]: ../dps/spark_df/preproc.py
