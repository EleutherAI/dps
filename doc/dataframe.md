# DataFrame processing framework

The dps DataFrame framework allows processing of documents using Spark
DataFrames as the wrapping infrastructure, and Pandas DataFrames as the
underlying preprocessing modules.

It does so by using a `UdfProcessor` module that is executed as a [Pandas
UDF] within Spark, and is governed by a [configuration file]

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
There are two ways of achieving this: via a separate complete Spark
installation, or through the [pyspark] package. Both should work if we will
use a local Spark master (but for a Spark distributed service we will need the
first option _and_ configuration of the Spark master server and workers).


#### 1.2.1 Option 1: Separate complete Spark installation

If there is no local Spark distribution available, install it:
* Download a Spark distribution from
  [https://spark.apache.org/downloads.html]. Use the latest release, pre-built
  for Hadoop
* Uncompress the package in a local folder.

Once Spark is locally available, define the environment variable `SPARK_HOME`
to point to the root path of the local Spark

	export SPARK_HOME=<spark_homedir>


#### 1.2.2 Option 2: Pyspark installation

Just install [pyspark] in the same virtualenv where the dps package was
installed:

    <VENVDIR>/bin/pip install pyspark


## 2. Operation

When installed, the `dps` package adds a command-line entry point,
`dps-run-spark`, which launches the full process. It requires a configuration
file as its main argument. The file can be given as an absolute path, or as
a plain filename (in the latter case it will be searched in the `etc/dps/df`
subfolder of the virtualenv).


### 2.1 Configuration

The configuration is a YAML file with the following sections:
 * `general`: generic options
 * `spark`: configuration to apply to create the Spark session
 * `logging`: defines the logging configuration
 * `io`: defines the data source & destination. It can be a single dict, with
   elements `source` and `dest`, or a list of dicts (which will be processed
   sequentially).
 * `process`: defines the processing blocks to apply

The `io` and `process` sections are compulsory. There is an [example
configuration] available.


### 2.2 Source data

In the configuration file the source data is specified as the `source`
subfield under `io`. There can be a list of sources; each one must be paired
with a corresponding `dest` subfield.

A `source` field contains:
 - `format`: source format (from among the ones accepted by Spark)
 - `base`: base path to prepend to all file paths
 - `paths`: an optional list of subpaths to add to `base`; they can be either
   filenames or directory names  (in the latter case all files in the directory
   will be considered)
 - `options`: options to pass to the Spark file loader. For instance:
     - `pathGlobFilter`: pattern to filter input filenames with
     - `encoding`: charset encoding for the files

Note that the Spark DataFrame reader has additional native capabilities:
 * it can read compressed files (e.g. `bz2` or `gz`) and uncompress them on the fly
 * it can also read files stored in S3 buckets (use an `s3a://` URL to activate
   the S3 provider). In this case a special configuration for S3 will be
   defined; it is possible to modify that configuration with custom parameters
   by using the `spark/s3_config` section in the configuration (all fields in
   that section will be prefixed with a `spark.hadoop.fs.s3a.` prefix)


### 2.3 Destination

Each source must have a parallel associated `dest` field, that describes the
output destination.

Elements inside `field` are:
 - `format`: destination format (from among the ones accepted by Spark)
 - `name`: output destination name
 - `mode`: `overwrite` (overwrite an existing destination with the same
   name), `errorifexists` (generate an error if the destination already
   exists)
 - `partitions`: number of output partitions
 - `options`: additional options to pass to the DataFrame writer

The Spark DataFrame writer can write to S3 buckets (use an `s3a://` prefix)
and can also write compressed files. It is also important to remember that
Spark works in a distributed fashion, and the output name is not actually a
filename, but a _directory_ name. Inside it a number of files will be written,
one per data partition (using the pattern `part-NNNNN-...`). By default the
number of partitions (hence the number of output files) is given by the Spark
partitioning configuration, but the `partitions` option in `dest` can be
used to repartition the data to a different number when writing.


## 3. Available processing

Implemented processor modules are:
 * [splitter]: split documents into pieces
 * [langfilter]: language detection and filtering
 

## 4. Implementation of a processor module

Each processor module must be implemented as a file inside the [udf] directory.

  * The implementation should be a class
  * The constructor of the class receives a configuration dictionary, taken
    as the config for this processor inside the main config file. It should
	then initialize any processing objects it needs (e.g. load models)
  * The class should be callable (i.e. have a `__call__` method). This is
    the entry point
	  - The entry point recceives a  *Pandas* DataFrame (*not* a Spark DataFrame)
	  - Each Dataframe row is a document; each row will have at least a
	    `text`column
	  - The method must process the DataFrame and return the result, also
	    as a Pandas DataFrame
  * Once the processor is implemented, it can be added to the configuration
    file as a [processor configuration](#processor-configuration)
  * It is also recommended to add a documentation for the module in the
    [udf processor doc folder](udf)


Note that, since it is called inside a Pandas UDF, a processor _does not work
with Spark types at all_; everything is done in Pandas.

You can check the [langfilter](../dps/spark_df/udf/langfilter.py) preprocessor
as an example.


### Processor configuration

The `process` field in a configuration contains a list of dictionaries. Each
one describes the configuration for a single processor; the system will call
each processor in the order given in the `process` list.

Each processor configuration contains:
 * a `class` field: this is the only compulsory field, and its value must
   contain the class to instantiate, as a `module.classname` string (e.g.
   `langfilter.LangFilter`), where `module` is the module name inside the
   [udf] folder .
 * a `columns` field. This is only required if the processor will create _new_
   columns); if so the field contains a list of dicts, each one with two fields:
      - the `name` element contains the column name
	  - the `type` element contains the [pyspark data type] for the column
 * any other content in the dictionary forms the parameter that will be
   sent as argument to the class constructor


## 5. Spark-level processing

To implement processing operations at the global Spark DataFrame level
(i.e. not working on local Pandas DataFrames) add those operations to the main
DataFrame processing code in the [process] module. Configuration for these
operations should be added to the main config file, on a section different
from `process` (which contains the list processing operations called inside
Pandas UDFs).


[pyspark data type]: https://spark.apache.org/docs/latest/sql-ref-datatypes.html
[example configuration]: ../configs/df/preproc_df.yaml
[configuration file]: ../configs/df/preproc-example.yaml
[splitter]: udf/splitter.md
[langfilter]: udf/langfilter.md
[udf]: ../dps/spark_df/udf
[process]: ../dps/spark_df/process.py
[preproc]: ../dps/spark_df/preproc.py
[pyspark]: https://pypi.org/project/pyspark
[Pandas UDF]: https://spark.apache.org/docs/latest/api/python/user_guide/sql/arrow_pandas.html#map
[S3 provider]: https://spark.apache.org/docs/latest/cloud-integration.html#installation
