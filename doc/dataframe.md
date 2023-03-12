# DataFrame processing framework

The dps DataFrame framework allows processing of documents using Spark
DataFrames as the wrapping infrastructure, and Pandas DataFrames as the
underlying preprocessing modules.

It does so by using a `UdfPreprocessor` module that is executed as a [Pandas
UDF] in Spark, and is governed by a [configuration file]

When installed, the `dps` package adds a command-line entry point,
`dps-sun-spark`, which launches the process. It requires a configuration file
as its main argument.


## Configuration

The configuration is a YAML file with the following sections:
 * `general`: generic options
 * `source`: defines the data source
 * `dest`: defines the output destination
 * `spark`: configuration to apply to create the Spark session
 * `preprocess`: defines the preprocessing blocks to apply
 * `logging`: defines configuration for loggers


## Source data

The source data is specified in the configuration file, in the `source` field.
It contains:
 - `base`: base path to prepend to all file paths
 - `paths`: list of filenames to read (it can point to a directory; all files
   in the directory will be read)
 - `options`: options to pass to the Spark file loader. For instance:
     - `pathGlobFilter`: pattern to filter input filenames with
     - `encoding`: charset encoding

Note that the Spark DataFrame reader has native capabilities:
 * it can read compressed files (e.g. `bz2` or `gz`) and uncompress them  on
   the fly
 * it can also read files stored in S3 buckets (use an `s3a://` URL to activate
   the S3 provider)


## Processing

Implemented preprocessing steps are:
 * [langfilter]: language detection and filtering
 

[Pandas UDF]: https://spark.apache.org/docs/3.1.2/api/python/user_guide/arrow_pandas.html#pandas-function-apis
[S3 provider]: https://spark.apache.org/docs/latest/cloud-integration.html#installation
[configuration file]: ../configs/preproc_df.yaml
[langfilter]: udf/langfilter.md
