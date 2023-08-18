# Creating a Spark session

The [spark_session_utils] module inside the DataFrame section implements code
to instantiate a Spark session via a context manager. This can be used as:

```Python

from dps.spark_df.utils.spark_session_utils import spark_session

with spark_session(**kwargs) as spark:

  # use "spark" as a SparkSession object, e.g.
  df = spark.read.format("json").load(name)
  ...
  
  # or work at the RDD level through a SparkContext object, e.g.
  sc = spark.sparkContext
  rdd = sc.textFile(filename)
  ...

```

## Arguments

The `spark_session` function accepts the following arguments:

* `appname`: Spark application name, possibly for monitoring/logging purposes
  (if not given, the name will be `dps-process`) 
* `master`: specification of the Spark master to use (see below)
* `config`: a dictionary with configuration options for the Spark driver & 
  executors. An example is given in the `spark` section of a [DataFrame
  configuration file]
* `s3`: a Boolean indicating if the session is going to access files in S3.
  If `True`, the function will add special [S3 provider] configuration to
  enable cluster access to S3 (using an EC2 Instance Profile as the IAM Role
  for authentication)


## Spark master

The Spark master to be used to launch the process is decided in the following
order:
1. The master defined in the `master` argument to the function
2. If not defined, the `master` field inside the `config` argument passed
   to the function
3. If that is not defined either, a default set in the code, which currently
   is `local[20]` (i.e. a local master with 20 execution threads).


[spark_session_utils]: ../dps/spark_df/utils/spark_session_utils.py
[DataFrame configuration file]: ../configs/df/preproc-example.yaml
[S3 provider]: https://spark.apache.org/docs/latest/cloud-integration.html#installation
