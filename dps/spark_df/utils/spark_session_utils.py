"""
Some utilities to manage Spark sessions
"""
import os
import sys
from contextlib import contextmanager

from typing import Dict

import boto3
from pyspark import version as pyspark_version
from pyspark.sql import SparkSession

from . import logging

DEFAULT_MASTER = 'local[20]'
DEFAULT_EXECUTOR_CORES = 5
DEFAULT_EXECUTOR_MEMORY = "2g"
DEFAULT_SCALA_VERSION = "2.12"
DEFAULT_S3_DURATION = 12

# Defaults S3 credentials provider
S3_EC2_PROVIDER = "com.amazonaws.auth.InstanceProfileCredentialsProvider"

# Hadoop AWS packages for each Spark version
HADOOP_VERSION = {
    "3.4.1": "3.3.4",
    "3.4.0": "3.3.4",
    "3.3.2": "3.3.2"
}


def s3_session_credentials(config: Dict) -> Dict:
    """
    Use the boto3 library to get credentials for an S3 session
    For this to work, the AWS account needs to have IAM Assumed Roles permission
    """
    arn = config.pop("arn", "arn:aws:iam::842865360552:role/s3_access_from_ec2")
    session_hours = config.pop("session_duration", DEFAULT_S3_DURATION)
    profile = config.pop("profile", "default")

    os.environ["AWS_CONFIG_FILE"] = os.path.expanduser('~') + "/.aws/sparkconfig"
    session = boto3.session.Session(profile_name=profile)
    sts_connection = session.client("sts")
    response = sts_connection.assume_role(RoleArn=arn, RoleSessionName="hi",
                                          DurationSeconds=session_hours*3600)
    credentials = response["Credentials"]
    return {"access.key": credentials["AccessKeyId"],
            "secret.key": credentials["SecretAccessKey"],
            "session.token": credentials["SessionToken"]}


def s3_config(sb: SparkSession.Builder, scala_version: str, s3_config: Dict = None):
    """
    Take a spark configuration and customize it for efficient S3 access
    Taken from https://github.com/rom1504/cc2dataset/blob/main/cc2dataset/spark_session_builder.py
    """

    # Add the relevant Hadoop JAR libraries to be able to use S3 filesystems
    version = pyspark_version.__version__
    if version not in HADOOP_VERSION:
        raise Exception(f"Unsupported Spark version {version} for Hadoop AWS. Supported versions: " + ",".join(HADOOP_VERSION))
    pkg = [
        f"org.apache.hadoop:hadoop-aws:{HADOOP_VERSION[version]}",
        f"org.apache.spark:spark-hadoop-cloud_{scala_version}:{version}"
    ]
    sb.config("spark.jars.packages", ",".join(pkg))

    # Set the S3 endpoint, if defined in the config
    endpoint = s3_config.get("endpoint", None)
    if endpoint:
        sb.config("spark.hadoop.fs.s3a.endpoint", endpoint)

    # Ton of options to try and make s3a run faster
    sb.config("spark.hadoop.fs.s3a.threads.max", "512")
    sb.config("spark.hadoop.fs.s3a.directory.marker.retention", "keep")
    sb.config("spark.hadoop.fs.s3a.max.total.tasks", "512")
    sb.config("spark.hadoop.fs.s3a.multipart.threshold", "5M")
    sb.config("spark.hadoop.fs.s3a.multipart.size", "5M")
    sb.config("spark.hadoop.fs.s3a.connection.maximum", "2048")
    sb.config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
    sb.config("spark.hadoop.fs.s3a.connection.timeout", "600000")
    sb.config("spark.hadoop.fs.s3a.readahead.range", "2M")
    sb.config("spark.hadoop.fs.s3a.socket.recv.buffer", "65536")
    sb.config("spark.hadoop.fs.s3a.socket.send.buffer", "65536")
    sb.config("spark.hadoop.fs.s3a.experimental.input.fadvise", "random")
    sb.config("spark.hadoop.fs.s3a.block.size", "2M")
    sb.config("spark.hadoop.fs.s3a.fast.buffer.size", "100M")
    sb.config("spark.hadoop.fs.s3a.fast.upload", "true")
    sb.config("spark.hadoop.fs.s3a.fast.upload.buffer", "array")
    sb.config("spark.hadoop.fs.s3a.fast.upload.active.blocks", "512")
    sb.config("spark.hadoop.fs.s3a.bucket.all.committer.magic.enabled", "true")

    # The cluster requires the connection to be through SSL
    sb.config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")

    # From https://spark.apache.org/docs/latest/cloud-integration.html#committing-work-into-cloud-storage-safely-and-fast
    sb.config("spark.hadoop.fs.s3a.committer.name", "directory")
    sb.config("spark.sql.sources.commitProtocolClass",
              "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol")
    sb.config("spark.sql.parquet.output.committer.class",
              "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter")

    # From https://spark.apache.org/docs/latest/cloud-integration.html#parquet-io-settings
    sb.config("spark.hadoop.parquet.enable.summary-metadata", "false")
    sb.config("spark.sql.parquet.mergeSchema", "false")
    sb.config("spark.sql.parquet.filterPushdown", "true")
    sb.config("spark.sql.hive.metastorePartitionPruning", "true")

    # Other
    sb.config("spark.sql.shuffle.partitions", "4000")
    sb.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    sb.config("spark.hadoop.com.amazonaws.services.s3.enableV4", "true")

    # Get any specific S3 options passed in the config
    if s3_config is None:
        s3_config = {}
    provider = s3_config.pop("aws.credentials.provider", None)
    if provider:
        #if provider == S3_EC2_PROVIDER:
        #   s3_config.update(s3_session_credentials(s3_config))
        # Change to the appropriate auth method
        # see https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html
        sb.config("spark.hadoop.fs.s3a.aws.credentials.provider", provider)

    # Add all S3 defined configs
    for k, v in s3_config.items():
        sb.config("spark.hadoop.fs.s3a."+k, v)


def build_spark_session(appname: str = None, master: str = None,
                        config: Dict = None, s3: bool = False) -> SparkSession:
    """
    Create a Spark session
      :param appname: name to give to the running Spark application
      :param master: Spark master (if different from the config)
      :param config: configuration deininig Spark launching parameters
      :param s3: activate S3 support in the Spark session
    """
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    if config is None:
        config = {}
    if appname is None:
        appname = "dps-process"
    if master is None:
        master = config.get("master", DEFAULT_MASTER)

    log = logging.getLogger(__name__)
    log.info("Spark session: master=%s appname=%s", master, appname)
    sb = SparkSession.builder.master(master).appName(appname)

    driver_memory = config.get("driver", {}).get("memory", "4g")
    sb.config("spark.driver.memory", str(driver_memory))

    executor = config.get("executor", {})

    cores = executor.get("cores", DEFAULT_EXECUTOR_CORES)
    sb.config("spark.executor.cores", str(cores))

    memory = executor.get("memory", DEFAULT_EXECUTOR_MEMORY)
    memory, unit = memory[:-1], memory[-1]
    memory = int(memory)*(1000 if unit == "g" else 1)
    memory_main = int(memory * 0.9)
    memory_overhead = memory - memory_main
    sb.config("spark.executor.memory", f"{memory_main}m")
    sb.config("spark.executor.memoryOverhead", f"{memory_overhead}m")
    log.info("Executor memory: %d %d", memory_main, memory_overhead)

    if s3:
        try:
            scv = config.get("scala_version", DEFAULT_SCALA_VERSION)
            s3_config(sb, scala_version=scv, s3_config=config.get("s3_config"))
        except Exception as e:
            raise Exception(f"Error: S3 config failed: {e}") from e

    custom_conf = config.get("config", {})
    for k, v in custom_conf.items():
        sb.config(k, v)

    # Shut down some internal loggers
    logging.getLogger("py4j").setLevel(logging.WARNING)
    logging.getLogger("pyspark").setLevel(logging.WARNING)

    # Create the SparkSession
    spark = sb.getOrCreate()

    # Adjust logging for Spark
    spark_loglevel = config.get("spark_logging", {}).get("level")
    if spark_loglevel:
        # Set Spark log level
        spark.sparkContext.setLogLevel(spark_loglevel)
        # Log Spark config
        if logging.getLevelName(spark_loglevel) <= logging.DEBUG:
            conf = spark.sparkContext.getConf().getAll()
            log.debug("Spark config:\n  %s",
                      "\n".join(f"  {k}={v}" for k, v in sorted(conf)))

    return spark


@contextmanager
def spark_session(**kwargs) -> SparkSession:
    """
    Create a Spark session as a context manager
    """
    spark = None
    try:
        spark = build_spark_session(**kwargs)
        yield spark
    finally:
        if spark is not None:
            spark.stop()
