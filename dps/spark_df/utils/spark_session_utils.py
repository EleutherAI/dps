"""
Some utilities to manage Spark sessions
"""
from contextlib import contextmanager

from typing import Dict

from pyspark import version as pyspark_version
from pyspark import SparkConf
from pyspark.sql import SparkSession

from . import logging

DEFAULT_MASTER = 'local[20]'
DEFAULT_EXECUTOR_CORES = 5
DEFAULT_EXECUTOR_MEMORY = "2g"
DEFAULT_SCALA_VERSION = "2.13"


def s3_config(sb: SparkSession.Builder, scala_version: str):
    """
    Take a spark configuration and customize it for efficient S3 access
    Taken from https://github.com/rom1504/cc2dataset/blob/main/cc2dataset/spark_session_builder.py
    """
    version = pyspark_version.__version__
    sb.config("spark.sql.shuffle.partitions", "4000")
    sb.config("spark.jars.packages",
              f"org.apache.hadoop:hadoop-aws:{version},org.apache.spark:spark-hadoop-cloud_{scala_version}:{version}")
    sb.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    # change to the appropriate auth method, see https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html
    sb.config("spark.hadoop.fs.s3a.aws.credentials.provider",
              "com.amazonaws.auth.InstanceProfileCredentialsProvider")
    # ton of options to try and make s3a run faster
    sb.config("spark.hadoop.fs.s3a.threads.max", "512")
    sb.config("spark.hadoop.fs.s3a.directory.marker.retention", "keep")
    sb.config("spark.hadoop.fs.s3a.max.total.tasks", "512")
    sb.config("spark.hadoop.fs.s3a.multipart.threshold", "5M")
    sb.config("spark.hadoop.fs.s3a.multipart.size", "5M")
    sb.config("spark.hadoop.fs.s3a.connection.maximum", "2048")
    sb.config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
    sb.config("spark.hadoop.fs.s3a.connection.timeout", "600000")
    sb.config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
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
    sb.config("spark.sql.hive.metastorePartitionPruning" "true")


def build_spark_session(appname: str = None, master: str = None,
                        config: Dict = None, s3: bool = False) -> SparkSession:
    """
    Create a Spark session
    """
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
    memory = int(memory)*(1000 if unit == 'g' else 1)
    memory_main = int(memory * 0.9)
    memory_overhead = memory - memory_main
    sb.config("spark.executor.memory", f"{memory_main}m")
    sb.config("spark.executor.memoryOverhead", f"{memory_overhead}m")
    log.info("Executor memory: %d %d", memory_main, memory_overhead)

    if s3:
        s3_config(sb, config.get("scala_version", DEFAULT_SCALA_VERSION))

    custom_conf = config.get("config", {})
    for k, v in custom_conf.items():
        sb.config(k, v)

    # Shut down some internal loggers
    logging.getLogger("py4j").setLevel(logging.WARNING)
    logging.getLogger("pyspark").setLevel(logging.WARNING)

    # Create the SparkSession
    spark = sb.getOrCreate()

    # Adjust logging
    loglevel = config.get("logging", {}).get("level")
    if loglevel:
        spark.sparkContext.setLogLevel(loglevel)

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
