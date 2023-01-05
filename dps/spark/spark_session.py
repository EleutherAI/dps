from contextlib import contextmanager

from pyspark.sql import SparkSession


@contextmanager
def spark_session(appname="dps-process") -> SparkSession:
    spark = None
    try:
        spark = SparkSession.builder \
            .master('local[20]') \
            .appName(appname) \
            .config("spark.driver.memory", "4g") \
            .getOrCreate()
        yield spark
    finally:
        if spark is not None:
            spark.stop()

@contextmanager
def spark_session_for_cluster(appname="dps-process") -> SparkSession:
    spark = None
    try:
        spark = SparkSession.builder \
            .appName(appname) \
            .getOrCreate()
        yield spark
    finally:
        if spark is not None:
            spark.stop()
