"""
Utilities for reading/writing DataFrames
"""


from pathlib import Path

from typing import Dict

from pyspark.sql import SparkSession, DataFrame

from . import logging

LOGGER = None


def read_sources(spark: SparkSession, source: Dict):
    """
    Read data sources
    """
    global LOGGER
    if LOGGER is None:
        LOGGER = logging.getLogger(__name__)

    try:
        # Source format
        fmt = source["format"]
        if fmt == "jsonl":
            fmt = "json"
        # Source files
        if 'paths' not in source:
            sources = source['base']
        else:
            sources = [source['base'] + p for p in source['paths']]
    except KeyError as e:
        raise Exception(f"invalid source config: missing field: {e}")
    options = source.get('options') or {}
    LOGGER.info("Sources: %s (%s)", sources, options)
    return spark.read.format(fmt).load(sources, **options)


def write_dataframe(df: DataFrame, dest: Dict):
    """
    Save a dataframe
    """
    global LOGGER
    if LOGGER is None:
        LOGGER = logging.getLogger(__name__)

    try:
        fmt = dest["format"]
        outname = dest["name"]
    except KeyError as e:
        raise Exception(f"invalid destination config: missing {e}")

    LOGGER.info("outname = %s", outname)

    outopts = dest.get("options") or {}
    mode = dest.get("mode", "errorifexists")
    if fmt == "jsonl":
        fmt = "json"

    num_part = dest.get("partitions")
    if num_part:
        LOGGER.info("repartitioning = %d", num_part)
        df = df.repartition(num_part)

    df.write.format(fmt).mode(mode).options(**outopts).save(outname)
