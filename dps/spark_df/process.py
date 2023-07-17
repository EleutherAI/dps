"""
Processing function for Spark preprocessing jobs
"""

from pathlib import Path
from os import environ
import sys

import yaml

from typing import Dict

import dps.spark_df.utils.logging as logging


def process(args: Dict):
    """
    Main processing function
    """
    # Import the main processing code, now that the path should contain Spark
    from dps.spark_df.utils.spark_session_utils import spark_session
    from dps.spark_df.utils.io import read_sources, write_dataframe
    from dps.spark_df.preproc import UdfPreprocessor

    verbose = args.pop("verbose", 1)

    # Read configuration
    configname = Path(args.pop("config"))
    if not configname.is_file() and not configname.is_absolute():
        configname = Path(sys.prefix) / "etc" / "dps" / "df" / configname
    if verbose:
        print(f"** Loading config: {configname}", flush=True)
    with open(configname, encoding="utf-8") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    for r in "io", "preprocess":
        if r not in config:
            raise Exception(f"invalid config: missing field {r}")

    # Activate logging
    logcfg = config.get("logging")
    logbasic = logcfg.get("logconfig")
    if logbasic:
        if logcfg.get("reset"):
            logging.basicConfig(filemode="w", **logbasic)
        else:
            logging.basicConfig(**logbasic)

    LOGGER = logging.getLogger(__name__)
    LOGGER.info("START")

    # Set the Python that will be run by Spark executors
    python = config.get("python") or Path(sys.prefix) / "bin" / "python"
    environ["PYSPARK_PYTHON"] = str(python)

    # Check input & output
    is_s3 = False
    iolist = config["io"]
    if isinstance(iolist, dict):
        iolist = [iolist]
    for n, io in enumerate(iolist, start=1):
        if "source" not in io or "dest" not in io:
            raise Exception(f"invalid config: invalid io entry #{n}")
        if io["source"]["base"].startswith("s3a://"):
            is_s3 = True

    # Get the configuration for Spark
    spark_config = config.get("spark") or {}
    spark_opt = {k: v for k, v in args.items() if k in ("appname", "master")}

    # Create a Spark session
    with spark_session(config=spark_config, s3=is_s3, **spark_opt) as spark:

        if LOGGER.getEffectiveLevel() >= logging.INFO:
            conf = spark.sparkContext.getConf().getAll()
            LOGGER.info("Spark config:\n  %s",
                        "\n  ".join(sorted("=".join(v) for v in conf)))

        # Create the preprocessor UDF
        preproc = UdfPreprocessor(config["preprocess"],
                                  seed=config.get("seed"), logconfig=logbasic)

        # Process all content
        for io in iolist:

            # Read source data
            df1 = read_sources(spark, io["source"])
            if verbose:
                print(f"** Read: {df1.count()} records", flush=True)

            # Repartition input, if needed
            cur_part = df1.rdd.getNumPartitions()
            tgt_part = spark_config.get("partitions",
                                        spark.sparkContext.defaultParallelism)
            LOGGER.info("partitions: source=%d target=%d", cur_part, tgt_part)
            if cur_part != tgt_part:
                LOGGER.info("repartitioning to %d", tgt_part)
                df1 = df1.repartition(tgt_part)

            # Send for execution
            schema_out = preproc.schema(df1.schema)
            LOGGER.info("Output schema:\n  %s", schema_out)
            df2 = df1.mapInPandas(preproc, schema_out)

            # Save results
            write_dataframe(df2, io["dest"])
            if verbose:
                print(f"** Written: {df2.count()} records", flush=True)
