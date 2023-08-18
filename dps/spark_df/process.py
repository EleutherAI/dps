"""
Main process entry point for Spark processing jobs
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
    from dps.spark_df.dfprocessor import DfProcessor
    from dps.spark_df.udfprocessor import UdfProcessor

    verbose = args.pop("verbose", 1)
    max_docs = args.pop("max_docs", None)

    # Read configuration
    configname = Path(args.pop("config"))
    if not configname.is_file() and not configname.is_absolute():
        configname = Path(sys.prefix) / "etc" / "dps" / "df" / configname
    if verbose:
        print(f"** Loading config: {configname}", flush=True)
    with open(configname, encoding="utf-8") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    # Check we've got compulsory sections
    io = config.get("io", {})
    for f in ("source", "dest"):
        if f not in io:
            raise Exception(f"invalid config: missing io field {f}")
    if "process_df" not in config and "process_udf" not in config:
        raise Exception("invalid config: no processing defined")

    # Activate logging
    logcfg = config.get("logging") or {}
    logbasic = logcfg.get("logconfig")
    if logbasic:
        if logcfg.get("reset"):
            logging.basicConfig(filemode="w", **logbasic)
        else:
            logging.basicConfig(**logbasic)

    LOGGER = logging.getLogger(__name__)
    LOGGER.info("START")

    # Set the Python that will be run by Spark executors
    general = config.get("general")
    python = general.get("python") or Path(sys.prefix) / "bin" / "python"
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
            LOGGER.info("spark config =\n  %s",
                        "\n  ".join(sorted("=".join(v) for v in conf)))

        # Create the DataFrame processor
        dfproc = config.get("process_df", {})
        if dfproc:
            preproc_df = DfProcessor(dfproc, doc_column=config.get("doc_column"),
                                     logconfig=logbasic)

        # Create the UDF processor
        udfproc = config.get("process_udf") or config.get("process")
        if udfproc:
            preproc_udf = UdfProcessor(udfproc, seed=config.get("seed"),
                                       logconfig=logbasic)

        # Process all content
        for io in iolist:

            # [1] Read source data
            df = read_sources(spark, io["source"])
            if verbose:
                print(f"** Read: {df.count()} records", flush=True)
            if max_docs:
                df = df.limit(max_docs)
                if verbose:
                    print(f"** Truncated: {df.count()} records", flush=True)

            # [2] Repartition input, if needed
            cur_part = df.rdd.getNumPartitions()
            tgt_part = spark_config.get("partitions",
                                        spark.sparkContext.defaultParallelism)
            LOGGER.info("partitions: source=%d target=%d", cur_part, tgt_part)
            if cur_part != tgt_part:
                LOGGER.info("repartitioning=%d", tgt_part)
                df = df.repartition(tgt_part)

            # [3] Spark DataFrame processing, phase 1
            if "phase_1" in dfproc:
                df = preproc_df("phase_1", df)

            # [4] UDF processing
            if udfproc:
                schema_out = preproc_udf.schema(df.schema)
                LOGGER.info("output UDF schema =\n  %s", schema_out)
                df = df.mapInPandas(preproc_udf, schema_out)

            # [5] Spark DataFrame processing, phase 2
            if "phase_2" in dfproc:
                df = preproc_df("phase_2", df)

            # [6] Save results
            write_dataframe(df, io["dest"])
            if verbose:
                print(f"** Written: {df.count()} records", flush=True)
