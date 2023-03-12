"""
Executable command-line script to launch Spark preprocessing jobs
"""

from pathlib import Path
from os import environ
import sys
import argparse

import yaml


from typing import List, Dict

import dps.spark_df.utils.logging as logging
from dps.spark_df.utils.spark_session_utils import spark_session
from dps.spark_df.utils.io import read_sources, write_dataframe
from dps.spark_df.preproc import UdfPreprocessor


LOGGER = None



def parse_args(args: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Launch a spark data preprocessing job")
    parser.add_argument("config", help="YAML configuration file")
    parser.add_argument("--appname",
                        help="Name to use when launching the Spark job")
    parser.add_argument("--master",
                        help="Define a spark master (overriding config)")
    return parser.parse_args(args)


def process(args: Dict):
    """
    Main processing code
    """
    global LOGGER

    # Read configuration
    configname = args.pop("config")
    with open(configname, encoding="utf-8") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    for r in "source", "dest", "preprocess":
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

    is_s3 = config["source"]["base"].startswith("s3a://")
    opt = {k: v for k, v in args.items() if k in ("appname", "master")}
    spark_config = config.get("spark") or {}

    with spark_session(config=spark_config, s3=is_s3, **opt) as spark:

        if LOGGER.getEffectiveLevel() >= logging.INFO:
            conf = spark.sparkContext.getConf().getAll()
            LOGGER.info("Spark config:\n  %s",
                        "\n  ".join(sorted("=".join(v) for v in conf)))

        # Read source data
        df1 = read_sources(spark, config["source"])
        print(f"Read: {df1.count()} records")

        # Repartition, if needed
        cur_part = df1.rdd.getNumPartitions()
        tgt_part = spark_config.get("partitions", spark.sparkContext.defaultParallelism)
        LOGGER.info("partitions: source=%d target=%d", cur_part, tgt_part)
        if cur_part != tgt_part:
            LOGGER.info("repartitioning to %d", tgt_part)
            df1 = df1.repartition(tgt_part)

        # Create the preprocessor UDF
        preproc = UdfPreprocessor(config["preprocess"], seed=config.get("seed"),
                                  logconfig=logbasic)

        # Send for execution
        schema_out = preproc.schema(df1.schema)
        LOGGER.info("Output schema:\n  %s", schema_out)
        df2 = df1.mapInPandas(preproc, schema_out)

        # Save results
        write_dataframe(df2, config["dest"])
        print(f"Written: {df2.count()} records")


def main(args: List[str] = None):
    """
    Entry point as a command line script
    """
    if args is None:
        args = sys.argv[1:]
    args = parse_args(args)
    process(vars(args))


if __name__ == '__main__':
    main()
