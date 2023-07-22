"""
Executable command-line script to launch Spark preprocessing jobs
"""

from pathlib import Path
from os import environ
import sys
import argparse

from typing import List

from dps.spark_df import VERSION
from dps.spark_df.process import process


def parse_args(args: List[str]) -> argparse.Namespace:
    """
    Read command-line options
    """
    parser = argparse.ArgumentParser(description=f"Launch a spark data preprocessing job (v. {VERSION})")
    parser.add_argument("config", help="YAML configuration file")
    parser.add_argument("--appname",
                        help="Name to use when launching the Spark job")
    parser.add_argument("--master",
                        help="Define a spark master (overriding config)")
    parser.add_argument("--reraise", action="store_true",
                        help="Reraise on exceptions")
    parser.add_argument("--verbose", type=int, default=1,
                        help="verbosity level")
    parser.add_argument("--max-docs", type=int,
                        help="maximum number of documents to process")
    return parser.parse_args(args)


def add_pyspark(spark_home: str):
    """
    Add the required Pyspark paths to Python path
    """
    print(f"** Using pyspark at: {spark_home}")
    spark_python = Path(spark_home) / "python"
    py4j = tuple((spark_python / "lib").glob("py4j-*-src.zip"))
    if not py4j:
        raise Exception(f"Error: cannot find a proper Spark dist at {spark_home}")
    sys.path += [str(p) for p in (spark_python, *py4j)]


def main(args: List[str] = None):
    """
    Entry point as a command line script
    """
    if args is None:
        args = sys.argv[1:]
    args = parse_args(args)

    try:
        # If SPARK_HOME is defined, add it to the Python path
        spark_home = environ.get("SPARK_HOME")
        if spark_home:
            add_pyspark(spark_home)

        # Process
        process(vars(args))
    except Exception as e:
        if args.reraise:
            raise
        print("ERROR:", e.__class__.__name__, ":", e)
        sys.exit(1)


if __name__ == '__main__':
    main()
