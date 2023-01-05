"""
Run this from project root path

python bin/sparkapp.py sample_job --config_path=./configs/sample_job.yaml
"""

import yaml

from pyspark import SparkContext
from pyspark.rdd import RDD

from dps.spark.spark_session import spark_session
from dps.spark.utils.io_utils import read_line, to_json


def sample_job(config_path):
    with open(config_path) as f:
        conf = yaml.load(f, Loader=yaml.FullLoader)

    input_paths = ",".join([f'{conf["base_dir"]}/{t}' for t in conf["targets"]])

    with spark_session(
        f'sample_jsonl_size_{conf["sample_ratio"]}_seed_{conf["seed"]}'
    ) as spark:
        sc: SparkContext = spark.sparkContext

        proc_rdd: RDD = (
            sc.textFile(input_paths)
            .repartition(10)
            .flatMap(read_line)
            .sample(False, conf["sample_ratio"], conf["seed"])
        )

        proc_rdd.repartition(1).flatMap(to_json).saveAsTextFile(conf["output_dir"])
