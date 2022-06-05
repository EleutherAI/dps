import yaml

from pyspark import SparkContext
from pyspark.rdd import RDD

from ..spark_session import spark_session, spark_session_for_cluster
from ..utils.io import read_line, to_json


def sample_jsonl(input_dir: str, output_dir: str, n_dist: int=10, n_output: int=10, 
                 ratio: float=0.1, seed: int=1234, is_cluster=False):
    if (not is_cluster) and (not os.path.isdir(input_dir)):
        raise ValueError('input_dir is not directory path')

    session_fn = spark_session_for_cluster if is_cluster else spark_session

    with session_fn(f'sample_jsonl_size_{ratio}_seed_{seed}') as spark:
        sc: SparkContext = spark.sparkContext
        proc_rdd: RDD = sc.textFile(input_dir) \
            .repartition(n_dist) \
            .flatMap(read_line) \
            .sample(False, ratio, seed)
            
        proc_rdd \
            .repartition(n_output) \
            .flatMap(to_json) \
            .saveAsTextFile(output_dir)

