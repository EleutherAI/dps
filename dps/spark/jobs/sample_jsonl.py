import yaml
import json
from pyspark import SparkContext
from pyspark.rdd import RDD
from pyspark.sql import DataFrame

from ..spark_session import spark_session
from ..utils.io import read_line, to_json


def sample_jsonl(config_path):
    with open(config_path) as f:
        conf = yaml.load(f, Loader=yaml.FullLoader)
    
    input_paths = ','.join([f'{conf["base_dir"]}/{t}' for t in conf["targets"]])

    with spark_session(f'sample_jsonl_size_{conf["sample_ratio"]}_seed_{conf["seed"]}') as spark:    
        df: DataFrame = spark.read.json(input_paths).repartition(10)
        df_json = df.distinct().repartition(1)
        # df_json = df_json.toJSON().map(lambda x: json.loads(x)).collect()
        # print(type(df_json))
        # with open('./datasets/test_output_data', 'w') as f:
        # json.dumps(df_json, indent=4).write.option("multiLine", "true").json('./datasets/test_output_data')

        df_json.write.option("delimiter", "\n").json('./datasets/test_output_data')
