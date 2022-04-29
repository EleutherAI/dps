import yaml
import json
from pyspark import SparkContext
from pyspark.sql import DataFrame

from ..spark_session import spark_session
from ..utils.text_normalize import *
from pathlib import Path


def sample_jsonl(config_path):
    with open(config_path) as f:
        conf = yaml.load(f, Loader=yaml.FullLoader)
    
    input_paths = ','.join([f'{conf["base_dir"]}/{t}' for t in conf["targets"]])

    with spark_session(f'sample_jsonl_size_{conf["sample_ratio"]}_seed_{conf["seed"]}') as spark:    
        df: DataFrame = spark.read.json(input_paths).repartition(10)
        df_json = df.distinct().repartition(1)

        preprocess_functions = [reduce_emoticon, replace_phone_number, replace_rrn, remove_whitespace, strip_html_tags]
        df_json = df_json.toJSON().map(lambda x: json.loads(x)).collect()
        
        try:
            with open('./datasets/test_output_data/sample.jsonl', 'w') as out_f:
                for file in df_json:
                    # import pdb; pdb.set_trace()
                    file['text'] = preprocess_text(file['text'], preprocess_functions)
                    json.dump(file, out_f, ensure_ascii=False)
                    out_f.write('\n')
        except:
            Path('./datasets/test_output_data/sample.jsonl').touch()
            with open('./datasets/test_output_data/sample.jsonl', 'w') as out_f:
                for file in df_json:
                    json.dump(file, out_f, ensure_ascii=False)
                    out_f.write('\n')
