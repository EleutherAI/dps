import os

from pyspark import SparkContext
from pyspark.rdd import RDD
from pyspark.sql import DataFrame


from ..spark_session import spark_session
from ..utils.common_preprocess import (reduce_emoticon, 
                                       replace_rrn, 
                                       remove_whitespace, 
                                       replace_phone_number, 
                                       remove_html_tags)
from ..utils.io import to_json


def preprocess_text(input_text: str):

    processing_function_list = [reduce_emoticon,
                                replace_phone_number,
                                replace_rrn,                                    
                                remove_whitespace,
                                remove_html_tags]
    
    for func in processing_function_list:
        input_text = func(input_text)

    if isinstance(input_text, str):
        processed_text = input_text
    else:
        processed_text = ' '.join(input_text)
        
    return processed_text


def preprocess(input_dir: str, output_dir: str, n_dist: int=10, n_output: int=10):
    if not os.path.isdir(input_dir):
        raise ValueError('input_dir is not directory path')
    
    with spark_session(f'common_preprocess_jsonl_{input_dir}') as spark:
        sc: SparkContext = spark.sparkContext
        proc_df: DataFrame = spark.read.json(input_dir).repartition(n_dist) \
            .select("text") \
            .distinct()
        
        # convert to RDD and preprocess
        proc_rdd: RDD = proc_df.rdd \
            .repartition(n_dist) \
            .map(lambda x: dict(text=preprocess_text(x["text"])))
            
        proc_rdd.flatMap(to_json).repartition(n_output).saveAsTextFile(output_dir)
