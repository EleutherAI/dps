import os
from pyspark.rdd import RDD
from pyspark.sql import DataFrame

from ..spark_session import spark_session
from ..utils.common_preprocess import (reduce_emoticon, 
                                       replace_rrn, 
                                       remove_whitespace, 
                                       replace_phone_number, 
                                       remove_html_tags)
from ..utils.io import to_json
from functools import reduce
from pyspark.sql.functions import col


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
        proc_df: DataFrame = spark.read.json(input_dir).repartition(n_dist)
        columns = set(["text"])
        filtered_null = map(lambda x: (col(x).isNotNull()) & (col(x) != ""), columns)
        processed_null = reduce((lambda x, y: x & y), filtered_null)

        proc_df = proc_df.select("text") \
            .distinct() \
            .filter(processed_null)
            
        # convert to RDD and preprocess
        proc_rdd: RDD = proc_df.rdd \
            .repartition(n_dist) \
            .map(lambda x: dict(text=preprocess_text(x["text"])))
            
        proc_rdd.flatMap(to_json).repartition(n_output).saveAsTextFile(output_dir)
