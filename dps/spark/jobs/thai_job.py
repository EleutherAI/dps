'''
Run this from project root path

python bin/sparkapp.py thai_job --config_path=./configs/thai_job.yaml
'''

import yaml
from pyspark import SparkContext
from pyspark.rdd import RDD

from dps.spark.prep.thai_prep import (
    thai_word_ratio_filter,
    thai_bad_words_filter,
    thai_frequent_char_existence_filter
)

from dps.spark.prep.lang_agnostic_prep import (
    doc_len_filter,
    mean_word_len_filter,
    symbol_to_word_ratio_filter,
    bullet_ellipsis_filter,
    remove_whitespace,
    process_html_and_uri_text,
    replace_email_and_url,
    remove_repeated_text,
)

from dps.spark.spark_session import spark_session, spark_session_for_cluster
from dps.spark.utils.io_utils import read_line, to_json

def preprocess_text (input_text: str):
    processing_function_list = [
        process_html_and_uri_text,
        remove_whitespace,
        replace_email_and_url,
        remove_repeated_text,
    ]
    
    for func in processing_function_list:
        input_text = func(input_text)
    
    if isinstance(input_text, str):
        processed_text = input_text
    else:
        processed_text = " ".join(input_text)
        
    return processed_text

def thai_job(config_path):
    with open(config_path) as f:
        conf = yaml.load(f, Loader=yaml.FullLoader)
    
    input_paths = ",".join([f'{conf["base_dir"]}/{t}' for t in conf["targets"]])
    session_fn = spark_session_for_cluster if conf["is_cluster"] else spark_session
    
    with session_fn("Thai text processing job") as spark:
        sc: SparkContext = spark.sparkContext
        proc_rdd: RDD = (
            sc.textFile(input_paths)
            .repartition(conf["n_dist"])
            .flatMap(read_line)
            .filter(lambda x: thai_bad_words_filter(x["text"]))
            .filter(lambda x: doc_len_filter(x["text"], conf["min_doc_len"], conf["max_doc_len"]))
            .filter(lambda x: mean_word_len_filter(x["text"], conf["min_mean_word_len"], conf["max_mean_word_len"]))
            .filter(lambda x: symbol_to_word_ratio_filter(x["text"], conf["symbol_to_word_ratio"]))
            .filter(lambda x: bullet_ellipsis_filter(x["text"], conf["bullet_point_ratio"], conf["ellipsis_ratio"]))
            .filter(lambda x: thai_word_ratio_filter(x["text"], conf["thai_word_ratio"]))
            .filter(lambda x: dict(text=preprocess_text(x["text"])))
            .filter(lambda x: doc_len_filter(x["text"], conf["min_doc_len"], conf["max_doc_len"]))
            .filter(lambda x: thai_frequent_char_existence_filter(x["text"], conf["frequent_char_ratio"]))
        )
        proc_rdd.repartition(conf["n_output"]).flatMap(to_json).saveAsTextFile(conf["output_dir"])