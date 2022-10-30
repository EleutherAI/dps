import os

from pyspark import SparkContext
from pyspark.rdd import RDD

from ..spark_session import spark_session, spark_session_for_cluster
from ..utils.io import read_line, to_json
from ..utils.massivetext_filter import (doc_len_filter, 
                                        mean_word_len_filter, 
                                        symbol_to_word_ratio_filter, 
                                        bullet_ellipsis_filter, 
                                        alphabetic_word_ratio_filter)


def massivetext_filter_jsonl(input_dir: str, output_dir: str,
                             n_dist: int=10, n_output: int=10, is_cluster=False,
                             min_doc_len: int=50, max_doc_len: int=100000,
                             min_mean_word_len: int=3, max_mean_word_len: int=10,
                             symbol_to_word_ratio: float=0.1,
                             bullet_point_ratio: float=0.9, ellipsis_ratio: float=0.3,
                             alphabetic_word_ratio: float=0.8):

    if (not is_cluster) and (not os.path.isdir(input_dir)):
        raise ValueError('input_dir is not directory path')
    session_fn = spark_session_for_cluster if is_cluster else spark_session

    with session_fn("massivetext quality filter") as spark:
        sc: SparkContext = spark.sparkContext
        proc_rdd: RDD = sc.textFile(input_dir) \
            .repartition(n_dist) \
            .flatMap(read_line) \
            .filter(lambda x: doc_len_filter(x['text'], min_doc_len, max_doc_len)) \
            .filter(lambda x: mean_word_len_filter(x['text'], min_mean_word_len, max_mean_word_len)) \
            .filter(lambda x: symbol_to_word_ratio_filter(x['text'], symbol_to_word_ratio)) \
            .filter(lambda x: bullet_ellipsis_filter(x['text'], bullet_point_ratio, ellipsis_ratio)) \
            .filter(lambda x: alphabetic_word_ratio_filter(x['text'], alphabetic_word_ratio)) \

        proc_rdd.repartition(n_output) \
            .flatMap(to_json) \
            .saveAsTextFile(output_dir)
