import yaml
from pyspark import SparkContext
from pyspark.rdd import RDD

from dps.spark.spark_session import spark_session, spark_session_for_cluster
from dps.spark.utils.io_utils import read_line, to_json
from dps.spark.prep.lang_agnostic_prep import (
    bullet_ellipsis_filter,
    doc_len_filter,
    process_html_and_uri_text,
    remove_whitespace,
    replace_email_and_url,
    symbol_to_word_ratio_filter,
)
from dps.spark.prep.japanese_prep import (
    japanese_word_ratio_filter,
    japanese_bad_words_filter,
    japanese_mean_word_len_filter,
    #japanese_remove_repeated_text,
    japanese_symbol_to_word_ratio_filter,
    japanese_frequent_char_existence_filter,
    reduce_japanese_emoticon,
    many_separators_filter,
    remove_symbols,
)


def preprocess_text(text: str):
    processing_functions = [
        process_html_and_uri_text,
        remove_whitespace,
        replace_email_and_url,
        reduce_japanese_emoticon
        # japanese_remove_repeated_text,
    ]
    for _function in processing_functions:
        text = _function(text)

    if isinstance(text, str):
        processed_text = text
    else:
        processed_text = " ".join(text)
    return processed_text


def japanese_job(config_path: str):
    with open(config_path) as f:
        conf = yaml.load(f, Loader=yaml.FullLoader)
    use_column = conf["use_column"]

    input_paths = ",".join([f'{conf["base_dir"]}/{t}' for t in conf["targets"]])
    if conf["is_local"]:
        from dps.spark.spark_session import spark_session_local
        session_fn = spark_session_local
    else:
        session_fn = spark_session_for_cluster if conf["is_cluster"] else spark_session

    with session_fn("Japanse text processing job") as spark:
        sc: SparkContext = spark.sparkContext
        proc_rdd: RDD = (
            sc.textFile(input_paths)
            .repartition(conf["n_dist"])
            .flatMap(read_line)
            .filter(lambda x: japanese_bad_words_filter(x[use_column]))
            .filter(lambda x: doc_len_filter(x[use_column], conf["min_doc_len"], conf["max_doc_len"]))
            .filter(lambda x: japanese_mean_word_len_filter(x[use_column], conf["min_mean_word_len"], conf["max_mean_word_len"]))
            .filter(lambda x: japanese_symbol_to_word_ratio_filter(x[use_column], conf["symbol_to_word_ratio"]))
            .filter(lambda x: bullet_ellipsis_filter(x[use_column], conf["bullet_point_ratio"], conf["ellipsis_ratio"]))
            .filter(lambda x: japanese_word_ratio_filter(x[use_column], conf["japanese_word_ratio"]))
            .filter(lambda x: dict(text=preprocess_text(x[use_column])))
            .filter(lambda x: japanese_frequent_char_existence_filter(x[use_column], conf["freq_char_cnt"]))
            .filter(lambda x: reduce_japanese_emoticon(x[use_column]))
            .filter(lambda x: many_separators_filter(x[use_column], conf["separator_ratio"]))
            .filter(lambda x: remove_symbols(x[use_column]))
        )
        proc_rdd.repartition(conf["n_output"]).flatMap(to_json).saveAsTextFile(conf["output_dir"])


def exact_dedup_job(config_path: str):
    with open(config_path) as f:
        conf = yaml.load(f, Loader=yaml.FullLoader)
    use_column = conf["use_column"]

    input_paths = ",".join([f'{conf["base_dir"]}/{t}' for t in conf["targets"]])
    if conf["is_local"]:
        from dps.spark.spark_session import spark_session_local
        session_fn = spark_session_local
    else:
        session_fn = spark_session_for_cluster if conf["is_cluster"] else spark_session

    with session_fn("Exact Deduplication") as spark:
        sc: SparkContext = spark.sparkContext
        proc_rdd: RDD = (
            sc.textFile(input_paths)
            .repartition(conf["n_dist"])
            .flatMap(read_line)
            .map(lambda x: x[use_column])
            .distinct()
            .map(lambda x: dict(text=x))
        )
        proc_rdd.repartition(conf["n_output"]).flatMap(to_json).saveAsTextFile(conf["output_dir"])
