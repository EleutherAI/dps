import yaml

from pyspark import SparkContext
from pyspark.rdd import RDD
from dps.spark.spark_session import spark_session, spark_session_for_cluster

from dps.spark.prep.lang_agnostic_prep import (
    doc_len_filter,
    bullet_ellipsis_filter,
    remove_whitespace,
    process_html_and_uri_text,
    replace_email_and_url,
    remove_repeated_text,
)
from dps.spark.prep.vietnamese_prep import normalize_vietnam, vietnamese_bad_words_filter, replace_vietnam_pii, reduce_emoticon
from dps.spark.utils.io_utils import read_line, to_json


def preprocess_text(input_text: str):
    processing_function_list = [
        process_html_and_uri_text,
        reduce_emoticon,
        remove_whitespace,
        replace_email_and_url,
        replace_vietnam_pii,
        remove_repeated_text,
        normalize_vietnam,
    ]

    for func in processing_function_list:
        input_text = func(input_text)

    if isinstance(input_text, str):
        processed_text = input_text
    else:
        processed_text = " ".join(input_text)

    return processed_text


def vietnamese_job(config_path: str):
    with open(config_path) as f:
        conf = yaml.load(f, Loader=yaml.FullLoader)

    input_paths = ",".join([f'{conf["base_dir"]}/{t}' for t in conf["targets"]])
    if conf["is_local"]:
        from dps.spark.spark_session import spark_session_local
        sessin_fn = spark_session_local
    else:
        session_fn = spark_session_for_cluster if conf["is_cluster"] else spark_session

    with session_fn("Japanse text processing job") as spark:
        sc: SparkContext = spark.sparkContext
        proc_rdd: RDD = (
            sc.textFile(input_paths)
            .repartition(conf["n_dist"])
            .flatMap(read_line)
            .filter(lambda x: vietnamese_bad_words_filter(x["text"]))
            .filter(lambda x: doc_len_filter(x["text"], conf["min_doc_len"], conf["max_doc_len"]))
            .filter(lambda x: bullet_ellipsis_filter(x["text"], conf["bullet_point_ratio"], conf["ellipsis_ratio"]))
            .filter(lambda x: dict(text=preprocess_text(x["text"])))
            .filter(lambda x: doc_len_filter(x["text"], conf["min_doc_len"], conf["max_doc_len"]))
        )
        proc_rdd.repartition(conf["n_output"]).flatMap(to_json).saveAsTextFile(conf["output_dir"])
