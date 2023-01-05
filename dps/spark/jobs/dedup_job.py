"""
Run this from project root path

python bin/sparkapp.py dedup_job --config_path=./configs/dedup_job.yaml
"""

import random
from itertools import combinations

import yaml
from pyspark import SparkContext
from pyspark.rdd import RDD

from dps.spark.spark_session import spark_session
from dps.spark.utils.io_utils import read_line, to_json
from dps.spark.prep.dedup_prep import (
    shingle_word,
    generate_minhash,
    jaccard_by_hashvalues,
)


def expand_instances_by_minhash(
    data, expand_size: int, n_gram: int, seed: int = 1, char_level: bool = False
):
    shingles = shingle_word(data["text"], n_gram=n_gram, char_level=char_level)
    minhashes = generate_minhash(shingles, num_perm=expand_size, seed=seed)

    for mh in minhashes.tolist():
        yield (str(mh), [dict(**data, shingles=shingles, hashvalues=minhashes)])


def explore_dedup_instance(hash_groups, threshold: float = 0.8):
    if len(hash_groups) <= 1:
        return

    group_represent_text = hash_groups[0][
        "text"
    ]  # not to remove all text instances in group.
    pairs = combinations(hash_groups, 2)

    for d_1, d_2 in pairs:
        sim_score = jaccard_by_hashvalues(d_1["hashvalues"], d_2["hashvalues"])
        if sim_score >= threshold:
            dedup_text = [d_1["text"], d_2["text"]]
            if group_represent_text in dedup_text:
                yield dedup_text[0] if dedup_text[
                    0
                ] != group_represent_text else dedup_text[1]
            else:
                yield random.choice(dedup_text)


def dedup_job(config_path):
    with open(config_path) as f:
        conf = yaml.load(f, Loader=yaml.FullLoader)

    input_paths = ",".join([f'{conf["base_dir"]}/{t}' for t in conf["targets"]])

    with spark_session(f"") as spark:
        sc: SparkContext = spark.sparkContext

        proc_rdd: RDD = (
            sc.textFile(input_paths)
            .repartition(conf["n_dist"])
            .flatMap(read_line)
            .cache()
        )

        overlap_kv_rdd: RDD = (
            proc_rdd.flatMap(
                lambda x: expand_instances_by_minhash(
                    x,
                    expand_size=conf["num_expand"],
                    n_gram=conf["n_gram"],
                    seed=conf["seed"],
                    char_level=conf["char_level"],
                )
            )
            .reduceByKey(lambda x, y: x + y)
            .flatMap(
                lambda x: explore_dedup_instance(x[1], threshold=conf["sim_threshold"])
            )
            .distinct()
            .map(lambda x: (x, dict(text=x)))
            .cache()
        )

        proc_rdd.map(lambda x: (x["text"], x)).subtractByKey(overlap_kv_rdd).map(
            lambda x: x[1]
        ).repartition(conf["n_output"]).flatMap(to_json).saveAsTextFile(
            conf["output_dir"]
        )
