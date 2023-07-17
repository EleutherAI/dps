"""inspired from `depup_job_romance_minhash.py `"""
import argparse

import yaml

from pyspark import SparkContext
from pyspark.ml import Pipeline
from pyspark.ml.feature import HashingTF, IDF, MinHashLSH
from pyspark.rdd import RDD
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col

import sparknlp
from sparknlp.annotator import WordSegmenterModel
from sparknlp.base import DocumentAssembler

from dps.spark.utils.io_utils import read_line, to_json


def create_feature_pipeline() -> Pipeline:
    hashing_tf = HashingTF(inputCol="result", outputCol="features", numFeatures=2**14)
    idf = IDF(inputCol="features", outputCol="idf_features")
    return Pipeline(stages=[hashing_tf, idf])


def create_minhash_lsh() -> MinHashLSH:
    return MinHashLSH(inputCol="idf_features", outputCol="hashes", numHashTables=5)


def deduplicate_dataset(df: DataFrame, threshold: float) -> DataFrame:
    # Fit and transform the DataFrame
    pipeline = create_feature_pipeline()
    model = pipeline.fit(df)
    transformed_df = model.transform(df)

    # Configure and fit the MinHashLSH model
    minhash_lsh = create_minhash_lsh()
    minhash_lsh_model = minhash_lsh.fit(transformed_df)

    # Perform self-join and filter based on the Jaccard distance threshold
    similar_pairs = minhash_lsh_model.approxSimilarityJoin(transformed_df, transformed_df, threshold)
    filtered_pairs = similar_pairs.filter("datasetA.id < datasetB.id")
    # Deduplicate the dataset
    deduplicated_ids = filtered_pairs.select(col("datasetA.id").alias("id")).distinct()
    deduplicated_df = df.join(deduplicated_ids, "id", "leftanti")
    return deduplicated_df


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config_path", type=str, required=True)
    args, _ = parser.parse_known_args()
    config_path = args.config_path

    with open(config_path) as f:
        conf = yaml.load(f, Loader=yaml.FullLoader)
    threshold = conf['threshold']
    input_paths = ",".join([f"{conf['base_dir']}/{t}" for t in conf['targets']])

    spark = sparknlp.start()
    print("Spark NLP version: ", sparknlp.version())
    print("Apache Spark version: ", spark.version)

    sc: SparkContext = spark.sparkContext
    proc_rdd: RDD = (
        sc.textFile(input_paths)
        .repartition(conf['n_dist'])
        .flatMap(read_line)
        .cache()
    )
    example = proc_rdd.toDF(['text'])

    # Word segmenter or Tokenizer
    document_assembler = DocumentAssembler()\
        .setInputCol("text")\
        .setOutputCol("document")

    word_segmenter = WordSegmenterModel.pretrained("wordseg_gsd_ud", "ja")\
        .setInputCols("document")\
        .setOutputCol("token")

    pipeline = Pipeline(stages=[
        document_assembler,
        word_segmenter,
    ])
    ws_model = pipeline.fit(spark.createDataFrame([[""]]).toDF("text"))
    segmented_df = ws_model.transform(example)

    # deduplicationn
    df = segmented_df.select(["text", "token.result"])
    df = df.withColumn("id", F.monotonically_increasing_id())

    feature_pipeline = create_feature_pipeline()
    model = feature_pipeline.fit(df)
    transformed_df = model.transform(df)

    minhash_lsh = MinHashLSH(inputCol="idf_features", outputCol="hashes", numHashTables=5)
    minhash_lsh_model = minhash_lsh.fit(transformed_df)
    similar_pairs = minhash_lsh_model.approxSimilarityJoin(
        transformed_df, transformed_df, threshold)
    filtered_pairs = similar_pairs.filter("datasetA.id < datasetB.id")

    deduplicated_ids = filtered_pairs.select(F.col("datasetA.id").alias("id")).distinct()
    deduplicated_df = df.join(deduplicated_ids, "id", "leftanti")
    
    deduplicated_df.rdd\
        .map(lambda x: dict(text=x[1]))\
        .repartition(conf['n_output']).flatMap(to_json).saveAsTextFile(conf['output_dir'])


if __name__ == "__main__":
    main()