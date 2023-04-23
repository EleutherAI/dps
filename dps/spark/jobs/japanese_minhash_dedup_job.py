"""inspired from `depup_job_romance_minhash.py `"""
import argparse

from pyspark.ml import Pipeline
from pyspark.ml.feature import HashingTF, IDF, MinHashLSH
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col

import sparknlp
from sparknlp.annotator import WordSegmenterModel
from sparknlp.base import DocumentAssembler


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
    # arguments
    threshold = 0.8

    spark = sparknlp.start()
    print("Spark NLP version: ", sparknlp.version())
    print("Apache Spark version: ", spark.version)

    example = spark.createDataFrame([
        ['清代は湖北省が置かれ、そのまま現代の行政区分になっている。'],
        ['清代は湖北省が置かれ、そのまま現在の行政区分になっている。'],
        ['清代は湖北省が置かれたが、それがそのまま現在の行政区分になっているというわけではない。'],
        ['データブリックスは、学術界とオープンソースコミュニティをルーツとするデータサイエンス＋AIの企業です。'],
        ['データブリックスは、学術界とオープンソースコミュニティをルーツとするデータ＋AIの企業です。'],
        ['データブリックスは、学術界とオープンソースコミュニティをルーツとするデータの企業です。'],
        ['ジョンスノーラボからこんにちは！ ']
    ], ["text"])

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
    
    print(deduplicated_df.toPandas().loc[:, ["id", "text"]])


if __name__ == "__main__":
    main()