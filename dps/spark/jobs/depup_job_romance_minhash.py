import argparse
from pyspark.sql import SparkSession, DataFrame
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, MinHashLSH
from pyspark.ml import Pipeline
from pyspark.sql.functions import col


def create_spark_session() -> SparkSession:
    return SparkSession.builder \
        .appName("DeduplicationJob") \
        .getOrCreate()


def read_dataset(spark: SparkSession, input_path: str) -> DataFrame:
    # Read the dataset as a DataFrame
    return spark.read.text(input_path).withColumnRenamed("value", "text")


def create_feature_pipeline() -> Pipeline:
    # Configure the feature extraction pipeline
    tokenizer = Tokenizer(inputCol="text", outputCol="tokens")

    # do not worry too much about space as the features are stored in a ml.linalg.SparseVector
    # https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.feature.HashingTF.html
    # So I'm taking a very high number here to avoid collisions 2**14

    # beware: Since a simple modulo is used to transform the hash function to a column index,
    # it is advisable to use a power of two as the numFeatures parameter;
    # otherwise the features will not be mapped evenly to the columns.
    hashingtf = HashingTF(inputCol="tokens", outputCol="features", numFeatures=2 ** 14)

    idf = IDF(inputCol="features", outputCol="idf_features")
    return Pipeline(stages=[tokenizer, hashingtf, idf])


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Deduplicate text files using MinHash LSH.")
    parser.add_argument("input_path", help="Path to the input text files.")
    parser.add_argument("output_path", help="Path to the output directory.")
    # default threshold is 0.8 because it is the usual and seems reasonable
    parser.add_argument("-t", "--threshold", type=float, default=0.8, help="Jaccard distance threshold (default: 0.8).")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    spark = create_spark_session()
    df = read_dataset(spark, args.input_path)
    deduplicated_df = deduplicate_dataset(df, args.threshold)
    # Save the deduplicated data to the output directory
    deduplicated_df.write.text(args.output_path)
    spark.stop()


if __name__ == "__main__":
    main()