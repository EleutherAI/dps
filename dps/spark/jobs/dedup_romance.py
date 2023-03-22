import os

from pyspark.ml.feature import BucketedRandomProjectionLSH
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
import argparse


def deduplicate_dataset(dataset: DataFrame,
                        input_col: str,
                        output_path: str,
                        num_hash_tables: int = 5,
                        bucket_length: float = 10.0,
                        jaccard_threshold: float = 0.6) -> DataFrame:
    # Add a unique ID to each record
    dataset = dataset.withColumn("id", monotonically_increasing_id())

    # Create the LSH model
    brp: BucketedRandomProjectionLSH = BucketedRandomProjectionLSH(inputCol=input_col,
                                                                   outputCol="hashes",
                                                                   numHashTables=num_hash_tables,
                                                                   bucketLength=bucket_length)
    model = brp.fit(dataset)

    # Apply the LSH model to the dataset
    hashed: DataFrame = model.transform(dataset)
    # Find similar records
    similar_pairs: DataFrame = model.approxSimilarityJoin(hashed, hashed, jaccard_threshold, distCol="JaccardDistance")
    # Remove self-matches
    similar_pairs: DataFrame = similar_pairs.filter("datasetA.id < datasetB.id")
    # Select only the matched pairs
    matches: DataFrame = similar_pairs.select("datasetA.id", "datasetB.id")
    # Remove duplicates
    deduplicated: DataFrame = dataset.join(matches, "dataset.id == matches.id", "left_anti")
    # deduplicated: DataFrame =  dataset.join(matches, (dataset.id == matches.id) | (dataset.id == matches.id), "left_anti")

    # Save the deduplicated dataset
    deduplicated.write.csv(output_path, header=True)
    return deduplicated


def deduplicate_files_in_folder(input_folder: str,
                                output_folder: str,
                                input_col: str,
                                num_hash_tables: int = 5,
                                bucket_length: float = 10.0,
                                jaccard_threshold: float = 0.6) -> list[DataFrame]:
    spark = SparkSession.builder.appName("deduplication").getOrCreate()
    deduplicated_files = []
    # just checking the files inside the folder, not the subfolders because this is a long task, to include the subfolders is trivial
    for root, dirs, files in os.walk(input_folder):
        for file in files:
            input_path = os.path.join(root, file)
            if input_path.endswith(".csv"):
                dataset = spark.read.csv(input_path, header=True, inferSchema=True)
                deduplicated = deduplicate_dataset(dataset,
                                                   input_col,
                                                   os.path.join(output_folder, file),
                                                   num_hash_tables,
                                                   bucket_length,
                                                   jaccard_threshold)
                deduplicated_files.append({"file_name": input_path, "deduplicated_df": deduplicated})
    return deduplicated_files


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Deduplicate CSV files using LSH.")
    parser.add_argument("--input_folder", type=str, required=True, help="Path to the input folder containing CSV files.")
    parser.add_argument("--output_folder", type=str, required=True, help="Path to the output folder where deduplicated CSV files will be saved.")
    parser.add_argument("--input_col", type=str, required=True, help="The column name used for deduplication.")
    parser.add_argument("--num_hash_tables", type=int, default=5, help="Number of hash tables used for LSH (default: 5).")
    parser.add_argument("--bucket_length", type=float, default=10.0, help="Bucket length used for LSH (default: 10.0).")
    parser.add_argument("--jaccard_threshold", type=float, default=0.6, help="Jaccard similarity threshold for considering records as duplicates (default: 0.6).")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    spark = SparkSession.builder.appName("deduplication_romance_job").getOrCreate()

    deduplicated_files = deduplicate_files_in_folder(input_folder=args.input_folder,
                                                     output_folder=args.output_folder,
                                                     input_col=args.input_col,
                                                     num_hash_tables=args.num_hash_tables,
                                                     bucket_length=args.bucket_length,
                                                     jaccard_threshold=args.jaccard_threshold)
      
    # write the deduplicated files
    for deduplicated_file in deduplicated_files:
        deduplicated_file["deduplicated_df"].write.csv(os.path.join(args.output_folder, deduplicated_file["file_name"]), header=True)
        
    spark.stop()
