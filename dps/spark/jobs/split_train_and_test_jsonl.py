from random import seed, random

from pyspark import SparkContext
from pyspark.rdd import RDD

from ..spark_session import spark_session
from ..utils.io import read_line, to_json


def split_jsonl(input_path, test_ratio=0.1, seed=1234):
    SPLIT_AS = 'split_as'
    seed(seed)
    
    def tag_train_or_val(data):
        if random() < test_ratio:
            data[SPLIT_AS] = 'val'
        else:
            data[SPLIT_AS] = 'train'

        return data

    with spark_session(f'split_train_and_test_ratio_{test_ratio}_seed_{seed}') as spark:
        sc: SparkContext = spark.sparkContext

        proc_rdd: RDD = sc.textFile(input_path) \
            .repartition(10) \
            .flatMap(read_line) \
            .map(tag_train_or_val)

        train_rdd: RDD = proc_rdd.filter(lambda x: x['split_as'] == 'train') \
            .map(lambda x: {k: v for k, v in x.items() if k != SPLIT_AS})
            
        val_rdd: RDD = proc_rdd.filter(lambda x: x['split_as'] == 'val') \
            .map(lambda x: {k: v for k, v in x.items() if k != SPLIT_AS})

        train_rdd \
            .repartition(10) \
            .flatMap(to_json) \
            .saveAsTextFile(f'{input_path}_train')
        
        val_rdd \
            .repartition(10) \
            .flatMap(to_json) \
            .saveAsTextFile(f'{input_path}_val')
