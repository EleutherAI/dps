import yaml

from pyspark import SparkContext
from pyspark.rdd import RDD

from ..spark_session import spark_session
from ..utils.io import read_line, to_json

import json
from glob import glob


def build_korean_modu_news_paper_data(dir_path, save_path):

    def read_files(data):
        documents = data['document']

        for doc in documents:
            yield dict(text='\n'.join([p['form'] for p in doc['paragraph']]), 
                       meta=data['metadata'])

    with spark_session(f'build_news_paper_data') as spark:
        sc: SparkContext = spark.sparkContext

        proc_rdd: RDD = sc.wholeTextFiles(dir_path).values().map(json.loads) \
                         .flatMap(read_files) \
                         .repartition(10) \
            
        proc_rdd \
            .repartition(20) \
            .flatMap(to_json) \
            .saveAsTextFile(save_path)
