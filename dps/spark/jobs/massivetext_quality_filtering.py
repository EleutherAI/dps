import yaml
from pyspark import SparkContext
from pyspark.rdd import RDD

from ..spark_session import spark_session
from ..utils.io import read_line, to_json


class MassiveTextFilters:
    def __init__(self, conf):
        self.conf = conf

    def doc_len_filter(self, example):
        """Filter any doc that does not contain between min_doc_len and max_doc_len words"""

        min_doc_len = self.conf["doc_len_filter"]["min"]
        max_doc_len = self.conf["doc_len_filter"]["max"]
        return min_doc_len <= len(example["text"]) <= max_doc_len

    def mean_word_len_filter(self, example):
        """Filter any doc whose mean word length is outside the range of min_word_len to max_word_len characters"""

        min_word_len = self.conf["mean_word_len_filter"]["min"]
        max_word_len = self.conf["mean_word_len_filter"]["max"]

        words_lens = [len(word) for word in example["text"].strip().split(" ")]
        mean_word_len = sum(words_lens) / len(words_lens)
        return min_word_len <= mean_word_len <= max_word_len

    def symbol_to_word_ratio_filter(self, example):
        """Filter any doc with a symbol-to-word ratio greater than symbol_to_word_ratio for either the hash symbol or the ellipsis"""

        symbol_to_word_ratio = self.conf["symbol_to_word_ratio_filter"]["ratio"]

        words = example["text"].strip().split(" ")
        return symbol_to_word_ratio >= len(
            [
                word
                for word in words
                if any([symbol in word for symbol in ["…", "...", "#"]])
            ]
        ) / len(words)

    def bullet_ellipsis_filter(self, example):
        """Filter any doc with more than bullet_point_ratio of lines starting with a bullet point, or more than ellipsis_ratio ending with an ellipsis"""

        bullet_point_ratio = self.conf["bullet_ellipsis_filter"]["bullet_point_ratio"]
        ellipsis_ratio = self.conf["bullet_ellipsis_filter"]["ellipsis_ratio"]
        bullets = ["*", "·", "•", "‧", "ㆍ"]
        ellipsis = ["…", "..."]
        sentences = example["text"].strip().split("\n")
        if example:  # for empty example
            bullet_ratio_of_example = len(
                [sentence for sentence in sentences if sentence.strip()[0]
                 in bullets]
            ) / len(sentences)
            ellipsis_endings = 0
            for sentence in sentences:
                for symbol in ellipsis:
                    if sentence.strip()[-len(symbol):] == symbol:
                        ellipsis_endings += 1
            ellipsis_ratio_of_example = ellipsis_endings / len(sentences)
            return (
                bullet_ratio_of_example <= bullet_point_ratio
                and ellipsis_ratio_of_example <= ellipsis_ratio
            )
        return True

    def alphabetic_word_ratio_filter(self, example):
        """Filter any doc that alphabetic_word_ratio of words in a document does not contain at least one alphabetic character"""

        alphabetic_word_ratio = self.conf["alphabetic_word_ratio_filter"]["ratio"]
        words = example["text"].strip().split(" ")
        return alphabetic_word_ratio > 1 - len(
            [word for word in words if any(char.isalpha() for char in word)]
        ) / len(words)

    def least_k_essential_words_filter(self, example):
        """Filter any doc that does not contain at least k of the following English word_list: the, be, to, of, and, that, have, with (language specific words may needed)"""

        k = self.conf["least_k_essential_words_filter"]["k"]
        word_list = self.conf["least_k_essential_words_filter"]["word_list"]

        words = example["text"].strip().split(" ")
        return k <= len([word for word in words if word in word_list])


def massivetext_quality_filtering(config_path):
    with open(config_path) as f:
        conf = yaml.load(f, Loader=yaml.FullLoader)
    massiveTextFilters = MassiveTextFilters(conf)
    input_paths = ','.join([f'{conf["base_dir"]}/{t}' for t in conf["targets"]])

    with spark_session("massivetext quality filtering") as spark:
        sc: SparkContext = spark.sparkContext
        proc_rdd: RDD = (
            sc.textFile(input_paths)
            .repartition(10)
            .flatMap(read_line)
            .filter(massiveTextFilters.doc_len_filter)
            .filter(massiveTextFilters.mean_word_len_filter)
            .filter(massiveTextFilters.symbol_to_word_ratio_filter)
            .filter(massiveTextFilters.bullet_ellipsis_filter)
            .filter(massiveTextFilters.alphabetic_word_ratio_filter)
            .filter(massiveTextFilters.least_k_essential_words_filter)
        )

        proc_rdd.repartition(1).flatMap(
            to_json).saveAsTextFile(conf["output_dir"])
