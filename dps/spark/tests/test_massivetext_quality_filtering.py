import pytest
import yaml
from dps.spark.jobs.massivetext_quality_filtering import MassiveTextFilters
import random
import string


class TestMassivTextFilters:

    config_path = "../../../configs/massivetext_config.yaml"
    with open(config_path) as f:
        conf = yaml.load(f, Loader=yaml.FullLoader)
    massiveTextFilters = MassiveTextFilters(conf)

    def test_doc_len_filter(self):
        min_doc_len = self.conf["doc_len_filter"]["min"]
        max_doc_len = self.conf["doc_len_filter"]["max"]

        less_than_min = "".join([random.choice(string.printable)
                                for _ in range(random.randrange(0, min_doc_len))])
        example = {"text": less_than_min}
        assert self.massiveTextFilters.doc_len_filter(
            example) == False  # Filtered

        between_min_and_max = "".join([random.choice(string.printable) for _ in range(
            random.randrange(min_doc_len, max_doc_len+1))])
        example = {"text": between_min_and_max}
        assert self.massiveTextFilters.doc_len_filter(example) == True

        greater_than_max = "".join([random.choice(string.printable) for _ in range(
            random.randrange(max_doc_len+1, max_doc_len * 2))])
        example = {"text": greater_than_max}
        assert self.massiveTextFilters.doc_len_filter(
            example) == False  # Filtered

    def test_mean_word_len_filter(self):
        min_word_len = self.conf["mean_word_len_filter"]["min"]
        max_word_len = self.conf["mean_word_len_filter"]["max"]

        less_than_min = ""
        for _ in range(random.randrange(10, 100)):  # word count
            for _ in range(random.randrange(1, min_word_len)):  # word length
                less_than_min += random.choice(
                    string.printable.replace(" ", ""))
            less_than_min += " "
        example = {"text": less_than_min.strip()}
        assert self.massiveTextFilters.mean_word_len_filter(
            example) == False  # Filtered

        between_min_and_max = ""
        for _ in range(random.randrange(10, 100)):  # word count
            for _ in range(random.randrange(min_word_len, max_word_len + 1)):  # word length
                between_min_and_max += random.choice(
                    string.printable.replace(" ", ""))
            between_min_and_max += " "
        example = {"text": between_min_and_max.strip()}
        assert self.massiveTextFilters.mean_word_len_filter(example) == True

        greater_than_max = ""
        for _ in range(random.randrange(10, 100)):  # word count
            for _ in range(random.randrange(max_word_len + 1, max_word_len * 2)):  # word length
                greater_than_max += random.choice(
                    string.printable.replace(" ", ""))
            greater_than_max += " "
        example = {"text": greater_than_max.strip()}
        assert self.massiveTextFilters.mean_word_len_filter(
            example) == False  # Filtered

    def test_symbol_to_word_ratio_filter(self):
        printable = string.printable
        symbols = ["…", "...", "#"]
        for symbol in symbols:
            printable = printable.replace(symbol, "")
        symbol_to_word_ratio = self.conf["symbol_to_word_ratio_filter"]["ratio"]

        equal_or_less_than_ratio = random.uniform(0.0, symbol_to_word_ratio)
        symbol_count = int(1000 * equal_or_less_than_ratio)
        word_count = 1000 - symbol_count
        symbol_words = ["".join([random.choice(printable) for _ in range(
            random.randrange(3, 10))]) + random.choice(symbols) for _ in range(symbol_count)]
        without_symbol_words = ["".join([random.choice(printable) for _ in range(
            random.randrange(3, 10))]) for _ in range(word_count)]
        total_words = symbol_words + without_symbol_words
        random.shuffle(total_words)
        text = " ".join(total_words)
        example = {"text": text}
        assert self.massiveTextFilters.symbol_to_word_ratio_filter(
            example) == True

        greater_than_ratio = symbol_to_word_ratio + \
            (1 - random.random()) * (1 - symbol_to_word_ratio)
        word_count = int(1000*(1-greater_than_ratio))
        symbol_count = 1000 - word_count
        symbol_words = ["".join([random.choice(printable) for _ in range(
            random.randrange(3, 10))]) + random.choice(symbols) for _ in range(symbol_count)]
        without_symbol_words = ["".join([random.choice(printable) for _ in range(
            random.randrange(3, 10))]) for _ in range(word_count)]
        total_words = symbol_words + without_symbol_words
        random.shuffle(total_words)
        text = " ".join(total_words)
        example = {"text": text}
        assert self.massiveTextFilters.symbol_to_word_ratio_filter(
            example) == False

    def test_bullet_ellipsis_filter(self):
        # TODO
        assert True

    def test_alphabetic_word_ratio_filter(self):
        # TODO
        assert True

    def test_least_k_essential_words_filter(self):
        # TODO
        assert True
