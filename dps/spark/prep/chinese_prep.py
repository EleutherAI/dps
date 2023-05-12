import re
import sys

from dps.spark.utils.japanese_utils import (
    SIMPLIFIED_CHINESE,
    BAD_WORDS,
)


def chinese_word_ratio_filter(text, chinese_word_ratio):
    return chinese_word_ratio <= len(re.findall(f"[{SIMPLIFIED_CHINESE}]", text)) / (
        len(re.sub("[ \r\n\t\f\v]", "", text)) + 1e-12
    )


def chinese_bad_words_filter(text):
    return all(bad_word not in text for bad_word in BAD_WORDS)


# def reduce_chinese_emotion(text):


def remove_html_tags(text: str):
    def clean_space(text):
        text = re.sub("[\r\n\f\v\t]", " ", text)
        while "  " in text:
            text = text.replace("  ", " ")
        return text.strip()

    return text
