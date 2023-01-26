import re
import sys

from dps.spark.utils.japanese_utils import (
    JAPANESE_CHARS
)


def japanese_word_ratio_filter(text, japanese_word_ratio):
    return japanese_word_ratio <= len(re.findall(f'[{JAPANESE_CHARS}]', text))\
         / len(re.sub("[ \r\n\t\f\v", "", text)) + 1e-12