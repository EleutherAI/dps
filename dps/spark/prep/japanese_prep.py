import re
import sys

from sudachipy import dictionary
from sudachipy import tokenizer


from dps.spark.utils.japanese_utils import (
    JAPANESE_CHARS,
    BAD_WORD_LIST,
)


tokenizer_obj = dictionary.Dictionary().create()


def word_tokenize(text):
    mode = tokenizer.Tokenizer.SplitMode.C
    tokenized_text = " ".join([m.surface() for m in tokenizer_obj.tokenize(text, mode)])
    return tokenized_text


def japanese_word_ratio_filter(text, japanese_word_ratio):
    return japanese_word_ratio <= len(re.findall(f'[{JAPANESE_CHARS}]', text)) / (
        len(re.sub("[ \r\n\t\f\v]", "", text)) + 1e-12)


def japanese_bad_words_filter(text):
    for bad_word in BAD_WORD_LIST:
        if bad_word in text:
            return False
    return True


def japanese_mean_word_len_filter(
    text: str, min_mean_word_len: int, max_mean_word_len: int
) -> bool:
    # TODO: might be better to add another argument `is_japanese` to lang_agnostic_prep.mean_word_len_filter
    words = word_tokenize(text)
    words_lens = [len(word) for word in words]
    mean_word_len = sum(words_lens) / len(words_lens)
    return min_mean_word_len <= mean_word_len <= max_mean_word_len


def japanese_symbol_to_word_ratio_filter(text: str, symbol_to_word_ratio: float) -> bool:
    symbols = ["…", "...", "#"]
    words = word_tokenize(text)
    return symbol_to_word_ratio >= (
        len([word for word in words if any([symbol in word for symbol in symbols])])
        / (len(words) + 1e-12)
    )