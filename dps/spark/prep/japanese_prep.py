import re
import sys

from sudachipy import dictionary
from sudachipy import tokenizer


from dps.spark.utils.japanese_utils import (
    JAPANESE_CHARS,
    BAD_WORD_LIST,
    JAPANESE_FREQ_CHAR_LIST
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
    symbols = [
        "...", "…", "[…]",
        "#",
    ]
    words = word_tokenize(text)
    return symbol_to_word_ratio >= (
        len([word for word in words if any([symbol in word for symbol in symbols])])
        / (len(words) + 1e-12)
    )


def japanese_frequent_char_existence_filter(text: str, freq_char_cnt: int) -> bool:
    return freq_char_cnt <= (
        sum([re.search(chr, text)!=None for chr in JAPANESE_FREQ_CHAR_LIST])
    )


def reduce_japanese_emoticon(text):
    text = re.sub("w{3,}", "www", text)
    text = re.sub("笑{2,}", "笑", text)
    return text


def many_separators_filter(text: str, separator_ratio: float):
    whitespace_ratio = (len(text.split()) - 1) / len(text)
    touten_ratio = (len(text.split("、")) - 1) / len(text)
    return (whitespace_ratio <= separator_ratio) and (touten_ratio <= separator_ratio)
    # NOTE: test and check the filter with the opposite condition
    # return (whitespace_ratio > 0.1) or (touten_ratio > 0.1)


def remove_symbols(text):
    return text.replace("[…]", "")