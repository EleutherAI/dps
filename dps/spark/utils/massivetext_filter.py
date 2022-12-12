import re
from pprint import pprint

from dps.spark.utils.regex_utils import (
    URL_PATTERN,
    URL_TOKEN,
    EMAIL_TOKEN,
    EMAIL_PATTERN,
    RE_SUB,
    RE_SPLIT,
)


def doc_len_filter(text, min_doc_len, max_doc_len):
    """Filter any doc that does not contain between min_doc_len and max_doc_len words"""
    return min_doc_len <= len(text.strip()) <= max_doc_len


def mean_word_len_filter(text, min_mean_word_len, max_mean_word_len):
    """Filter any doc whose mean word length is outside the range of min_word_len to max_word_len characters"""
    words_lens = [len(word) for word in text.strip().split()]
    mean_word_len = sum(words_lens) / len(words_lens)
    return min_mean_word_len <= mean_word_len <= max_mean_word_len


def symbol_to_word_ratio_filter(text, symbol_to_word_ratio):
    """Filter any doc with a symbol-to-word ratio greater than symbol_to_word_ratio for either the hash symbol or the
    ellipsis"""
    words = text.strip().split()
    return symbol_to_word_ratio >= len(
        [
            word
            for word in words
            if any([symbol in word for symbol in ["…", "...", "#"]])
        ]
    ) / len(words)


def bullet_ellipsis_filter(text, bullet_point_ratio, ellipsis_ratio):
    """Filter any doc with more than bullet_point_ratio of lines starting with a bullet point, or more than
    ellipsis_ratio ending with an ellipsis"""

    bullets = ["*", "·", "•", "‧", "ㆍ"]
    ellipsis = ["…", "..."]
    sentences = text.strip().split("\n")

    bullet_ratio_of_example = len(
        [sentence for sentence in sentences if sentence.strip()[0] in bullets]
    ) / len(sentences)
    ellipsis_endings = 0
    for sentence in sentences:
        for symbol in ellipsis:
            if sentence.strip()[-len(symbol) :] == symbol:
                ellipsis_endings += 1
    ellipsis_ratio_of_example = ellipsis_endings / len(sentences)
    return (
        bullet_ratio_of_example <= bullet_point_ratio
        and ellipsis_ratio_of_example <= ellipsis_ratio
    )


def korean_word_ratio_filter(text, korean_word_ratio):
    """Filter any doc that alphabetic_word_ratio of words in a document does not contain at least one alphabetic
    character"""
    return korean_word_ratio <= len(re.findall("[ㄱ-힣]", text)) / (
        len(re.sub("[ \n\t]", "", text)) + 1e-12  # eps
    )


def least_k_essential_words_filter(text, k, word_list):
    """Filter any doc that does not contain at least k of the following English word_list: the, be, to, of, and,
    that, have, with (language specific words may be needed)"""
    words = text.strip().split()
    return k <= len([word for word in words if word in word_list])


def spam_words_filter(text):
    """Remove spam words from the given input text"""
    text = re.sub(EMAIL_PATTERN, EMAIL_TOKEN, text)
    text = re.sub(URL_PATTERN, URL_TOKEN, text)
    for pattern, repl in RE_SUB:
        text = re.sub(pattern, repl, text)
    for pattern in RE_SPLIT:
        text = re.split(pattern, text, maxsplit=1)[0]
    return text.strip()
