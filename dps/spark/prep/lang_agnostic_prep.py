import html
import re

from dps.spark.utils.lang_agnostic_utils import (
    EMAIL_PATTERN,
    URL_PATTERN,
    replace_with_token,
)
from dps.spark.utils.token_utils import (
    EMAIL_START_TOKEN,
    EMAIL_END_TOKEN,
    URL_START_TOKEN,
    URL_END_TOKEN,
)


def doc_len_filter(text: str, min_doc_len: int, max_doc_len: int) -> bool:
    """Filter any doc that does not contain between min_doc_len and max_doc_len words"""
    return min_doc_len <= len(text.strip()) <= max_doc_len


def mean_word_len_filter(
    text: str, min_mean_word_len: int, max_mean_word_len: int
) -> bool:
    """Filter any doc whose mean word length is outside the range of min_word_len to max_word_len characters"""
    words_lens = [len(word) for word in text.strip().split()]
    mean_word_len = sum(words_lens) / len(words_lens)
    return min_mean_word_len <= mean_word_len <= max_mean_word_len


def symbol_to_word_ratio_filter(text: str, symbol_to_word_ratio: float) -> bool:
    """Filter any doc with a symbol-to-word ratio greater than symbol_to_word_ratio for either the hash symbol or the
    ellipsis"""
    words = text.strip().split()
    return symbol_to_word_ratio >= (
        len(
            [
                word
                for word in words
                if any([symbol in word for symbol in ["…", "...", "#"]])
            ]
        )
        / (len(words) + 1e-12)
    )


def bullet_ellipsis_filter(
    text: str, bullet_point_ratio: float, ellipsis_ratio: float
) -> bool:
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


def remove_whitespace(text: str, remove_duplicate_whitespace: bool = True) -> str:
    if remove_duplicate_whitespace:
        return " ".join(re.split("[^\S\r\n\t\v\f]+", text.strip(), flags=re.UNICODE))
    return text.strip()


def process_html_and_uri_text(text: str):
    text = html.unescape(text)
    text = re.sub(r"<\s*/?\s*br\s*/?\s*>", "\n", text)  # https://chojja7.tistory.com/34
    text = re.sub(r"%[0-9A-Fa-f]{2}", "", text)
    return text


def replace_email_and_url(text: str):
    """
    TODO:
        email de-identification needed.
        cc @paulvn
    """
    replaces = []
    text = replace_with_token(
        text, EMAIL_PATTERN, EMAIL_START_TOKEN, EMAIL_END_TOKEN, replaces
    )
    text = replace_with_token(
        text, URL_PATTERN, URL_START_TOKEN, URL_END_TOKEN, replaces
    )

    for before, after in replaces:
        text = text.replace(before, after)

    return text
