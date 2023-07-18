from ast import Index
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

    # print('\n'.join(sentences))
    try:
        bullet_ratio_of_example = len(
            [sentence for sentence in sentences 
                if len(sentence.strip()) > 0 and sentence.strip()[0] in bullets]
        ) / len(sentences)
    except IndexError:
        print(f"IndexError: {sentences}")
        raise IndexError
    
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
    text = re.sub(r"<\s*/?\s*BR\s*/?\s*>", "\n", text)  # https://chojja7.tistory.com/34
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


def remove_repeated_text(input_text, ngram_range=(3, 13), trial=3):
    # TODO: Algorithm is wrong. Need to fix.
    def _remove_repeated_phrase(input_text, ngram_range):
        # words = input_text.replace('\n', '<br>').split()
        # repeated_part_spans = []

        # for i, word in enumerate(words):
        #     prev_ngrams = {
        #         j: " ".join(words[i - j : i])
        #         for j in range(ngram_range[0], ngram_range[1] + 1)
        #     }
        #     next_ngrams = {
        #         j: " ".join(words[i + 1 : i + j + 1])
        #         for j in range(ngram_range[0], ngram_range[1] + 1)
        #     }

        #     for j, (prev_ngram, next_ngram) in enumerate(
        #         zip(prev_ngrams.values(), next_ngrams.values())
        #     ):
        #         if prev_ngram == next_ngram:
        #             repeated_part_spans.append(((i - j, i), (i + 1, i + j + 1)))

        # for word_pos, word in enumerate(words):
        #     for span in repeated_part_spans:
        #         if word_pos in range(span[0][0], span[0][1]) or word_pos in range(
        #             span[1][0], span[1][1]
        #         ):
        #             print(word_pos, word, span)
        #             words[word_pos] = ""

        # print(words)
        # input_text = " ".join(words)
        # input_text = re.sub(r"\s+", " ", input_text)
        # return input_text.strip(), repeated_part_spans
        return input_text, []

    def _remove_repeated_word_over_n_times(input_text, n=3):
        words = input_text.split()
        repeated = []

        for i, word in enumerate(words):
            for n_repeat in range(n):
                if i + n_repeat + 1 < len(words):
                    if word == words[i + n_repeat + 1]:
                        repeated.append(i)
                    else:
                        break

        # remove repeated
        for word_pos, word in enumerate(words):
            if word_pos in repeated:
                words[word_pos] = ""

        input_text = " ".join(words)
        input_text = re.sub(r"\s+", " ", input_text)
        return input_text

    input_text = input_text.replace('\n', '<br>')
    total_len_spans = 0
    for _ in range(trial):
        input_text, spans = _remove_repeated_phrase(input_text, ngram_range)
        total_len_spans += len(spans)

    input_text = _remove_repeated_word_over_n_times(input_text)
    input_text = input_text.replace('<br>', '\n')
    return input_text


def __test__():
    func_list = [
        remove_whitespace,
        process_html_and_uri_text,
        replace_email_and_url,
        remove_repeated_text,
    ]

    text = "Hello\nMy name is       Kevin.\nMy personal Ifno\nemail: ygdsag@gmail.com\n\nPhone:849-5432-1235\nBank Account:\n1234-1234-1234-1234\n\n"

    for func in func_list:
        print(func.__name__)
        print(func(text))
        print()
        

if __name__ == "__main__":
    __test__()