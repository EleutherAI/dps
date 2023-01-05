import re
import sys

import html2text
from bs4 import BeautifulSoup

from dps.spark.utils.korean_utils import (
    KOR_BEGIN,
    CHOSUNG_BASE,
    CHOSUNG,
    JUNGSUNG_BASE,
    JUNGSUNG,
    JONGSUNG,
    JAUM_END,
    JAUM_BEGIN,
    MOUM_BEGIN,
    MOUM_END,
    KOR_END,
    HTML_REMOVE,
    HTML_SPLIT,
    SPAM_REMOVE,
    SPAM_SPLIT,
    CARD_PATTERN,
    PHONE_NUMBER_PATTERN,
    RRN_PATTERN,
    ACCOUNT_PATTERN,
)
from dps.spark.utils.lang_agnostic_utils import (
    URL_PATTERN,
    EMAIL_PATTERN,
    replace_with_token,
)
from dps.spark.utils.token_utils import (
    URL_START_TOKEN,
    URL_END_TOKEN,
    EMAIL_START_TOKEN,
    EMAIL_END_TOKEN,
    CARD_END_TOKEN,
    CARD_START_TOKEN,
    PHONE_NUMBER_END_TOKEN,
    PHONE_NUMBER_START_TOKEN,
    RRN_END_TOKEN,
    RRN_START_TOKEN,
    ACCOUNT_START_TOKEN,
    ACCOUNT_END_TOKEN,
)


def korean_word_ratio_filter(text, korean_word_ratio):
    return korean_word_ratio <= len(re.findall("[ㄱ-힣]", text)) / (
        len(re.sub("[ \r\n\t\f\v]", "", text)) + 1e-12  # eps
    )


def reduce_emoticon(text: str, num_repeats=2):
    """
    Original Code
    https://github.com/lovit/soynlp/blob/master/soynlp/normalizer/_normalizer.py

    Function that reduces repeating Korean characters
    ex) ㅋㅋㅋㅋㅋㅋㅋ => ㅋㅋ
    """

    repeatchars_pattern = re.compile("(\w)\\1{2,}")
    doublespace_pattern = re.compile("[^\S\r\n]+")
    if not text:
        return text

    def to_base(c):
        if sys.version_info.major == 2:
            if type(c) == str or type(c) == unicode:
                return ord(c)
            else:
                raise TypeError
        else:
            if type(c) == str or type(c) == int:
                return ord(c)
            else:
                raise TypeError

    def compose(chosung, jungsung, jongsung):
        return chr(
            KOR_BEGIN
            + CHOSUNG_BASE * CHOSUNG.index(chosung)
            + JUNGSUNG_BASE * JUNGSUNG.index(jungsung)
            + JONGSUNG.index(jongsung)
        )

    def decompose(c):
        if not character_is_korean(c):
            return None
        i = to_base(c)
        if JAUM_BEGIN <= i <= JAUM_END:
            return c, " ", " "
        if MOUM_BEGIN <= i <= MOUM_END:
            return " ", c, " "
        i -= KOR_BEGIN
        cho = i // CHOSUNG_BASE
        jung = (i - cho * CHOSUNG_BASE) // JUNGSUNG_BASE
        jong = i - cho * CHOSUNG_BASE - jung * JUNGSUNG_BASE
        return CHOSUNG[cho], JUNGSUNG[jung], JONGSUNG[jong]

    def character_is_korean(c):
        i = to_base(c)
        return (
            (KOR_BEGIN <= i <= KOR_END)
            or (JAUM_BEGIN <= i <= JAUM_END)
            or (MOUM_BEGIN <= i <= MOUM_END)
        )

    def repeat_normalize(sent, num_repeats=2):
        if num_repeats > 0:
            sent = repeatchars_pattern.sub("\\1" * num_repeats, sent)
        sent = doublespace_pattern.sub(" ", sent)
        return sent.strip()

    # Pattern matching ㅋ쿠ㅜ
    def pattern(idx):
        # Jaum: 0, Moum: 1, Complete: 2, else -1
        if 12593 <= idx <= 12622:
            return 0
        elif 12623 <= idx <= 12643:
            return 1
        elif 44032 <= idx <= 55203:
            return 2
        else:
            return -1

    indices = [pattern(ord(c)) for c in text]
    sent_ = []
    last_idx = len(indices) - 1
    for i, (idx, c) in enumerate(zip(indices, text)):
        if (0 < i < last_idx) and (
            indices[i - 1] == 0 and idx == 2 and indices[i + 1] == 1
        ):
            cho, jung, jong = decompose(c)
            if (cho == text[i - 1]) and (jung == text[i + 1]) and (jong == " "):
                sent_.append(cho)
                sent_.append(jung)
            else:
                sent_.append(c)
        elif (i < last_idx) and (idx == 2) and (indices[i + 1] == 0):
            cho, jung, jong = decompose(c)
            if jong == text[i + 1]:
                sent_.append(compose(cho, jung, " "))
                sent_.append(jong)
        elif (i > 0) and (idx == 2 and indices[i - 1] == 0):
            cho, jung, jong = decompose(c)
            if cho == text[i - 1]:
                sent_.append(cho)
                sent_.append(jung)
        else:
            sent_.append(c)

    return repeat_normalize("".join(sent_), num_repeats)


def remove_html_tags(text: str):
    def clean_space(text):
        text = re.sub("[\r\n\f\v\t]", " ", text)
        while "  " in text:
            text = text.replace("  ", " ")
        return text.strip()

    if bool(BeautifulSoup(text, "html.parser").find()):
        text = html2text.html2text(text)
        text = clean_space(text)

        for pattern in [
            URL_PATTERN,
            URL_START_TOKEN,
            URL_END_TOKEN,
            EMAIL_PATTERN,
            EMAIL_START_TOKEN,
            EMAIL_END_TOKEN,
        ]:
            text = re.sub(pattern, "", text)

        sents = re.split(r"(?<=[.!?])\s", text)

        filtered_sents = []
        for sent in sents:
            add = True
            for symbol in HTML_REMOVE:
                if symbol in sent:
                    add = False
                    break

            if add is True:
                for symbol in HTML_SPLIT:
                    sent = sent.split(symbol)[0]
                filtered_sents.append(sent)

        text = " ".join(filtered_sents)
        text = clean_space(text)
        text = text.replace(" !", "")
    return text


def spam_words_filter(text):
    """Remove spam words from the given input text"""
    for pattern, repl in SPAM_REMOVE:
        text = re.sub(pattern, repl, text)
    for pattern in SPAM_SPLIT:
        text = re.split(pattern, text, maxsplit=1)[0]
    return text.strip()


def replace_korean_pii(text: str):
    replaces = []
    text = replace_with_token(
        text, CARD_PATTERN, CARD_START_TOKEN, CARD_END_TOKEN, replaces
    )
    text = replace_with_token(
        text, RRN_PATTERN, RRN_START_TOKEN, RRN_END_TOKEN, replaces
    )
    text = replace_with_token(
        text,
        PHONE_NUMBER_PATTERN,
        PHONE_NUMBER_START_TOKEN,
        PHONE_NUMBER_END_TOKEN,
        replaces,
    )
    text = replace_with_token(
        text, ACCOUNT_PATTERN, ACCOUNT_START_TOKEN, ACCOUNT_END_TOKEN, replaces
    )

    for before, after in replaces:
        text = text.replace(before, after)

    return text
