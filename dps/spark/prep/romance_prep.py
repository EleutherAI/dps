import re
import sys

import html2text
from bs4 import BeautifulSoup

from dps.spark.utils.romance_utils import (
    SPANISH_PATTERN,
    PORTUGUESE_PATTERN,
    FRENCH_PATTERN,
    ITALIAN_PATTERN,
    ROMANIAN_PATTERM,
    NATIONAL_ID_PATTERN,
    PHONE_NUMBER_PATTERN,
    BAD_WORDS_ROMACE,
    ROMANCE_HTML_SPLIT,
    HTML_REMOVE
)

from dps.spark.utils.lang_agnostic_utils import (
    URL_PATTERN,
    EMAIL_PATTERN,
    CREDIT_CARD_PATTERN,
    replace_with_token,
    BANK_ACCOUNT_PATTERN
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

import re


def romance_word_ratio_filter_factory(language_pattern):
    """
    Returns a function that takes a string and a desired word ratio as input,
    and returns True if the word ratio for the given language is greater than
    or equal to the desired ratio, and False otherwise.

    language_pattern: a regular expression pattern that matches words in the
    desired language. The pattern should only match lowercase letters, and any
    diacritical marks or accented characters used in the language.

    Example:
    pattern = "[a-zà-ú]+"
    filter_function = romance_word_ratio_filter_factory(pattern)
    text = "Este es un ejemplo de texto en español."
    is_spanish = filter_function(text, 0.2)
    """

    def word_ratio_filter(text, word_ratio):
        num_words = len(re.findall(language_pattern, text.lower()))
        num_non_space_chars = len(re.sub("[ \r\n\t\f\v]", "", text))
        actual_word_ratio = num_words / (num_non_space_chars + 1e-12)
        return actual_word_ratio >= word_ratio

    return word_ratio_filter


spanish_word_ratio_filter = romance_word_ratio_filter_factory(SPANISH_PATTERN)
portuguese_word_ratio_filter = romance_word_ratio_filter_factory(PORTUGUESE_PATTERN)
french_word_ratio_filter = romance_word_ratio_filter_factory(FRENCH_PATTERN)
italian_word_ratio_filter = romance_word_ratio_filter_factory(ITALIAN_PATTERN)
romanian_word_ratio_filter = romance_word_ratio_filter_factory(ROMANIAN_PATTERM)


def reduce_emoticon(text: str, num_repeats=2):
    """
    Reducing the number of repeating emoticons in a text.
    If the number of emoticons repeated are more than num_repeats, then reduce it to num_repeats.
    example: :):):):):) => :):)
    """
    emoticons = [":)", ":D", ":P", ":(", ":O", ";)", "xD"]  # list of emoticons to check for repetition
    for emoticon in emoticons:
        count = 0
        while emoticon * (num_repeats + 1) in text:
            count += 1
            text = text.replace(emoticon * (num_repeats + 1), emoticon * num_repeats)
    return text


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
                for symbol in ROMANCE_HTML_SPLIT:
                    sent = sent.split(symbol)[0]
                filtered_sents.append(sent)

        text = " ".join(filtered_sents)
        text = clean_space(text)
        text = text.replace(" !", "")
    return text


def bad_words_filter(text):
    """Drop text that contains bad words"""
    for bad_word in BAD_WORDS_ROMACE:
        if bad_word in text:
            return False
    return True


def replace_romance_pii(text: str):
    replaces = []
    text = replace_with_token(
        text, CREDIT_CARD_PATTERN, CARD_START_TOKEN, CARD_END_TOKEN, replaces
    )
    text = replace_with_token(
        text, NATIONAL_ID_PATTERN, RRN_START_TOKEN, RRN_END_TOKEN, replaces
    )
    text = replace_with_token(
        text,
        PHONE_NUMBER_PATTERN,
        PHONE_NUMBER_START_TOKEN,
        PHONE_NUMBER_END_TOKEN,
        replaces,
    )
    text = replace_with_token(
        text, BANK_ACCOUNT_PATTERN, ACCOUNT_START_TOKEN, ACCOUNT_END_TOKEN, replaces
    )

    for before, after in replaces:
        text = text.replace(before, after)

    return text
