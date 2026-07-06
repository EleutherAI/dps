from dps.spark.utils.korean_utils import CARD_PATTERN
from dps.spark.utils.lang_agnostic_utils import replace_with_token
from dps.spark.utils.token_utils import CARD_END_TOKEN, CARD_START_TOKEN, PHONE_NUMBER_END_TOKEN, PHONE_NUMBER_START_TOKEN, RRN_END_TOKEN, RRN_START_TOKEN
from dps.spark.utils.vietnamese_utils import BAD_WORDS_VIETNAMESE, NATIONAL_ID_PATTERN, PHONE_NUMBER_PATTERN

from underthesea import text_normalize, word_tokenize


def vietnamese_bad_words_filter(text):
    """Drop text that contains bad words"""
    for bad_word in BAD_WORDS_VIETNAMESE:
        if bad_word in text:
            return False
    return True


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


def normalize_vietnam(text: str):
    """
    Normalize text in Vietnamese
    example: Ðảm baỏ chất lựơng phòng thí nghịêm hoá học => Đảm bảo chất lượng phòng thí nghiệm hóa học
    """
    text = text_normalize(text)
    return text


def replace_vietnam_pii(text: str):
    replaces = []

    text = replace_with_token(
        text, CARD_PATTERN, CARD_START_TOKEN, CARD_END_TOKEN, replaces
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

    for before, after in replaces:
        text = text.replace(before, after)

    return text


def vietnamese_mean_word_len_filter(
    text: str, min_mean_word_len: int, max_mean_word_len: int
) -> bool:
    # TODO: might be better to add another argument `is_japanese` to lang_agnostic_prep.mean_word_len_filter
    words = word_tokenize(text)
    words_lens = [len(word) for word in words]
    mean_word_len = sum(words_lens) / len(words_lens)
    return min_mean_word_len <= mean_word_len <= max_mean_word_len