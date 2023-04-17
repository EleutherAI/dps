import re
from pythainlp import thai_characters

from dps.spark.utils.thai_utils import (
    BAD_WORD_LIST,
    THAI_FREQ_CHAR_LIST
)    

def thai_word_ratio_filter(text, thai_word_ratio):
    return thai_word_ratio <= len(re.findall(f'[{thai_characters}]', text)) / (
        len(re.sub("[ \r\n\t\f\v]", "", text)) + 1e-12)
    
def thai_bad_words_filter(text):
    for bad_word in BAD_WORD_LIST:
        if bad_word in text:
            return False
    return True

def thai_frequent_char_existence_filter(text: str,
                                        freq_char_ratio: float) -> bool:
    return freq_char_ratio > (
        sum([re.search(chr, text)!=None for chr in THAI_FREQ_CHAR_LIST])
        /len(THAI_FREQ_CHAR_LIST)
    )