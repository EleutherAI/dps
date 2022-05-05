import re
from unicodedata import normalize
from typing import List, Optional, Callable
from bs4 import BeautifulSoup

from soynlp.normalizer import *


def clean_text(input_text: str) -> str:
    processed_text = re.sub('[^가-힣ㄱ-ㅎㅏ-ㅣ\\s]', " ", input_text)
    return processed_text


def remove_url(input_text: str) -> str:
    return re.sub('(www|http)\\S+', '', input_text)


def strip_html_tags(text):
    soup = BeautifulSoup(text, "html.parser")
    stripped_text = soup.get_text(separator=" ")
    return stripped_text


def remove_whitespace(input_text: str, remove_duplicate_whitespace: bool = True) -> str:
    if remove_duplicate_whitespace:
        return ' '.join(re.split('\\s+', input_text.strip(), flags=re.UNICODE))
    return input_text.strip()


def replace_email(input_text: str) -> str:
    regex_pattern = '[a-z0-9._%+-]+@[a-z0-9.-]+\\.[a-z]{2,}'
    return re.sub(regex_pattern, '<|email_address|>', input_text)


def replace_phone_number(input_text: str) -> str:
    regex_pattern = '[0-9]{2,3}-[0-9]{3,4}-[0-9]{4}'    

    return re.sub(regex_pattern, '<|tel|>', input_text)


def replace_rrn(input_text: str) -> str:
    regex_pattern = "([0-9]{6})\\-[0-9]{7}"
        # p = "[0-9]{6}\\-[0-9]{7}"
    return re.sub(regex_pattern, '<|rrn|>', input_text)


def reduce_emoticon(text: str, n=2):
    """
    Function that reduces repeating Korean characters
    ex) ㅋㅋㅋㅋㅋㅋㅋ => ㅋㅋ
    """
    return emoticon_normalize(text, num_repeats=n)


def strip_html_tags(text):
    """remove html tags from text"""
    soup = BeautifulSoup(text, "html.parser")
    stripped_text = soup.get_text(separator=" ")
    return stripped_text.strip()
