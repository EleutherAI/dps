import os
import re
import string
import logging
import csv
from pathlib import Path
from functools import wraps
from unicodedata import normalize
from typing import List, Optional, Union, Callable

import contractions
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize, PunktSentenceTokenizer
from bs4 import BeautifulSoup
from soynlp.normalizer import *

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)


def clean_text(input_text: str) -> str:
    """ Remove bullets or numbering in itemized input """
    processed_text = re.sub('[^가-힣ㄱ-ㅎㅏ-ㅣ\\s]', " ", input_text)
    return processed_text



def remove_url(input_text: str) -> str:
    """ Remove url in the input text """
    return re.sub('(www|http)\S+', '', input_text)


def strip_html_tags(text):
    """remove html tags from text"""
    soup = BeautifulSoup(text, "html.parser")
    stripped_text = soup.get_text(separator=" ")
    return stripped_text


def remove_punctuation(input_text: str, punctuations: Optional[str] = None) -> str:
    """
    Removes all punctuations from a string, as defined by string.punctuation or a custom list.
    For reference, Python's string.punctuation is equivalent to '!"#$%&\'()*+,-./:;<=>?@[\\]^_{|}~'
    """
    if punctuations is None:
        punctuations = string.punctuation
    processed_text = input_text.translate(str.maketrans('', '', punctuations))
    return processed_text


def remove_special_character(input_text: str, special_characters: Optional[str] = None) -> str:
    """ Removes special characters """
    if special_characters is None:
        # TODO: add more special characters
        special_characters = 'å¼«¥ª°©ð±§µæ¹¢³¿®ä£'
    processed_text = input_text.translate(str.maketrans('', '', special_characters))
    return processed_text


def remove_whitespace(input_text: str, remove_duplicate_whitespace: bool = True) -> str:
    """ Removes leading, trailing, and (optionally) duplicated whitespace """
    if remove_duplicate_whitespace:
        return ' '.join(re.split('\s+', input_text.strip(), flags=re.UNICODE))
    return input_text.strip()


def expand_contraction(input_text: str) -> str:
    """ Expand contractions in input text """
    return contractions.fix(input_text)


def normalize_unicode(input_text: str) -> str:
    """ Normalize unicode data to remove umlauts, and accents, etc. """
    processed_tokens = normalize('NFKD', input_text).encode('ASCII', 'ignore').decode('utf8')
    return processed_tokens


def replace_email(input_text: str) -> str:
    """ Remove email in the input text """
    regex_pattern = '[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}'
    return re.sub(regex_pattern, '<|email_address|>', input_text)


def replace_phone_number(input_text: str) -> str:
    """ Remove phone number in the input text """
    regex_pattern = '[0-9]{2,3}-[0-9]{3,4}-[0-9]{4}'    

    return re.sub(regex_pattern, '<|tel|>', input_text)


def replace_ssn(input_text: str) -> str:
    """ Remove social security number in the input text """
    regex_pattern = "([0-9]{6})\\-[0-9]{7}"
        # p = "[0-9]{6}\\-[0-9]{7}"
    return re.sub(regex_pattern, '<|rrn|>', input_text)


def tokenize_word(input_text: str) -> List[str]:
    """ Converts a text into a list of word tokens """
    if input_text is None or len(input_text) == 0:
        return []
    return word_tokenize(input_text)


def tokenize_sentence(input_text: str) -> List[str]:
    """ Converts a text into a list of sentence tokens """
    if input_text is None or len(input_text) == 0:
        return []
    tokenizer = PunktSentenceTokenizer()
    return tokenizer.tokenize(input_text)


def reduce_emoticon(text: str, n=2):
    return emoticon_normalize(text, num_repeats=n)


def preprocess_text(input_text: str, processing_function_list: Optional[List[Callable]] = None) -> str:
    """ Preprocess an input text by executing a series of preprocessing functions specified in functions list """
    if processing_function_list is None:
        processing_function_list = [remove_url,
                                    reduce_emoticon,
                                    clean_text,
                                    expand_contraction,
                                    remove_special_character,
                                    replace_phone_number,
                                    replace_ssn,
                                    remove_punctuation,
                                    remove_whitespace,
                                    normalize_unicode
                                    ]
    for func in processing_function_list:
        input_text = func(input_text)
    if isinstance(input_text, str):
        processed_text = input_text
    else:
        processed_text = ' '.join(input_text)
    return processed_text


def strip_html_tags(text):
    """remove html tags from text"""
    soup = BeautifulSoup(text, "html.parser")
    stripped_text = soup.get_text(separator=" ")
    return stripped_text


if __name__ == "__main__":
    # text = '안녕하세요 반갑습니다🐶'
    # print(text) 
    
    only_BMP_pattern = re.compile("["
            u"\U00010000-\U0010FFFF"  #BMP characters 이외
                            "]+", flags=re.UNICODE)
    
    text_to_process = '<br>안녕하세요,       내 이름은 양승무야!!! 내 주민등록번호는 900022-1133098이고, 내 이메일 주소는 smyang@gmail.com 이고 전화번호는 02-211-0223 이야<br>'
    # print(only_BMP_pattern.sub(r'', text_to_process))# BMP characters만
    # print(only_BMP_pattern.sub(r'', text))# BMP characters만
    


    preprocess_functions = [reduce_emoticon, replace_phone_number, replace_ssn, remove_whitespace, strip_html_tags]
    preprocessed_text = preprocess_text(text_to_process, preprocess_functions)
    print(f"Before preprocessing: {text_to_process}")
    print(f"After preprocessing: {preprocessed_text}")
