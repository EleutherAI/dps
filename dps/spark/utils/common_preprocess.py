import re

from bs4 import BeautifulSoup
from soynlp.normalizer import emoticon_normalize


def remove_url(input_text: str) -> str:
    return re.sub('(www|http)\\S+', '', input_text)


def remove_html_tags(text):
    soup = BeautifulSoup(text, "html.parser")
    return soup.get_text(separator=" ").strip()


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
    return re.sub(regex_pattern, '<|rrn|>', input_text)


def replace_creditcard(input_text: str) -> str:
    regex_pattern = "(5[1-5]\d{14})|(4\d{12})(\d{3}?)|3[47]\d{13}|(6011\d{12})|([0-9]{4})\\-([0-9]{4})\\-([0-9]{4})\\-([0-9]{4})"
    return re.sub(regex_pattern, '<|crd|>', input_text)


def replace_bank_account(input_text: str) -> str:
    regex_pattern = "^(\d{1,})(-(\d{1,})){1,}|([0-9]\d{11})|([0-9]\d{12})|([0-9]\d{13})|([0-9]\d{14})|([0-9]{3,4}\\-[0-9]{3,4}\\-[0-9]{5,6})|([0-9]{3}\\-[0-9]{2,4}\\-[0-9]{4,6}\\-[0-9]{1,2})|([0-9]{3}\\-[0-9]{6}\\-[0-9]{2,4}\\-[0-9]{3})"
    # regex_pattern = re.compile("(\d{2,6}[ -]-?\d{2,6}[ -]-?\d{2,6}[ -]-?\d{2,6})")
    # return re.sub(regex_pattern, '<|acc|>', input_text)
    # if regex_pattern.search(input_text):
    return re.sub(regex_pattern, '<|acc|>', input_text)


def reduce_emoticon(text: str, n=2):
    """
    Function that reduces repeating Korean characters
    ex) ㅋㅋㅋㅋㅋㅋㅋ => ㅋㅋ
    """
    return emoticon_normalize(text, num_repeats=n)


if __name__ == "__main__":
    print(replace_bank_account("제 통장 번호는 110-274-742823입니다."))
    