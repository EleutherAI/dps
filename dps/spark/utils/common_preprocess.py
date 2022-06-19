import re
import sys
from bs4 import BeautifulSoup

kor_begin     = 44032
kor_end       = 55203
chosung_base  = 588
jungsung_base = 28
jaum_begin = 12593
jaum_end = 12622
moum_begin = 12623
moum_end = 12643

chosung_list = [ 'ㄱ', 'ㄲ', 'ㄴ', 'ㄷ', 'ㄸ', 'ㄹ', 'ㅁ', 'ㅂ', 'ㅃ', 
        'ㅅ', 'ㅆ', 'ㅇ' , 'ㅈ', 'ㅉ', 'ㅊ', 'ㅋ', 'ㅌ', 'ㅍ', 'ㅎ']

jungsung_list = ['ㅏ', 'ㅐ', 'ㅑ', 'ㅒ', 'ㅓ', 'ㅔ', 
        'ㅕ', 'ㅖ', 'ㅗ', 'ㅘ', 'ㅙ', 'ㅚ', 
        'ㅛ', 'ㅜ', 'ㅝ', 'ㅞ', 'ㅟ', 'ㅠ', 
        'ㅡ', 'ㅢ', 'ㅣ']

jongsung_list = [
    ' ', 'ㄱ', 'ㄲ', 'ㄳ', 'ㄴ', 'ㄵ', 'ㄶ', 'ㄷ',
        'ㄹ', 'ㄺ', 'ㄻ', 'ㄼ', 'ㄽ', 'ㄾ', 'ㄿ', 'ㅀ', 
        'ㅁ', 'ㅂ', 'ㅄ', 'ㅅ', 'ㅆ', 'ㅇ', 'ㅈ', 'ㅊ', 
        'ㅋ', 'ㅌ', 'ㅍ', 'ㅎ']

jaum_list = ['ㄱ', 'ㄲ', 'ㄳ', 'ㄴ', 'ㄵ', 'ㄶ', 'ㄷ', 'ㄸ', 'ㄹ', 
              'ㄺ', 'ㄻ', 'ㄼ', 'ㄽ', 'ㄾ', 'ㄿ', 'ㅀ', 'ㅁ', 'ㅂ', 
              'ㅃ', 'ㅄ', 'ㅅ', 'ㅆ', 'ㅇ', 'ㅈ', 'ㅉ', 'ㅊ', 'ㅋ', 'ㅌ', 'ㅍ', 'ㅎ']

moum_list = ['ㅏ', 'ㅐ', 'ㅑ', 'ㅒ', 'ㅓ', 'ㅔ', 'ㅕ', 'ㅖ', 'ㅗ', 'ㅘ', 
              'ㅙ', 'ㅚ', 'ㅛ', 'ㅜ', 'ㅝ', 'ㅞ', 'ㅟ', 'ㅠ', 'ㅡ', 'ㅢ', 'ㅣ']


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
    regex_pattern = '([0-9]{2,3}-[0-9]{3,4}-[0-9]{4})|([0-9]{2,3}[0-9]{3,4}[0-9]{4})'
    return re.sub(regex_pattern, '<|tel|>', input_text)


def replace_rrn(input_text: str) -> str:
    regex_pattern = "([0-9]{6})\\-[0-9]{7}"
    return re.sub(regex_pattern, '<|rrn|>', input_text)


def replace_creditcard(input_text: str) -> str:
    regex_pattern = "(5[1-5]\d{14})|(4\d{12})(\d{3}?)|3[47]\d{13}|(6011\d{12})|([0-9]{4})\\-([0-9]{4})\\-([0-9]{4})\\-([0-9]{4})"
    return re.sub(regex_pattern, '<|crd|>', input_text)


def replace_bank_account(input_text: str) -> str:
    ibk = "([0-9]\d{13})|([0-9,\-]{3,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2,6})"
    kb = "([0-9]\d{13})|([0-9,\-]{3,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2,6})"
    nh = "([0-9]\d{12})|([0-9,\-]{3,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2})"
    shinhan = "([0-9]\d{11})|([0-9,\-]{3,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2,6})"
    woori = "([0-9]\d{12})|([0-9,\-]{3,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2,6})"
    keb = "([0-9]\d{13})|([0-9,\-]{3,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2,6})"
    citi = "([0-9]\d{11})|([0-9,\-]{3,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2,6})"
    dgb = "([0-9]\d{11})|([0-9]\d{11})|([0-9,\-]{3,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2,6}\-[0-9,\-])"
    bnk = "([0-9]\d{12})|([0-9]\d{12})|([0-9,\-]{3,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2})"
    sc = "([0-9]\d{10})|([0-9]\d{10})|([0-9,\-]{3,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2,6})"
    kbank = "([0-9]\d{11})|([0-9]\d{10})|([0-9,\-]{3,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2,6})"
    kakao = "([0-9]\d{12})|([0-9]\d{10})|([0-9,\-]{3,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2,7})"
    
    regex_pattern = ibk + kb + nh + shinhan + woori \
                    + keb + citi + dgb + bnk + sc + kbank + kakao
                    
    return re.sub(regex_pattern, '<|acc|>', input_text)
    
    
def reduce_emoticon(text: str, num_repeats=2):
    """
    Original Code
    https://github.com/lovit/soynlp/blob/master/soynlp/normalizer/_normalizer.py
    
    Function that reduces repeating Korean characters
    ex) ㅋㅋㅋㅋㅋㅋㅋ => ㅋㅋ
    """
    
    repeatchars_pattern = re.compile('(\w)\\1{2,}')
    doublespace_pattern = re.compile('\s+')
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
        return chr(kor_begin + chosung_base * chosung_list.index(chosung) + jungsung_base * jungsung_list.index(jungsung) + jongsung_list.index(jongsung))

    def decompose(c):
        if not character_is_korean(c):
            return None
        i = to_base(c)
        if (jaum_begin <= i <= jaum_end):
            return (c, ' ', ' ')
        if (moum_begin <= i <= moum_end):
            return (' ', c, ' ')    
        i -= kor_begin
        cho  = i // chosung_base
        jung = ( i - cho * chosung_base ) // jungsung_base 
        jong = ( i - cho * chosung_base - jung * jungsung_base )    
        return (chosung_list[cho], jungsung_list[jung], jongsung_list[jong])
    
    def character_is_korean(c):
        i = to_base(c)
        return (kor_begin <= i <= kor_end) or (jaum_begin <= i <= jaum_end) or (moum_begin <= i <= moum_end)
    
    def repeat_normalize(sent, num_repeats=2):
        if num_repeats > 0:
            sent = repeatchars_pattern.sub('\\1' * num_repeats, sent)
        sent = doublespace_pattern.sub(' ', sent)
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

    idxs = [pattern(ord(c)) for c in text]
    sent_ = []
    last_idx = len(idxs) - 1
    for i, (idx, c) in enumerate(zip(idxs, text)):
        if (i > 0 and i < last_idx) and (idxs[i-1] == 0 and idx == 2 and idxs[i+1] == 1):
            cho, jung, jong = decompose(c)
            if (cho == text[i-1]) and (jung == text[i+1]) and (jong == ' '):
                sent_.append(cho)
                sent_.append(jung)
            else:
                sent_.append(c)
        elif (i < last_idx) and (idx == 2) and (idxs[i+1] == 0):
            cho, jung, jong = decompose(c)
            if (jong == text[i+1]):
                sent_.append(compose(cho, jung, ' '))
                sent_.append(jong)
        elif (i > 0) and (idx == 2 and idxs[i-1] == 0):
            cho, jung, jong = decompose(c)
            if (cho == text[i-1]):
                sent_.append(cho)
                sent_.append(jung)
        else:
            sent_.append(c)
            
    return repeat_normalize(''.join(sent_), num_repeats)
    
