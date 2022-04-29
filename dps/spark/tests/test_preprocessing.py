from dps.spark.utils.text_normalize import *


def test_reduce_emoticon():
    text = "영화 완전 웃기지 않았냐 ㅋㅋㅋㅋㅋㅋㅋㅋㅋㅋ"
    assert preprocess_text(text, [reduce_emoticon])
    

def test_replace_phone_number():
    text = "내 전화 번호는 022-9298-1213이야"
    assert preprocess_text(text, [replace_phone_number])


def test_replace_rrn():
    text = "내 주민등록번호는 141555-4124492이야"
    assert preprocess_text(text, [replace_rrn])


def test_remove_whitespace():
    text = "아         오늘 저녁 맛있는거 먹고    싶    다. "
    assert preprocess_text(text, [remove_whitespace])


def test_strip_html_tags():
    text = "<br> 안녕하세요 좋은 하루 되세요.!<br>"
    assert preprocess_text(text, [strip_html_tags])
