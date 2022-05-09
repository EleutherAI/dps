from dps.spark.utils.common_preprocess import (reduce_emoticon,
                                               replace_phone_number,
                                               replace_rrn,
                                               remove_whitespace,
                                               remove_html_tags)


def test_reduce_emoticon():
    text = "영화 완전 웃기지 않았냐 ㅋㅋㅋㅋㅋㅋㅋㅋㅋㅋ"
    assert reduce_emoticon(text) == "영화 완전 웃기지 않았냐 ㅋㅋ"
    

def test_replace_phone_number():
    text = "내 전화 번호는 022-9298-1213이야"
    assert replace_phone_number(text) == "내 전화 번호는 <|tel|>이야"


def test_replace_rrn():
    text = "내 주민등록번호는 141555-4124492이야"
    assert replace_rrn(text) == "내 주민등록번호는 <|rrn|>이야"


def test_remove_whitespace():
    text = " 아         오늘 저녁 맛있는거 먹고    싶    다. "
    assert remove_whitespace(text) == "아 오늘 저녁 맛있는거 먹고 싶 다."


def test_remove_html_tags():
    text = "<br> 안녕하세요 좋은 하루 되세요.!<br>"
    assert remove_html_tags(text) == "안녕하세요 좋은 하루 되세요.!"
