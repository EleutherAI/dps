from dps.spark.utils.preprocessing import *


def test(text: str, methods: List) -> None:
    if methods is None:
        preprocessed_text = preprocess_text(text)
        print(f"Before: {text}")
        print(f"After: {preprocessed_text}")
    else:
        preprocessed_text = preprocess_text(text, methods)
        print(f"Before: {text}")
        print(f"After: {preprocessed_text}")


if __name__ == "__main__":
    text = '<br>안녕하세요,       내 이름은 이순신이야!!! 내 주민등록번호는 100022-1133098이고, 내 이메일 주소는 soonsin@gmail.com 이고 전화번호는 02-211-0223 이야   <br>'
    preprocess_methods = [reduce_emoticon, replace_phone_number, replace_rrn, remove_whitespace, strip_html_tags]
    test(text, preprocess_methods)