import re

CN_BEGIN = 19968
CN_END = 40959

# SIMPLIFIED_CHINESE = '一-鿕'  #  U+4E00 ~ U+9FFF
# SIMPLIFIED_CHINESE = '\u4E00-\u9FFF\u3400-\u4DBF'  # [\u4e00-\u9FFF]

# TRADITIONAL_CHINESE = '一-龥'
# TRADITIONAL_CHINESE = '\u4E00-\u9FFF\uF900-\uFAFF'

# CHINESE_CHARS = f'{SIMPLIFIED_CHINESE}{TRADITIONAL_CHINES}'

# HTML PROCESSING
HTML_REMOVE = [
    "javascript",
    "/",
    "#",
    "*",
    "![",
    "[!",
    "[(",
    ")]",
    "[]",
    "()",
    ";",
    "=",
    "__",
]

# HTML_SPLIT

# PHONE NUMBER
# https://m.blog.naver.com/PostView.naver?isHttpsRedirect=true&blogId=donghoiee&logNo=220083830808
# In Chinese mobile phone numbers, the second digit can be any one of seven digits[3456789].
PHONE_NUMBER_PATTERN = re.compile(r"(?:\+86)?1[3456789]\d{9}")

# RRN
# Chinese landline phone numbers
RRN_PATTERN = re.compile(r"0\d{2}-\d{8}|0\d{3}-\d{7,8}")

# CARD
CARD_PATTERN = re.compile(r"\b(?:\d[ -]*?){13,16}\b")

# wiki footnote
# Only used on Wiki text
# example) ^1  (wiki_ca_zh.jsonl)
# ^[1]
# 문장 시작 ^
# ^–
# ^3—
# ^1—
# ^1——
# ^1–
# ^1,2,3—
# "威尔曾获得以下荣誉和嘉奖：^"  : https://zh.wikipedia.org/wiki/%E6%96%AF%E5%9D%A6%E5%88%A9%C2%B7%E6%99%AE%E8%8E%B1%E6%96%AF%C2%B7%E5%A8%81%E5%B0%94
footnote_PATTERN = re.compile(r"(^|[1-9](,[1-9]){0,6}|\[[1-9]\])(—){0,2}")

# BANK ACCOUNT

# SPAM FILTERING

# _WEBSITE_FOOTER

# SPAM_SPLIT

# SPAM_REMOVE

# REPEATED_CN = re.compile(r"[一-鿕]{20,}")

# https://www.jumpspeak.com/blog/chinese-swear-words
BAD_WORDS = [
    "高丽棒子",
    "韩国棒子" "小西八",
    "日本鬼子",
    "傻逼",
    "肏你",
    "舔狗",
    "賤人",
    "日你大爷",
    "骚货",
    "你妈逼",
    "肏你妈",
    "坏蛋",
    "笨蛋 ",
    "王八蛋",
    "滚蛋",
    "糊涂蛋",
    "混蛋",
    "滚开",
    "贱女人",
    "变态",
    "肏你妈",
    "f**k",
    "fuck",
]
