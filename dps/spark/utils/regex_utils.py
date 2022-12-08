import re

# URL
_URL_PREFIXES = "https|http|mailto|ftp"
_URL_SUFFIXES = (
    "com|net|org|edu|game|ebiz|club|gov|mil|aero|asia|biz|cat|coop|info|int|jobs|mobi"
    "|museum|name|post|pro|tel|travel|xxx|ac|ad|ae|af|ag|ai|al|am|an|ao|aq|ar|as|at|au|aw|ax|az|ba|bb|bd"
    "|be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cd|cf|cg|ch|ci|ck|cl|cm|cn|co|cr|cs|cu|cv|cx"
    "|cy|cz|dd|de|dj|dk|dm|do|dz|ec|ee|eg|eh|er|es|et|eu|fi|fj|fk|fm|fo|fr|ga|gb|gd|ge|gf|gg|gh|gi|gl|gm"
    "|gn|gp|gq|gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|in|io|iq|ir|is|it|je|jm|jo|jp|ke|kg|kh|ki"
    "|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|ly|ma|mc|md|me|mg|mh|mk|ml|mm|mn|mo|mp|mq|mr|ms"
    "|mt|mu|mv|mw|mx|my|mz|na|nc|ne|nf|ng|ni|nl|no|np|nr|nu|nz|om|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|ps|pt|pw"
    "|py|qa|re|ro|rs|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj|Ja|sk|sl|sm|sn|so|sr|ss|st|su|sv|sx|sy|sz|tc|td|tf"
    "|tg|th|tj|tk|tl|tm|tn|to|tp|tr|tt|tv|tw|tz|ua|ug|uk|us|uy|uz|va|vc|ve|vg|vi|vn|vu|wf|ws|ye|yt|yu|za"
    "|zm|zw|mp3|mp4|avi|html|idv|jpg|bmp|png|gif|htm|cdn|media"
)
URL_PATTERN = re.compile(
    rf"""(?i)\b((?:{_URL_PREFIXES}?:(?:/{{1,3}}|[a-z0-9%])|[a-z0-9.\-]+[.](?:{_URL_SUFFIXES})/)(?:[^\s()<>{{}}\[\]]+|\([^\s()]*?\([^\s()]+\)[^\s()]*?\)|\([^\s]+?\))+(?:\([^\s()]*?\([^\s()]+\)[^\s()]*?\)|\([^\s]+?\)|[^\s`!()\[\]{{}};:'".,<>?«»“”‘’])|(?:(?<!@)[a-z0-9]+(?:[.\-][a-z0-9]+)*[.](?:{_URL_SUFFIXES})\b/?(?!@)))""",
)
URL_TOKEN = "<|url|>"

# EMAIL
EMAIL_PATTERN = re.compile(
    r"[a-z0-9.\-+_]+@[a-z0-9.\-+_]+\.[a-z]+|[a-z0-9.\-+_]+@[a-z0-9.\-+_]+\.[a-z]+\.[a-z]"
)
EMAIL_TOKEN = "<|email|>"

# SPAM FILTERING
_NEWS_1 = [
    "<저작권자(c) 연합뉴스, 무단 전재-재배포 금지>",
    "<저작권자(c) AP연합뉴스, 무단 전재-재배포 금지>",
    "〈ⓒ 대한경제신문(<|url|>), 무단전재 및 수집, 재배포금지〉",
    "( KTV 국민방송 케이블방송, 위성방송 ch164, <|url|> )",
    "< ⓒ 한국정책방송원 무단전재 및 재배포 금지 >",
    "<저작권자(c)연합인포맥스. 무단 전재-재배포 금지.>",
    "<저작권자(c)로이터연합뉴스, 무단 전재-재배포 금지>",
    "저작권자 ⓒ 비전성남, 무단전재 및 재배포금지",
    "<저작권자(c) 비즈니스포스트 무단전재 및 재배포금지>",
    "Copyright ⓒ 뉴스1. All rights reserved. 무단 전재 및 재배포 금지.",
    "[조세금융신문(<|url|>), 무단전재 및 재배포 금지]",
    "ⓒ 부산일보(<|url|>), 무단전재 및 수집, 재배포금지",
    "저작권자 © 디지털투데이 (DigitalToday) 무단전재 및 재배포 금지",
    "저작권자 © 노동과세계 무단전재 및 재배포 금지",
    "저작권자 © 골프한국 무단전재 및 재배포 금지",
    "저작권자 © 메디소비자뉴스 무단전재 및 재배포 금지",
    "저작권자 | 가스신문 무단전재 및 재배포 금지",
    "저작권자 © 전북일보 인터넷신문 무단전재 및 재배포 금지",
    "[ⓒ 매일경제 & <|url|>, 무단전재 및 재배포 금지]",
    "© 텐아시아, 무단전재 및 재배포 금지",
    "ⓒ중앙일보(<|url|>), 무단 전재 및 재배포 금지",
    "- Copyrights ⓒ 디지털세정신문 & <|url|>, 무단 전재 및 재배포 금지 -",
    "ⓒ 폴리뉴스(<|url|>), 무단전재 및 재배포금지",
    "폴리뉴스는 인터넷신문위원회의 인터넷신문 윤리강령을 준수합니다.",
    "[사진] ⓒGettyimages(무단전재 및 재배포 금지)",
    "<저작권자 ⓒ FPN(소방방재신문사ㆍ119플러스) 무단전재 및 재배포 금지>",
    "<저작권자 © ‘돈이 보이는 리얼타임 뉴스’ 머니투데이, 무단전재 및 재배포 금지>",
    "저작권자 © Best Eleven 무단전재 및 재배포 금지",
    "<저작권자 ⓒ 1980-2022 ㈜연합뉴스. 무단 전재 재배포 금지.>",
    "<저작권자ⓒ 공감언론 뉴시스통신사. 무단전재-재배포 금지.>",
    "저작권자 © 시사IN 무단전재 및 재배포 금지",
    "▶DAUM에서 [채널A 뉴스] 구독하기 (모바일)",
    "▶NAVER에서 [채널A 뉴스] 구독하기",
]

_NEWS_2 = [
    f"{prefix}{word}"
    for word in [
        "Copyright",
        "Copyrights",
        "저작권자©",
        "저작권자 ©",
        "저작권자ⓒ",
        "저작권자 ⓒ",
        "저작권자(c)",
        "저작권자 (c)",
        "저작권자:",
        "저작권자 :",
        "저작권자|",
        "저작권자 |",
        "©",
        "ⓒ",
        "무단전재",
        "무단 전재",
        "방송된 기사 내용은",
        "촬영기자:",
        "촬영기자 :",
        "영상편집:",
        "영상편집 :",
        "▷ 카카오톡:",
        "▷ 카카오톡 :",
        "카카오톡:",
        "카카오톡 :",
        "▷ 전화:",
        "▷ 전화 :",
        "전화:",
        "전화 :",
        "■ 제보하기",
        "연합뉴스TV 기사문의 및 제보",
        "제보는 카카오톡",
        "(끝)",
        "기사제보 및 보도자료 제공",
    ]
    for prefix in ["<", "< ", "〈", "〈 ", "[", "[ ", "(", "( ", ""]
]
_WEBSITE_FOOTER = [
    f"{prefix}{word}{suffix}"
    for word in [
        "이메일",
        "TEL",
        "FAX",
        "PHONE",
        "EMAIL",
        "전화번호",
        "전화 번호",
        "주소",
        "대표자명",
        "대표자 명",
        "사업자등록번호",
        "사업자 등록번호",
        "사업자등록 번호",
        "사업자 등록 번호",
        "대표이사",
        "대표 이사",
        "통신판매번호",
        "통신 판매번호",
        "통신판매 번호",
        "통신 판매 번호",
    ]
    for prefix in ["", "|", "| "]
    for suffix in [":", " :"]
]

RE_SPLIT = [
    # 지금까지 (공백 0~6개) ~~기자였습니다.
    re.compile(
        r"(?i)(?:\b(지금까지|.{2,7}에서)\b)\W+(?:\w+[^\w]+){0,6}?\b(기자|특파원|교통정보|KBS 뉴스|SBS 뉴스|MBC 뉴스|YTN|MBN|뉴스더하기)"
    ),
    # KBS 뉴스 (공백 0~3개) ~~입니다.
    re.compile(
        r"(?i)(?:\b(KBS 뉴스|SBS 뉴스|MBC 뉴스|YTN|MBN)\b)\W+(?:\w+){0,3}?(였습니다\.|입니다\.).*"
    ),
    re.compile("|".join([re.escape(p) for p in _NEWS_1 + _NEWS_2 + _WEBSITE_FOOTER])),
]

RE_SUB = [
    (
        # (서울=연합뉴스) ~~기자 =
        # 사진=연합뉴스
        re.compile("\(+.+=연합뉴스\)+.+(기자 =|특파원 =)|=연합뉴스|"),
        "",
    ),
    # yyyy.mm.dd <|email|>
    (
        re.compile("([0-9]{4}\.[0-9]{1,2}\.[0-9]{1,2})( |\w)(<\|email\|>|<\|url\|>)"),
        "",
    ),
    # ~~했다. <|email|>
    (
        re.compile("(다\. <\|email\|>)|다\. <\|url\|>"),
        "다.",
    ),
    # ~~~기자 <|email|>
    (
        re.compile("기자 <\|email\|>"),
        "기자",
    ),
]
