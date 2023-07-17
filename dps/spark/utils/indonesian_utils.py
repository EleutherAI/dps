import re


INDONESIAN_NIN_PATTERN = re.compile(r'((1[1-9])|(21)|([37][1-6])|(5[1-4])|(6[1-5])|([8-9][1-2]))[0-9]{2}[0-9]{2}(([0-6][0-9])|(7[0-1]))((0[1-9])|(1[0-2]))([0-9]{2})[0-9]{4}')
MALAYSIA_NIN_PATTER = re.compile(r"\d{6}-\d{2}-\d{4}")

NATIONAL_ID_PATTERN = re.compile(INDONESIAN_NIN_PATTERN.pattern + "|" +
                                 MALAYSIA_NIN_PATTER.pattern)


PHONE_NUMBER_PATTERN = re.compile( r'((\+65|65|60|+60)(6|8|9)\d{7})|((\+62\s?|08|628\s?|\+\s?)(\d{1,4}-?|\d{2,4}\.?|\d{1,5}\ ?)(\d|\ |\-|\.){4,5}\d{3,5})|((?:(?:\+|0{0,2})91(\s*[\-]\s*)?|[0]?)?[789]\d{9})')


BAD_WORDS_INDONESIA = ['sexchat',
'gratis porno',
'porno anaal',
'live flirts',
'porno webcam',
'webcam sex',
'gratis sex',
"xxx",
"lotto",
"poker",
"porn",
"bokep",
"sange",
"togel",
"memek",
"kontol",
"dientot",
"qiuqiu",
"betting",
"taruhan",
"cock",
"toket",
"video jav",
"video porn",
"pussy",
"domino",
"blowjob",
"cerita seks",
"cerita sex",
"teen sex",
"milf",
"doggy style",
"squirt",
"fuck",
"hentai",
"cumshot",
"rape",
"colmek",
"coli",
"masturb",
"tetek",
"entot",
"bispak",
"nyepong",
"sepong",
"jablay",
"ngewe",
"jilbab hot",
"lonte",
"fortamen",
"slot online",
"vimax",
"vig power",
"ceritasex",
"cerita dewasa",
"sabung ayam",
"agen bola",
"bugil",
"judionline",
"agenjudi"
]

BAD_WORDS_MALAYSIA = [
    "mak kau hijau",
    "eh butoh uh babi",
    "pungkoq hang"
    "pukimak"
]

BAD_WORDS_INDONESIA_MALAYSIA  = BAD_WORDS_INDONESIA + BAD_WORDS_MALAYSIA