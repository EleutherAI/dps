# Make the URL regular expression

import re

# Spanish
SPANISH_PATTERN = "[a-záéíóúñ]+"

# Portuguese
PORTUGUESE_PATTERN = "[a-zà-ú]+"

# French
FRENCH_PATTERN = "[a-zà-ÿœŒ]+"

# Italian
ITALIAN_PATTERN = "[a-zà-ÿ]+"

# Romanian
ROMANIAN_PATTERM = "[a-zăâîșț]+"

# National Identification Number (NIF in Spain)
SPAIN_NIN_PATTERN = re.compile(r"^[0-9]{8}[ABCDEFGHJKLMNPQRSUVWXY]$")

# Codice Fiscale in Italy
ITALY_NIN_PATTERN = re.compile(r"^[A-Z]{6}[0-9]{2}[ABCDEHLMPRST][0-9]{2}[A-Z][0-9]{3}[A-Z]$")

# France INSEE code
FRANCE_NIN_PATTERN = re.compile(r"^[1-37-8][0-9]{2}(0[1-9]|1[0-2])(0[1-9]|[1-2][0-9]|3[0-1])[0-9]{3}(0[1-9]|[1-9][0-9])$")

# CNP (Cod Numeric Personal) in Romania
ROMANIA_NIN_PATTERN = re.compile(r"^[1-8][0-9]{12}$")

# Portugal NIF
PORTUGAL_NIN_PATTERN = re.compile(r"^[1-9][0-9]{8}$")

NATIONAL_ID_PATTERN = re.compile(SPAIN_NIN_PATTERN.pattern + "|" +
                                 ITALY_NIN_PATTERN.pattern + "|" +
                                 FRANCE_NIN_PATTERN.pattern + "|" +
                                 ROMANIA_NIN_PATTERN.pattern + "|" +
                                 PORTUGAL_NIN_PATTERN.pattern)

# Telephone numbers in Spain
SPAIN_PHONE_PATTERN = re.compile(r"^(?:\+34|0034|34)?[6|7|9][0-9]{8}$")

# Telephone numbers in Italy
ITALY_PHONE_PATTERN = re.compile(r"^(?:\+39|0039|39)?[3|6|7|8|9][0-9]{8}$")

# Telephone numbers in France
FRANCE_PHONE_PATTERN = re.compile(r"^(?:\+33|0033|33)?[1-9][0-9]{8}$")

# Telephone numbers in Romania
ROMANIA_PHONE_PATTERN = re.compile(r"^(?:\+40|0040|40)?[7|2|3][0-9]{8}$")

# Telephone numbers in portugal
PORTUGAL_PHONE_PATTERN = re.compile(r"^(?:\+351|00351|351)?[9][0-9]{8}$")

PHONE_NUMBER_PATTERN = re.compile(SPAIN_PHONE_PATTERN.pattern + "|" +
                                  ITALY_PHONE_PATTERN.pattern + "|" +
                                  FRANCE_PHONE_PATTERN.pattern + "|" +
                                  ROMANIA_PHONE_PATTERN.pattern + "|" +
                                  PORTUGAL_PHONE_PATTERN.pattern)

# BAD WORDS in Spanish
BAD_WORDS_SPANISH = [
    "estupido",
    "culero",
    "mierda",
    "caraculo",
    "zoquete",
    "puta",
    "put4",
    "puta",
    "vergazos",
    "mamon",
    "puta madre",
    "cabron",
    "hijo de perra",
    "hijo de puta",
    "hijo de la gran puta",
    "asqueroso",
    "idiota",
    "gilipollas",
    "anormal"
]

# BAD WORDS in Italian
BAD_WORDS_ITALIAN = [
    "merda",
    "merdoso",
    "merdaccia",
    "bocchino",
    "anale",
    "fica",
    "fottuto",
    "fottuta"
]

# BAD WORDS in French
BAD_WORDS_FRENCH = [
    "enculé",
    "anale",
    "bite",
    "bouffon",
    "jouir",
    "merde",
    "lèche"
]

# BAD WORDS in Portuguese
BAD_WORDS_PORTUGUESE = [
    "merda",
    "merdoso",
    "buceta"
]

# BAD WORDS in Romanian
BAD_WORDS_ROMANIAN = [
    "merda",
    "jucarie"
]

# BAD WORLD in all romance languages
BAD_WORDS_ROMACE = BAD_WORDS_SPANISH + BAD_WORDS_ITALIAN + BAD_WORDS_FRENCH + BAD_WORDS_PORTUGUESE + BAD_WORDS_ROMANIAN

# HTML SPLIT
HTML_SPLIT_PATTERN = re.compile(r"(<[^>]*>)")

SPANISH_HTML_SPLIT = [
    "ver más",
    "ver más información",
    "más información",
    "ampliar información",
    "más detalles"
]

ITALIAN_HTML_SPLIT = [
    "vedi di più",
    "scopri di più",
    "mostra di più",
    "maggiori informazioni",
    "ulteriori dettagli"
]


FRENCH_HTML_SPLIT = [
    "vezi mai mult",
    "află mai multe",
    "mai multe informații",
    "detalii suplimentare"
]

PORTUGUESE_HTML_SPLIT = [
    "ver mais",
    "ver mais informações",
    "mais informações",
    "ampliar informações",
    "mais detalhes"
]

ROMANIAN_HTML_SPLIT = [
    "vezi mai mult",
    "află mai multe",
    "mai multe informații",
    "detalii suplimentare"
]

ROMANCE_HTML_SPLIT = SPANISH_HTML_SPLIT + ITALIAN_HTML_SPLIT + FRENCH_HTML_SPLIT + PORTUGUESE_HTML_SPLIT + ROMANIAN_HTML_SPLIT


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


