import itertools
import random
import re

from stdnum import bitcoin

# URL
URL_PATTERN = re.compile(
    r"""\b((?:https?://)?(?:(?:www\.)?(?:[\da-z\.-]+)\.(?:[a-z]{2,6})|(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)|(?:(?:[0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|(?:[0-9a-fA-F]{1,4}:){1,7}:|(?:[0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|(?:[0-9a-fA-F]{1,4}:){1,5}(?::[0-9a-fA-F]{1,4}){1,2}|(?:[0-9a-fA-F]{1,4}:){1,4}(?::[0-9a-fA-F]{1,4}){1,3}|(?:[0-9a-fA-F]{1,4}:){1,3}(?::[0-9a-fA-F]{1,4}){1,4}|(?:[0-9a-fA-F]{1,4}:){1,2}(?::[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:(?:(?::[0-9a-fA-F]{1,4}){1,6})|:(?:(?::[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(?::[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(?:ffff(?::0{1,4}){0,1}:){0,1}(?:(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9])|(?:[0-9a-fA-F]{1,4}:){1,4}:(?:(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9])))(?::[0-9]{1,4}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])?(?:/[\w\.-]*)*/?)\b"""
)

# EMAIL
EMAIL_PATTERN = re.compile(
    r"[a-z0-9.\-+_]+@[a-z0-9.\-+_]+\.[a-z]+|[a-z0-9.\-+_]+@[a-z0-9.\-+_]+\.[a-z]+\.[a-z]"
)

# IP PATTERN
IP_PATTERN = re.compile(
    r"""
     \b
     (?: (?: 25[0-5] | 2[0-4][0-9] | [01]?[0-9][0-9]? ) \. ){3}
     (?: 25[0-5] | 2[0-4][0-9] | [01]?[0-9][0-9]?)
     \b
"""
)

# BANK ACCOUNT
BANK_ACCOUNT_PATTERN = re.compile(
    r"([0-9]\d{13})|([0-9,\-]{3,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2,6})"  # IBK
    + r"([0-9]\d{13})|([0-9,\-]{3,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2,6})"  # KB
    + r"([0-9]\d{12})|([0-9,\-]{3,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2})"  # NH
    + r"([0-9]\d{11})|([0-9,\-]{3,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2,6})"  # SHINHAN
    + r"([0-9]\d{12})|([0-9,\-]{3,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2,6})"  # WOORI
    + r"([0-9]\d{13})|([0-9,\-]{3,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2,6})"  # KEB
    + r"([0-9]\d{11})|([0-9,\-]{3,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2,6})"  # CITI
    + r"([0-9]\d{11})|([0-9]\d{11})|([0-9,\-]{3,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2,6}\-[0-9,\-])"  # DGB
    + r"([0-9]\d{12})|([0-9]\d{12})|([0-9,\-]{3,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2})"  # BNK
    + r"([0-9]\d{10})|([0-9]\d{10})|([0-9,\-]{3,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2,6})"  # SC
    + r"([0-9]\d{11})|([0-9]\d{10})|([0-9,\-]{3,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2,6})"  # KBANK
    + r"([0-9]\d{12})|([0-9]\d{10})|([0-9,\-]{3,6}\-[0-9,\-]{2,6}\-[0-9,\-]{2,7})"  # KAKAO
)

CREDIT_CARD_PATTERN = re.compile(
    r"^(4026|417500|4405|4508|4844|4913|4917)\d+$|"  # VISA Electron
    + r"^(?:50|5[6-9]|6[0-9])\d+$|"  # Maestro
    + r"^(5019|4571)\d+$|"  # Dankort
    + r"^(62|81)\d+$|"  # China UnionPay
    + r"^4[0-9]\d+$|"  # Visa
    + r"^(?:5[1-5]|222[1-9]|22[3-9][0-9]|2[3-6][0-9][0-9]|27[0-1][0-9]|2720)\d+$|"  # MasterCard
    + r"^(34|37)\d+$|"  # American Express
    + r"^6(?:011|22(12[6-9]|1[3-9][0-9]|[2-8][0-9][0-9]|9[01][0-9]|92[0-5])|5|4|2[4-6][0-9]{3}|28[2-8][0-9]{2})\d+$|"  # Discover
    + r"^(35[2-8][0-9])\d+$|"  # JCB
    + r"^(636)\d+$|"  # InterPayment
    + r"^9[0-9]\d+$|"  # KOREAN
    + r"^(220[0-4])\d+$"  # MIR
)

# From https://github.com/unicode-org/cldr/blob/release-26-0-1/common/supplemental/postalCodeData.xml
# Extracting the most common to avoid the checking of 158 countries
ZIP_PATTERN = re.compile(
    r"GIR[ ]?0AA|((AB|AL|B|BA|BB|BD|BH|BL|BN|BR|BS|BT|CA|CB|CF|CH|CM|CO|CR|CT|CV|CW|DA|DD|DE|DG|DH|DL|DN|DT|DY|E|EC|EH|EN|EX|FK|FY|G|GL|GY|GU|HA|HD|HG|HP|HR|HS|HU|HX|IG|IM|IP|IV|JE|KA|KT|KW|KY|L|LA|LD|LE|LL|LN|LS|LU|M|ME|MK|ML|N|NE|NG|NN|NP|NR|NW|OL|OX|PA|PE|PH|PL|PO|PR|RG|RH|RM|S|SA|SE|SG|SK|SL|SM|SN|SO|SP|SR|SS|ST|SW|SY|TA|TD|TF|TN|TQ|TR|TS|TW|UB|W|WA|WC|WD|WF|WN|WR|WS|WV|YO|ZE)(\d[\dA-Z]?[ ]?\d[ABD-HJLN-UW-Z]{2}))|BFPO[ ]?\d{1,4}|"
    + r"JE\d[\dA-Z]?[ ]?\d[ABD-HJLN-UW-Z]{2}|"  # Jersey
    + r"GY\d[\dA-Z]?[ ]?\d[ABD-HJLN-UW-Z]{2}|"  # Guernsey
    + r"IM\d[\dA-Z]?[ ]?\d[ABD-HJLN-UW-Z]{2}|"  # IM
    + r"\d{5}([ \-]\d{4})?|"  # US ZIP
    + r"[ABCEGHJKLMNPRSTVXY]\d[ABCEGHJ-NPRSTV-Z][ ]?\d[ABCEGHJ-NPRSTV-Z]\d|"  # Canada
    + r"\d{5}|"  # DE, IT, ES, FI, DZ
    + r"\d{3}-\d{4}|"  # JP
    + r"\d{2}[ ]?\d{3}|"  # FR
    + r"\d{4}|"  # AU, CH, AT, BE, DK, NO, AZ, BD
    + r"\d{4}[ ]?[A-Z]{2}|"  # NL
    + r"\d{3}[ ]?\d{2}|"  # SE
    + r"\d{5}[\-]?\d{3}|"  # BR
    + r"\d{4}([\-]\d{3})?|"  # PT
    + r"\d{3}[\-]\d{3}"  # KR
)

# BITCOIN PATTERN
BITCOIN_PATTERN = (
        r"( [13] ["
        + bitcoin._base58_alphabet
        + "]{25,34}"
        + "| bc1 ["
        + bitcoin._bech32_alphabet
        + "]{8,87})"
)

# https://stackoverflow.com/questions/827557/how-do-you-validate-a-url-with-a-regular-expression-in-python
ul = '\u00a1-\uffff'  # Unicode letters range (must not be a raw string).

# IP patterns
IPV4_PATTERN = r'(?:0|25[0-5]|2[0-4]\d|1\d?\d?|[1-9]\d?)(?:\.(?:0|25[0-5]|2[0-4]\d|1\d?\d?|[1-9]\d?)){3}'
IPV6_PATTERN = r'\[?((([0-9A-Fa-f]{1,4}:){7}([0-9A-Fa-f]{1,4}|:))|(([0-9A-Fa-f]{1,4}:){6}(:[0-9A-Fa-f]{1,' \
    r'4}|((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3})|:))|(([0-9A-Fa-f]{' \
    r'1,4}:){5}(((:[0-9A-Fa-f]{1,4}){1,2})|:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[' \
    r'0-4]\d|1\d\d|[1-9]?\d)){3})|:))|(([0-9A-Fa-f]{1,4}:){4}(((:[0-9A-Fa-f]{1,4}){1,' \
    r'3})|((:[0-9A-Fa-f]{1,4})?:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[' \
    r'1-9]?\d)){3}))|:))|(([0-9A-Fa-f]{1,4}:){3}(((:[0-9A-Fa-f]{1,4}){1,4})|((:[0-9A-Fa-f]{1,4}){0,' \
    r'2}:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}))|:))|(([' \
    r'0-9A-Fa-f]{1,4}:){2}(((:[0-9A-Fa-f]{1,4}){1,5})|((:[0-9A-Fa-f]{1,4}){0,3}:((25[0-5]|2[' \
    r'0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}))|:))|(([0-9A-Fa-f]{1,4}:){1}(((:[' \
    r'0-9A-Fa-f]{1,4}){1,6})|((:[0-9A-Fa-f]{1,4}){0,4}:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[' \
    r'0-4]\d|1\d\d|[1-9]?\d)){3}))|:))|(:(((:[0-9A-Fa-f]{1,4}){1,7})|((:[0-9A-Fa-f]{1,4}){0,' \
    r'5}:((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}))|:)))(%.+)?\]?'


# Host patterns
HOSTNAME_PATTERN = r'[a-z' + ul + r'0-9](?:[a-z' + ul + r'0-9-]{0,61}[a-z' + ul + r'0-9])?'
# Max length for domain name labels is 63 characters per RFC 1034 sec. 3.1
DOMAIN_PATTERN = r'(?:\.(?!-)[a-z' + ul + r'0-9-]{1,63}(?<!-))*'
TLD_PATTERN = (
        r'\.'                                # dot
        r'(?!-)'                             # can't start with a dash
        r'(?:[a-z' + ul + '-]{2,63}'         # domain label
        r'|xn--[a-z0-9]{1,59})'              # or punycode label
        r'(?<!-)'                            # can't end with a dash
        r'\.?'                               # may have a trailing dot
)

HOST_PATTERN = '(' + HOSTNAME_PATTERN + DOMAIN_PATTERN + TLD_PATTERN + '|localhost)'


URL2_PATTERN = re.compile(
    r'([a-z0-9.+-]*:?//)?'                                      # scheme is validated separately
    r'(?:[^\s:@/]+(?::[^\s:@/]*)?@)?'                           # user:pass authentication
    r'(?:' + IPV4_PATTERN + '|' + IPV6_PATTERN + '|' + HOST_PATTERN + ')'
    r'(?::\d{2,5})?'                                            # port
    r'(?:[/?#][^\s]*)?',                                        # resource path
    re.IGNORECASE
)


def replace_with_token(text, pattern, start, end, replaces, random_number=True):
    def replace_number(match):
        return str(random.randint(0, 9))

    found = re.findall(pattern, text)
    for one in found:
        if isinstance(one, str):
            one = [one]
        for o in one:
            if len(o) == 0:
                continue
            if random_number:
                replace = re.sub(r"\d", replace_number, o)
            else:
                replace = o
            replaces.append((f"{start}{end}", f"{start}{replace}{end}"))
            text = text.replace(o, f"{start}{end}")
    return text


def split_sentences(text):
    sents = re.split(r"\r\n|\n|\t|\v|\f", text)
    sents = [re.split(r"(?<=[.!?])\s", s) for s in sents]
    sents = list(itertools.chain(*sents))
    return sents
