import random
import re

from stdnum import bitcoin

# URL
URL_PATTERN = re.compile(r"""\b((?:https?://)?(?:(?:www\.)?(?:[\da-z\.-]+)\.(?:[a-z]{2,6})|(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)|(?:(?:[0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|(?:[0-9a-fA-F]{1,4}:){1,7}:|(?:[0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|(?:[0-9a-fA-F]{1,4}:){1,5}(?::[0-9a-fA-F]{1,4}){1,2}|(?:[0-9a-fA-F]{1,4}:){1,4}(?::[0-9a-fA-F]{1,4}){1,3}|(?:[0-9a-fA-F]{1,4}:){1,3}(?::[0-9a-fA-F]{1,4}){1,4}|(?:[0-9a-fA-F]{1,4}:){1,2}(?::[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:(?:(?::[0-9a-fA-F]{1,4}){1,6})|:(?:(?::[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(?::[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(?:ffff(?::0{1,4}){0,1}:){0,1}(?:(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9])|(?:[0-9a-fA-F]{1,4}:){1,4}:(?:(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9])))(?::[0-9]{1,4}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])?(?:/[\w\.-]*)*/?)\b""")


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


# BITCOIN PATTERN
BITCOIN_PATTERN = (
    r"( [13] ["
    + bitcoin._base58_alphabet
    + "]{25,34}"
    + "| bc1 ["
    + bitcoin._bech32_alphabet
    + "]{8,87})"
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
