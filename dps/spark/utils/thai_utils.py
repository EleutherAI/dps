# TOP 20 BAD THAI WORDS from 
# https://www.thailandredcat.com/wp-content/uploads/2014/09/TOP-20-BAD-THAI-WORDS.pdf
BAD_WORD_LIST = [
    "กู",
    "มึง",
    "มัน",
    "ควาย",
    "สัตว์",
    "ดาก",
    "เย็ด",
    "น่าเย็ด",
    "เงี่ยน",
    "จู๋",
    "หา",
    "หี",
    "แตด",
    "หมอย",
    "กระหรี่",
    "ปากหมา",
    "อี",
    "ไอ้",
    "แม่มึงตาย",
    "พ่อมึงตาย"
]

with open('stopwords_th.txt', 'r') as f:
    THAI_FREQ_CHAR_LIST = []
    for line in f:
        THAI_FREQ_CHAR_LIST.append(line.strip())