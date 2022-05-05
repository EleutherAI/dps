import pytest
import yaml
from dps.spark.jobs.massivetext_quality_filtering import MassiveTextFilters
import random
import string


class TestMassivTextFilters:

    # config for test code
    config = {
        "doc_len_filter": {"min": 50, "max": 100},
        "mean_word_len_filter": {"min": 3, "max": 10},
        "symbol_to_word_ratio_filter": {"ratio": 0.01},
        "bullet_ellipsis_filter": {"bullet_point_ratio": 0.9, "ellipsis_ratio": 0.3},
        "alphabetic_word_ratio_filter": {"ratio": 0.8},
        "least_k_essential_words_filter": {
            "k": 2,
            "word_list": ["the", "be", "to", "of", "and", "that", "have", "with"],
        },
    }

    massiveTextFilters = MassiveTextFilters(config)

    def test_doc_len_filter(self):
        # case 1 : too short
        example = {"text": "친환경업체(한살림 등)의 DDT파동으로 또 다시 술렁였습니다."}
        assert self.massiveTextFilters.doc_len_filter(example) == False

        # case 2 : rigt size
        example = {
            "text": "얼마 전 살충제 계란 파동 등으로 씨끄러웠던 중 친환경업체(한살림 등)의 DDT파동으로 또 다시 술렁였습니다."
        }
        assert self.massiveTextFilters.doc_len_filter(example) == True

        # case 3 : too long
        example = {
            "text": "초과근무 시간을 연가일수로 산입 전환 □ 추진배경 ㅇ 공직의 생산성 및 경쟁력 향상을 위해 도입된 연가저축제 등 여러 가지 휴가 제도를 활성화 및 보완하기 위한 방안 □ 현황 및 문제점"
        }
        assert self.massiveTextFilters.doc_len_filter(example) == False

    def test_mean_word_len_filter(self):
        # case 1 : mean word length too short
        example = {"text": "월. 화. 수. 목. 금. 토."}
        assert self.massiveTextFilters.mean_word_len_filter(example) == False

        # case 2 : mean word length rigt size
        example = {"text": "친환경업체(한살림 등)의 DDT파동으로 또 다시 술렁였습니다."}
        assert self.massiveTextFilters.mean_word_len_filter(example) == True

        # case 3 : mean word length too long
        example = {"text": "얼마전살충제계란파동등으로씨끄러웠던중 친환경업체(한살림등)의DDT파동으로또다시술렁였습니다."}
        assert self.massiveTextFilters.mean_word_len_filter(example) == False

    def test_symbol_to_word_ratio_filter(self):
        # case 1 : too high symbol ratio
        example = {"text": "아니... 이건 아니잖아요… #시험기간#넋두리"}
        assert self.massiveTextFilters.symbol_to_word_ratio_filter(example) == False

        # case 2 : right amount of symbol ratio
        example = {"text": "아니 이건 아니잖아요 시험기간,넋두리"}
        assert self.massiveTextFilters.symbol_to_word_ratio_filter(example) == True

    def test_bullet_ellipsis_filter(self):
        # case 1 : too high bullet & ellipsis symbol ratio
        example = {
            "text": "보리스 존슨 영국 총리와 볼로디미르 젤렌스키 우크라이나 대통령이 5일(현지시간) 통화를 하고 ...\n북한 미사일 규탄…기시다 개인 140명 등 대러 추가...\n 블라디미르 푸틴 러...\n제니퍼 그랜홈 미 에너지부 장...\n..."
        }
        assert self.massiveTextFilters.bullet_ellipsis_filter(example) == False

        # case 4 : right amount of symbol ratio
        example = {"text": "친환경업체(한살림 등)의 DDT파동으로 또 다시 술렁였습니다."}
        assert self.massiveTextFilters.bullet_ellipsis_filter(example) == True

    def test_alphabetic_word_ratio_filter(self):
        # case 1 : too many non-alphabetic-character only words
        example = {"text": "！ ＇ ， ． ￣ ： ； ‥ … ¨ 〃 ― ∥ ＼ ∼ ´ ～ ˇ ˘ ˝ ˚ ˙ ¸ ˛ ¡ ¿ ː"}
        assert self.massiveTextFilters.alphabetic_word_ratio_filter(example) == False

        # case 4 : right amount of alphabetic word ratio
        example = {"text": "친환경업체(한살림 등)의 DDT파동으로 또 다시 술렁였습니다."}
        assert self.massiveTextFilters.alphabetic_word_ratio_filter(example) == True

    def test_least_k_essential_words_filter(self):
        # case 1 : too many non-alphabetic-character only words
        example = {
            "text": "Table tennis at 1962 Asian Games Table tennis was contested at 1962 Asian Games at Istora Senayan in Jakarta, Indonesia, from 25 August 1962 31 August 1962."
        }
        assert self.massiveTextFilters.least_k_essential_words_filter(example) == False

        # case 4 : right amount of alphabetic word ratio
        example = {
            "text": "Table tennis at the 1962 Asian Games Table tennis was contested at the 1962 Asian Games at the Istora Senayan in Jakarta, Indonesia, from 25 August 1962 to 31 August 1962. "
        }
        assert self.massiveTextFilters.least_k_essential_words_filter(example) == True
