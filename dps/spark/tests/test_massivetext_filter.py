from dps.spark.utils.massivetext_filter import (
    doc_len_filter,
    mean_word_len_filter,
    symbol_to_word_ratio_filter,
    bullet_ellipsis_filter,
    korean_word_ratio_filter,
    least_k_essential_words_filter,
    email_and_url_filter,
    spam_words_filter,
)


def test_doc_len_filter():
    # case 1 : too short
    example_1 = "친환경업체(한살림 등)의 DDT파동으로 또 다시 술렁였습니다."
    # case 2 : rigt size
    example_2 = "얼마 전 살충제 계란 파동 등으로 씨끄러웠던 중 친환경업체(한살림 등)의 DDT파동으로 또 다시 술렁였습니다."
    # case 3 : too long
    example_3 = "초과근무 시간을 연가일수로 산입 전환 □ 추진배경 ㅇ 공직의 생산성 및 경쟁력 향상을 위해 도입된 연가저축제 등 여러 가지 휴가 제도를 활성화 및 보완하기 위한 방안 □ 현황 및 문제점"

    assert doc_len_filter(example_1, 50, 100) is False
    assert doc_len_filter(example_2, 50, 100) is True
    assert doc_len_filter(example_3, 50, 100) is False


def test_mean_word_len_filter():
    # case 1 : mean word length too short
    example_1 = "월. 화. 수. 목. 금. 토."
    # case 2 : mean word length rigt size
    example_2 = "친환경업체(한살림 등)의 DDT파동으로 또 다시 술렁였습니다."
    # case 3 : mean word length too long
    example_3 = "얼마전살충제계란파동등으로씨끄러웠던중 친환경업체(한살림등)의DDT파동으로또다시술렁였습니다."

    assert mean_word_len_filter(example_1, 3, 10) is False
    assert mean_word_len_filter(example_2, 3, 10) is True
    assert mean_word_len_filter(example_3, 3, 10) is False


def test_symbol_to_word_ratio_filter():
    # case 1 : too high symbol ratio
    example_1 = "아니... 이건 아니잖아요… #시험기간#넋두리"
    # case 2 : right amount of symbol ratio
    example_2 = "아니 이건 아니잖아요 시험기간,넋두리"

    assert symbol_to_word_ratio_filter(example_1, 0.01) is False
    assert symbol_to_word_ratio_filter(example_2, 0.01) is True


def test_bullet_ellipsis_filter():
    # case 1 : too high bullet & ellipsis symbol ratio
    example_1 = (
        "보리스 존슨 영국 총리와 볼로디미르 젤렌스키 우크라이나 대통령이 5일(현지시간) 통화를 하고 ...\n북한 미사일 규탄…기시다 개인 140명 등 대러 추가...\n 블라디미르 푸틴 "
        "러...\n제니퍼 그랜홈 미 에너지부 장...\n... "
    )
    # case 4 : right amount of symbol ratio
    example_2 = "친환경업체(한살림 등)의 DDT파동으로 또 다시 술렁였습니다."
    assert bullet_ellipsis_filter(example_1, 0.9, 0.3) is False
    assert bullet_ellipsis_filter(example_2, 0.9, 0.3) is True


def test_alphabetic_word_ratio_filter():
    # case 1 : too many non-alphabetic-character only words
    example_1 = "！ ＇ ， ． ￣ ： ； ‥ … ¨ 〃 ― ∥ ＼ ∼ ´ ～ ˇ ˘ ˝ ˚ ˙ ¸ ˛ ¡ ¿ ː"
    # case 4 : right amount of alphabetic word ratio
    example_2 = "친환경업체(한살림 등)의 DDT파동으로 또 다시 술렁였습니다."
    assert korean_word_ratio_filter(example_1, 0.7) is False
    assert korean_word_ratio_filter(example_2, 0.7) is True


def test_least_k_essential_words_filter():
    k = 2
    word_list = ["the", "be", "to", "of", "and", "that", "have", "with"]

    # case 1 : too many non-alphabetic-character only words
    example_1 = (
        "Table tennis at 1962 Asian Games Table tennis was contested at 1962 Asian Games at Istora Senayan in "
        "Jakarta, Indonesia, from 25 August 1962 31 August 1962. "
    )
    # case 4 : right amount of alphabetic word ratio
    example_2 = (
        "Table tennis at the 1962 Asian Games Table tennis was contested at the 1962 Asian Games at the "
        "Istora Senayan in Jakarta, Indonesia, from 25 August 1962 to 31 August 1962. "
    )
    assert least_k_essential_words_filter(example_1, k, word_list) is False
    assert least_k_essential_words_filter(example_2, k, word_list) is True


def tet_spam_words_filter():
    """All examples came from real news data"""
    examples = [
        "검증의 객관성과 공정성이 훼손될 수 있다는 지적입니다. 지금까지 김성기 기자 였습니다. [앵커] 김기자 수고했습니다.",
        "여왕이 고소까지 하는 사태까지 왔다는 것은 이러한 정부와 언론의 대결의 심각성을 단적을 표현해주고 있습니다. 지금까지 정용석 특파원 였습니다.",
        "동작대교까지 잠실방향으로 구간 정체를 빚고 있습니다. 지금까지 실시간 교통정보였습니다. ■ 제보하기 ▷ 카카오톡 : 'KBS제보' 검색",
        "특정범죄가중처벌법 위반 혐의 등으로 구속영장을 신청했습니다. KBS 뉴스 공민경입니다. 촬영기자:안민식/영상편집:김선영 ▣ KBS 기사 원문보기 : http://news.kbs.co.kr/news/view.do?nc... ▣ 제보 하기",
        "그가 남긴 한국 축구의 유산을 이어 받게 될 차기 감독은 누가 될까요. 지금까지 '뉴스더하기'였습니다.",
        "서울청의 인지도 그만큼 늦어지게 된 것”이라고 설명했습니다. [사진 출처 : 홈페이지 캡처] ■ 제보하기 ▷ 카카오톡 : 'KBS제보' 검색 ▷ 전화 : 02-781-1234 ▷ 이메일 : kbs1234@kbs.co.kr ▷ 뉴스홈페이지 : https://goo.gl/4bWbkG",
        "‘16강 쾌거’ 벤투호 오늘 금의환향…내일 尹대통령과 만찬 Copyright ⓒ 동아일보 & donga.com",
        "(서울=연합뉴스) 이정훈 기자 = 7일 오후 국회에서 열린 성탄트리 점등식에서 참석자들이 점등 버튼을 누르고 있다. 2022.12.7 uwg806@yna.co.kr 제보는 카카오톡 okjebo <저작권자(c) 연합뉴스, 무단 전재-재배포 금지> 2022/12/07 18:49 송고",
        "오른 2,418.01로 장을 종료했다. 2022.11.23 pdj6635@yna.co.kr (끝) 〈저작권자(c) 연합뉴스, 무단 전재-재배포 금지〉",
        "양쪽 다 피해가 더 커지기 전에 물밑에서 접점을 찾은 뒤 조만간 마주앉을 것으로 보입니다. 지금까지 경제산업부 박지혜 기자였습니다. 박지혜 기자 sophia@ichannela.com ▶Daum에서 [채널A 뉴스] 구독하기 (모바일) ▶Naver에서 [채널A 뉴스] 구독하기",
        "중앙분리대 <사진=연합뉴스> 이들은 각 경찰서에서 중앙분리대를 설치해달라고 서울시에 신청한 12곳의 도로를 검토해 대상지 4곳을 최종 선정했습니다. 기사제보 및 보도자료 제공 tbs3@naver.com / copyrightⓒ tbs. 무단전재 & 재배포 금지",
        "(뉴욕=연합뉴스) 강건택 특파원 = 사우스다코타주를 비롯한 미국의 여러 주에서 중국의 동영상 공유 플랫폼 틱톡을 부분적으로 규제하는 조치에 나서고 있다. firstcircle@yna.co.kr (끝) <저작권자(c) 연합뉴스, 무단 전재-재배포 금지>",
        '행정안전부는 "지방채무와 관련된 제도들을 종합적으로 정비해 지방자치단체와 지방공공기관에 대한 금융시장의 신뢰를 제고해나가겠다" 고 밝혔습니다. (영상편집: 김종석 / 영상그래픽: 손윤지) KTV 김민아입니다. ( KTV 국민방송 케이블방송, 위성방송 ch164, www.ktv.go.kr ) < ⓒ 한국정책방송원 무단전재 및 재배포 금지 > [출처] 대한민국 정책브리핑(www.korea.kr)',
        '"영국 정부의 지원을 받아 영국령 국가들로 사업 영역을 넓혀 나갈 계획"이라고 말했다. sh@yna.co.kr (끝) <저작권자(c) 연합뉴스, 무단 전재-재배포 금지>',
        "한편 PC 공법은 탈현장 시공(OSC)의 일환으로 건축물의 기둥, 보, 슬래브 등 콘크리트 구조물을 공장에서 제작해 선설 현장으로 옮겨 조립하는 시공 방식이다. 이종무기자 jmlee@ 〈ⓒ 대한경제신문(www.dnews.co.kr), 무단전재 및 수집, 재배포금지〉",
        "백윤희 정책기획과장은 “안전의 중요성을 교직원, 학생, 학부모가 그 어느 때보다도 뼈아프게 느끼고 있는 시기이다”라며, “유관기관과 힘을 모아 우리 아이들의 안전을 지키기 위해 최선을 다하겠다”라고 말했다. <무단전재 및 재배포 금지> 권혁선 기자",
        "새로운 50년에 AI가 핵심 동력이 될 것으로 보고 아낌없는 지원을 해나갈 것”이라고 말했다. 김민상 기자 kim.minsang@joongang.co.kr ⓒ중앙일보(https://www.joongang.co.kr), 무단 전재 및 재배포 금지",
        "30여개 기업이 참여해 국내 기업과 150만달러(약 20억원) 규모의 업무협약(MOU)을 체결할 예정이다. [조세금융신문(tfmedia.co.kr), 무단전재 및 재배포 금지]",
    ]
    labels = [
        "검증의 객관성과 공정성이 훼손될 수 있다는 지적입니다.",
        "여왕이 고소까지 하는 사태까지 왔다는 것은 이러한 정부와 언론의 대결의 심각성을 단적을 표현해주고 있습니다.",
        "동작대교까지 잠실방향으로 구간 정체를 빚고 있습니다.",
        "특정범죄가중처벌법 위반 혐의 등으로 구속영장을 신청했습니다.",
        "그가 남긴 한국 축구의 유산을 이어 받게 될 차기 감독은 누가 될까요.",
        "서울청의 인지도 그만큼 늦어지게 된 것”이라고 설명했습니다. [사진 출처 : 홈페이지 캡처]",
        "‘16강 쾌거’ 벤투호 오늘 금의환향…내일 尹대통령과 만찬",
        "7일 오후 국회에서 열린 성탄트리 점등식에서 참석자들이 점등 버튼을 누르고 있다.",
        "오른 2,418.01로 장을 종료했다.",
        "양쪽 다 피해가 더 커지기 전에 물밑에서 접점을 찾은 뒤 조만간 마주앉을 것으로 보입니다.",
        "중앙분리대 <사진> 이들은 각 경찰서에서 중앙분리대를 설치해달라고 서울시에 신청한 12곳의 도로를 검토해 대상지 4곳을 최종 선정했습니다.",
        "사우스다코타주를 비롯한 미국의 여러 주에서 중국의 동영상 공유 플랫폼 틱톡을 부분적으로 규제하는 조치에 나서고 있다.",
        '행정안전부는 "지방채무와 관련된 제도들을 종합적으로 정비해 지방자치단체와 지방공공기관에 대한 금융시장의 신뢰를 제고해나가겠다" 고 밝혔습니다.',
        '"영국 정부의 지원을 받아 영국령 국가들로 사업 영역을 넓혀 나갈 계획"이라고 말했다.',
        "한편 PC 공법은 탈현장 시공(OSC)의 일환으로 건축물의 기둥, 보, 슬래브 등 콘크리트 구조물을 공장에서 제작해 선설 현장으로 옮겨 조립하는 시공 방식이다. 이종무기자 jmlee@",
        "백윤희 정책기획과장은 “안전의 중요성을 교직원, 학생, 학부모가 그 어느 때보다도 뼈아프게 느끼고 있는 시기이다”라며, “유관기관과 힘을 모아 우리 아이들의 안전을 지키기 위해 최선을 다하겠다”라고 말했다.",
        "새로운 50년에 AI가 핵심 동력이 될 것으로 보고 아낌없는 지원을 해나갈 것”이라고 말했다. 김민상 기자",
        "30여개 기업이 참여해 국내 기업과 150만달러(약 20억원) 규모의 업무협약(MOU)을 체결할 예정이다.",
    ]

    for example, label in zip(examples, labels):
        assert spam_words_filter(example) == label
