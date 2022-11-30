import fire

from .jobs.sample_jsonl import sample_jsonl
from .jobs.build_news_paper_data import build_korean_modu_news_paper_data
from .jobs.massivetext_filter_jsonl import massivetext_filter_jsonl
from .jobs.common_preprocess_jsonl import preprocess
from .jobs.minhash_dedup_data import minhash_deduplication

def run():
    fire.Fire({
        'sample_jsonl': sample_jsonl,
        'common_preprocess_jsonl': preprocess,
        'massivetext_quality_filtering': massivetext_filter_jsonl,
        'build_news_paper_data': build_korean_modu_news_paper_data,
        'minhash_deduplication': minhash_deduplication,
        })
