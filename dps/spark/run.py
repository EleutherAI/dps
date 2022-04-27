import fire

from .jobs.sample_jsonl import sample_jsonl
from .jobs.build_news_paper_data import build_news_paper_data
from .jobs.massivetext_filter_jsonl import massivetext_filter_jsonl
from .jobs.common_preprocess_jsonl import preprocess

def run():
    fire.Fire({
        'sample_jsonl': sample_jsonl,
        'common_preprocess_jsonl': preprocess,
        'massivetext_quality_filtering': massivetext_filter_jsonl,
        'build_news_paper_data': build_news_paper_data,
        })
