import fire

from .jobs.sample_jsonl import sample_jsonl
from .jobs.build_news_paper_data import build_news_paper_data

def run():
    fire.Fire({'sample_jsonl': sample_jsonl,
               'build_news_paper_data': build_news_paper_data})
