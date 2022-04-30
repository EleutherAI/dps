import fire

from .jobs.sample_jsonl import sample_jsonl
from .jobs.preprocess_jsonl import preprocess_jsonl

def run():
    fire.Fire({
        'sample_jsonl': sample_jsonl,
        'preprocess_jsonl': preprocess_jsonl,
        })