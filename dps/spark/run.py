import fire

from .jobs.sample_jsonl import sample_jsonl
from .jobs.common_preprocess_jsonl import preprocess
from .jobs.split_train_and_test_jsonl import split_jsonl

def run():
    fire.Fire({
        'sample_jsonl': sample_jsonl,
        'common_preprocess_jsonl': preprocess,
        'split_train_and_test_jsonl': split_jsonl
        })