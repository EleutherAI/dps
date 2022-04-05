import fire

from .jobs.sample_jsonl import sample_jsonl

def run():
    fire.Fire({'sample_jsonl': sample_jsonl})