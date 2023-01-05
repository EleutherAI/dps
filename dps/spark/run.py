import fire

from .jobs import dedup_job
from .jobs.korean_job import korean_job
from .jobs.sample_jsonl import sample_jsonl


def run():
    fire.Fire(
        {
            "sample_jsonl": sample_jsonl,
            "korean_job": korean_job,
            "dedup_job": dedup_job,
        }
    )
