import fire

from .jobs.dedup_job import dedup_job
from .jobs.korean_job import korean_job
from .jobs.sample_job import sample_job


def run():
    fire.Fire(
        {
            "sample_job": sample_job,
            "korean_job": korean_job,
            "dedup_job": dedup_job,
        }
    )
