import fire

from .jobs.dedup_job import dedup_job
from .jobs.korean_job import korean_job
from .jobs.japanese_job import japanese_job
from .jobs.chinese_job import chinese_job
from .jobs.sample_job import sample_job


def run():
    fire.Fire(
        {
            "sample_job": sample_job,
            "korean_job": korean_job,
            "japanese_job": japanese_job,
            "chinese_job" : chinese_job,
            "dedup_job": dedup_job,
        }
    )
