import fire

from .jobs.sample_jsonl import sample_jsonl
from .jobs.massivetext_quality_filtering import massivetext_quality_filtering


def run():
    fire.Fire({'sample_jsonl': sample_jsonl,
               "massivetext_quality_filtering": massivetext_quality_filtering
               })
