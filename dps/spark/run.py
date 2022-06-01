import fire

from .jobs.sample_jsonl import sample_jsonl
from .jobs.massivetext_quality_filtering import massivetext_quality_filtering
from .jobs.common_preprocess_jsonl import preprocess

def run():
    fire.Fire({
        'sample_jsonl': sample_jsonl,
        'common_preprocess_jsonl': preprocess,
        'massivetext_quality_filtering': massivetext_quality_filtering
        })
