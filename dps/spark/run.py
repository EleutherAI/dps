import fire

from .jobs.sample_jsonl import sample_jsonl
from .jobs.massivetext_filter_jsonl import massivetext_filter_jsonl
from .jobs.common_preprocess_jsonl import preprocess
from .jobs.japanese_dedup import japanese_exact_dedup

def run():
    fire.Fire({
        'sample_jsonl': sample_jsonl,
        'common_preprocess_jsonl': preprocess,
        'massivetext_quality_filtering': massivetext_filter_jsonl,
        'japanese_exact_dedup': japanese_exact_dedup,
    })
