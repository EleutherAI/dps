import json

def read_line(text):
    yield json.loads(text)


def to_json(obj):
    yield json.dumps(obj)


def write_jsonl(obj):
    yield json.dumps(obj, ensure_ascii=False)
