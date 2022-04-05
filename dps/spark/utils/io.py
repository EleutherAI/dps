import json

def read_line(text):
    yield json.loads(text)


def to_json(obj):
    yield json.dumps(obj)
