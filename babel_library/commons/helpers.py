import json
import uuid

def intTryParse(value):
    try:
        return int(value)
    except Exception:
        return False

def tryParse(value):
    try:
        return json.loads(value)
    except Exception:
        return []

def get_correlation_id():
    return str(uuid.uuid4())