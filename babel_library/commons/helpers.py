import json

def intTryParse(value):
    try:
        return int(value)
    except Exception:
        return False

def tryParse(value):
    try:
        return json.load(value)
    except:
        return ''