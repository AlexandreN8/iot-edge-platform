import hashlib

seen_hashes = set()

def is_duplicate(payload_bytes):
    h = hashlib.sha256(payload_bytes).hexdigest()
    if h in seen_hashes:
        return True
    seen_hashes.add(h)
    return False

def is_valid_json(payload_bytes):
    import json
    try:
        json.loads(payload_bytes)
        return True
    except json.JSONDecodeError:
        return False