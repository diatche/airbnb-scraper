import json

def hash_str(x):
    return abs(hash(json.dumps(x, sort_keys=True))).to_bytes(8, "big").hex()
