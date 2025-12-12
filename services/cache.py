import time

_cache_store = {}

def get_cached(key, ttl):
    entry = _cache_store.get(key)
    if entry and time.time() - entry["ts"] < ttl:
        return entry["val"]
    return None

def set_cached(key, val):
    _cache_store[key] = {"val": val, "ts": time.time()}

def invalidate_cache(key_prefix):
    for k in list(_cache_store.keys()):
        if k.startswith(key_prefix):
            del _cache_store[k]
