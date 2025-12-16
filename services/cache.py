"""Thread-safe LRU cache with size limits and TTL support."""

import time
import threading
from collections import OrderedDict
from typing import Any

# Maximum number of entries in the cache
MAX_CACHE_SIZE = 1000


class LRUCache:
    """Thread-safe LRU cache with TTL support."""
    
    def __init__(self, max_size: int = MAX_CACHE_SIZE):
        self._cache: OrderedDict[str, dict] = OrderedDict()
        self._lock = threading.RLock()
        self._max_size = max_size
    
    def get(self, key: str, ttl: float) -> Any | None:
        """
        Get a cached value if it exists and hasn't expired.
        
        Args:
            key: Cache key
            ttl: Time-to-live in seconds
            
        Returns:
            Cached value or None if not found/expired
        """
        with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                return None
            if time.time() - entry["ts"] >= ttl:
                # Expired, remove it
                del self._cache[key]
                return None
            # Move to end (most recently used)
            self._cache.move_to_end(key)
            return entry["val"]
    
    def set(self, key: str, val: Any) -> None:
        """
        Set a cache value.
        
        Args:
            key: Cache key
            val: Value to cache
        """
        with self._lock:
            # Remove oldest entries if at capacity
            while len(self._cache) >= self._max_size:
                self._cache.popitem(last=False)
            
            self._cache[key] = {"val": val, "ts": time.time()}
            self._cache.move_to_end(key)
    
    def invalidate(self, key_prefix: str) -> int:
        """
        Remove all entries with keys starting with prefix.
        
        Args:
            key_prefix: Prefix to match
            
        Returns:
            Number of entries removed
        """
        with self._lock:
            keys_to_delete = [k for k in self._cache.keys() if k.startswith(key_prefix)]
            for k in keys_to_delete:
                del self._cache[k]
            return len(keys_to_delete)
    
    def clear(self) -> None:
        """Clear all cached entries."""
        with self._lock:
            self._cache.clear()
    
    def size(self) -> int:
        """Return current cache size."""
        with self._lock:
            return len(self._cache)


# Module-level cache instance
_cache_store = LRUCache()


def get_cached(key: str, ttl: float) -> Any | None:
    """Get cached value if exists and not expired."""
    return _cache_store.get(key, ttl)


def set_cached(key: str, val: Any) -> None:
    """Set cache value."""
    _cache_store.set(key, val)


def invalidate_cache(key_prefix: str) -> int:
    """Remove all cached entries with the given prefix."""
    return _cache_store.invalidate(key_prefix)


def clear_cache() -> None:
    """Clear entire cache."""
    _cache_store.clear()


def cache_size() -> int:
    """Get current cache size."""
    return _cache_store.size()

