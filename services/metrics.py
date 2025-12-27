"""Simple in-memory metrics for latency and error tracking."""

import threading
import time
from typing import Dict, Any

_lock = threading.RLock()
_metrics: Dict[str, dict] = {}


def _ensure(name: str) -> dict:
    with _lock:
        if name not in _metrics:
            _metrics[name] = {
                "calls": 0,
                "errors": 0,
                "total_ms": 0.0,
                "last_ms": None,
                "last_error": None,
                "last_error_ts": None,
            }
        return _metrics[name]


def record_timing(name: str, ms: float) -> None:
    """Record a timing datapoint in milliseconds."""
    with _lock:
        m = _ensure(name)
        m["calls"] += 1
        m["total_ms"] += ms
        m["last_ms"] = ms


def record_error(name: str, detail: str) -> None:
    """Record an error occurrence with a brief detail string."""
    with _lock:
        m = _ensure(name)
        m["errors"] += 1
        m["last_error"] = detail
        m["last_error_ts"] = time.time()


def _summarize(m: dict) -> dict:
    calls = m.get("calls") or 0
    total_ms = m.get("total_ms") or 0.0
    avg_ms = total_ms / calls if calls else None
    return {
        "calls": calls,
        "errors": m.get("errors") or 0,
        "avg_ms": round(avg_ms, 2) if avg_ms is not None else None,
        "last_ms": m.get("last_ms"),
        "last_error": m.get("last_error"),
        "last_error_ts": m.get("last_error_ts"),
    }


def get_stats(name: str | None = None) -> Dict[str, Any]:
    """Return a copy of metrics for one key or all keys."""
    with _lock:
        if name:
            if name not in _metrics:
                return {}
            return {name: _summarize(_metrics[name])}
        return {k: _summarize(v) for k, v in _metrics.items()}



