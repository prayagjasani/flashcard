"""Shared ThreadPoolExecutor to avoid per-request overhead."""

from concurrent.futures import ThreadPoolExecutor

# Shared executor for CPU-bound tasks like audio generation
# Using a module-level singleton prevents creating new executors per request
_shared_executor: ThreadPoolExecutor | None = None

# Default max workers - matches typical I/O bound workload
DEFAULT_MAX_WORKERS = 10


def get_executor(max_workers: int = DEFAULT_MAX_WORKERS) -> ThreadPoolExecutor:
    """
    Get or create the shared ThreadPoolExecutor.
    
    Args:
        max_workers: Maximum number of worker threads (only used on first call)
        
    Returns:
        The shared ThreadPoolExecutor instance
    """
    global _shared_executor
    if _shared_executor is None:
        _shared_executor = ThreadPoolExecutor(max_workers=max_workers)
    return _shared_executor


def shutdown_executor(wait: bool = True) -> None:
    """
    Shutdown the shared executor. Call during app shutdown.
    
    Args:
        wait: If True, wait for pending tasks to complete
    """
    global _shared_executor
    if _shared_executor is not None:
        _shared_executor.shutdown(wait=wait)
        _shared_executor = None
