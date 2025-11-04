# aep_etl/utils.py
"""
Utility helpers: retries, sleep, slugify, etc.
"""
import time, random, re
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

class Transient(Exception):
    """Raised for retryable transient failures (e.g., no data captured)."""

def polite_sleep(ms: int):
    """Sleep a random small offset around the throttle ms."""
    time.sleep(ms/1000 + random.uniform(0, 0.3))

def slugify(s: str) -> str:
    """Convert a string into a URL-safe slug (used for country URLs)."""
    s = s.replace("’","").replace(",","")
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")

def retryable(fn=None, attempts: int = 3, wait_s: float = 1.0):
    """
    Decorator to make a function retryable with Tenacity.
    Usage:
        @retryable
        def my_func(...):
            ...
    """
    def deco(f):
        return retry(
            stop=stop_after_attempt(attempts),
            wait=wait_fixed(wait_s),
            retry=retry_if_exception_type(Transient),
            reraise=True,
        )(f)
    return deco if fn is None else deco(fn)
