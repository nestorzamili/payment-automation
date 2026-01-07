import time
import random
from functools import wraps
from typing import Type, Tuple

from gspread.exceptions import APIError

from src.core.logger import get_logger

logger = get_logger(__name__)

DEFAULT_MAX_RETRIES = 5
DEFAULT_MAX_BACKOFF = 64


def exponential_backoff(
    max_retries: int = DEFAULT_MAX_RETRIES,
    max_backoff: int = DEFAULT_MAX_BACKOFF,
    exceptions: Tuple[Type[Exception], ...] = (APIError,)
):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    retries += 1
                    if retries > max_retries:
                        logger.error(f"Max retries ({max_retries}) exceeded for {func.__name__}")
                        raise
                    
                    wait_time = min((2 ** retries) + random.uniform(0, 1), max_backoff)
                    logger.warning(f"API error in {func.__name__}, retrying in {wait_time:.1f}s (attempt {retries}/{max_retries})")
                    time.sleep(wait_time)
        return wrapper
    return decorator
