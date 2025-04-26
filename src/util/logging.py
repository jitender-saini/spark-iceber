import functools
import logging
from collections.abc import Callable
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo


def configure_logging():
    logging.basicConfig(level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S.%f'[:-3], format='%(asctime)s %(levelname)+5s %(name)s: %(message)s')


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


def log_execution_time(logger: logging.Logger) -> Callable:
    def decorator(func: Callable) -> Callable:
        tz = ZoneInfo('Europe/Paris')

        @functools.wraps(func)  # Preserves the original function's metadata
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            process_start = datetime.now(tz=tz)
            logger.info(f'Starting process at {process_start}')
            result = func(*args, **kwargs)
            logger.info(f'Process finished in {(datetime.now(tz=tz) - process_start).total_seconds()}s')
            return result

        return wrapper

    return decorator
