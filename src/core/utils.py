import logging
import sys
import typing as tp
from enum import Enum
from functools import wraps


def get_console_logger(*, name: str, level: int = logging.INFO) -> logging.Logger:
    fmt = '%(asctime)s | [%(levelname)s] | %(message)s'
    date_fmt = "%H:%M:%S %d-%m-%Y"
    logger = logging.getLogger(name)
    logger.setLevel(level)
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setLevel(level)
    formatter = logging.Formatter(fmt=fmt, datefmt=date_fmt)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


class JobStatus(str, Enum):
    WAITING = "WAITING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    ERROR = "ERROR"
    STOPPED = "STOPPED"


def coroutine(func: tp.Callable) -> tp.Callable:
    @wraps(func)
    def wrap(*args, **kwargs) -> tp.Generator:
        gen = func(*args, **kwargs)
        gen.send(None)
        return gen

    return wrap
