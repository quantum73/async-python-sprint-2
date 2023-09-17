import typing as tp
from enum import Enum
from functools import wraps


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
