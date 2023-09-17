import random
import time
import typing as tp

import requests


def calculate_func(start: int = 0, end: int = 10):
    summ = 0
    for i in range(start, end + 1):
        summ += i
        time.sleep(0.1)
    return summ


def web_func(url: str) -> int:
    time.sleep(1)
    r = requests.get(url, timeout=1)
    return r.status_code


def error_func():
    time.sleep(1)
    raise ValueError("err")


def random_error_func():
    time.sleep(1)
    if random.randint(1, 2) == 1:
        raise ValueError("err")
    return "GOOD"


def run_delay(delay: int):
    print(f"Sleep on {delay} seconds")
    time.sleep(delay)
    return delay


def long_func():
    for i in range(1, 6):
        print(f"[long_func] step №{i}")
        time.sleep(1)


TARGET_FUNCS: tp.Mapping[str, tp.Callable] = {
    "calculate_func": calculate_func,
    "web_func": web_func,
    "error_func": error_func,
    "random_error_func": random_error_func,
    "run_delay": run_delay,
    "long_func": long_func,
}
