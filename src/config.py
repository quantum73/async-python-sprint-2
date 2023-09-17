import logging
import sys


def get_console_logger(*, name: str, level: str = logging.INFO) -> logging.Logger:
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
