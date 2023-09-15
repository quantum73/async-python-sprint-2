import typing as tp
from multiprocessing import Queue


class Scheduler:
    def __init__(self, pool_size: int = 10):
        self._pool_size = pool_size
        self._tasks_queue = Queue(maxsize=self._pool_size)
        self._results_queue = Queue()

    def schedule(self, task):
        pass

    def run(self):
        pass

    def restart(self):
        pass

    def stop(self):
        pass