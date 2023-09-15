import typing as tp


class Job:
    def __init__(
            self,
            start_at: str = None,
            max_working_time: int = -1,
            tries: int = 0,
            dependencies: tp.Sequence["Job"] = None,
    ):
        self._start_at = start_at
        self._max_working_time = max_working_time
        self._tries = tries
        self._dependencies = dependencies or []

    def run(self):
        pass

    def pause(self):
        pass

    def stop(self):
        pass
