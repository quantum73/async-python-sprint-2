import json
import typing as tp
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from multiprocessing import Process, Queue

from config import get_console_logger
from task_examples import TARGET_FUNCS
from utils import JobStatus

job_logger = get_console_logger(name=__name__)

JobType = tp.TypeVar("JobType", bound="Job")


@dataclass
class Job:
    idx: uuid.UUID
    target_func: tp.Callable
    args: tp.Sequence = field(default_factory=tuple)
    kwargs: tp.MutableMapping = field(default_factory=dict)
    start_at: datetime = field(default_factory=datetime.now)
    max_working_time: int | None = None
    tries: int = 0
    dependencies: tp.Sequence["Job"] = field(default_factory=list)

    result: tp.Any = field(init=False, repr=False, default=None)
    is_paused: bool = field(init=False, repr=False, default=False)
    is_stopped: bool = field(init=False, repr=False, default=False)
    status: JobStatus = field(init=False, repr=False, default=JobStatus.WAITING)
    target_func_name: str = field(init=False, repr=False)

    def __post_init__(self):
        self.start_at = self.validate_start_at(value=self.start_at)
        self.max_working_time = self.validate_max_working_time(value=self.max_working_time)
        self.tries = self.validate_tries(value=self.tries)
        self._target_func_name = self.target_func.__name__

    def __str__(self):
        return f"Job[{self.idx}](target_func={self.target_func.__name__})"

    @staticmethod
    def validate_start_at(value: tp.Any) -> datetime:
        if not isinstance(value, datetime):
            raise ValueError(f"\"start_at\" field must be datetime.datetime. You passed - {type(value)}.")
        if value < datetime.now():
            raise ValueError("\"start_at\" field must have the value of the current or future date.")
        return value

    @staticmethod
    def validate_max_working_time(value: tp.Any) -> int | None:
        if value is None:
            return value
        if not isinstance(value, int):
            raise ValueError(f"\"max_working_time\" field must be int. You passed - {type(value)}.")
        if value <= 0:
            raise ValueError(f"\"max_working_time\" field must be greater than 0.")
        return value

    @staticmethod
    def validate_tries(value: tp.Any) -> int:
        if not isinstance(value, int):
            raise ValueError(f"\"tries\" field must be int. You passed - {type(value)}")
        if value < 0:
            raise ValueError(f"\"tries\" field must be greater or equal than 0")
        return value

    @property
    def is_start_later(self) -> bool:
        return bool(self.start_at > datetime.now())

    @staticmethod
    def wrap_func_for_process(*, q: Queue, func: tp.Callable, args: tp.Sequence, kwargs: tp.MutableMapping) -> tp.Any:
        result = func(*args, **kwargs)
        q.put(result)

    def run_func_by_max_working_time(
            self,
            func: tp.Callable,
            timeout: int = None,
            args: tp.Sequence = None,
            kwargs: tp.MutableMapping = None,
    ):
        args = args or ()
        kwargs = kwargs or {}
        result_queue = Queue()
        process_kwargs = {
            "q": result_queue,
            "func": func,
            "args": args,
            "kwargs": kwargs,
        }
        p = Process(target=self.wrap_func_for_process, kwargs=process_kwargs)
        p.start()
        p.join(timeout)
        if p.is_alive():
            p.terminate()

        return result_queue.get(block=False)

    def run(self) -> tp.Any:
        if self.max_working_time:
            return self.run_func_by_max_working_time(
                func=self.target_func,
                timeout=self.max_working_time,
                args=self.args,
                kwargs=self.kwargs,
            )

        return self.target_func(*self.args, **self.kwargs)

    def pause(self):
        self.is_paused = not self.is_paused

    def stop(self):
        self.is_stopped = True

    @staticmethod
    def _job_to_dict(job: JobType) -> dict:
        return {
            "idx": str(job.idx),
            "target_func": job._target_func_name,
            "args": tuple(job.args),
            "kwargs": job.kwargs,
            "start_at": job.start_at.isoformat(),
            "max_working_time": job.max_working_time,
            "tries": job.tries,
        }

    def to_dict(self) -> dict:
        dependencies = [self._job_to_dict(job) for job in self.dependencies]
        job_data = self._job_to_dict(job=self)
        job_data["dependencies"] = dependencies
        return job_data

    @staticmethod
    def _prepare_job_data(target_job_data: dict) -> dict | None:
        try:
            target_func_name = target_job_data["target_func"]
            target_func = TARGET_FUNCS.get(target_func_name)
            if target_func is None:
                job_logger.warning(f"Not found \"{target_func_name}\" function.")
                return None

            target_job_data["target_func"] = target_func
            target_job_data["start_at"] = max(datetime.now(), datetime.fromisoformat(target_job_data["start_at"]))
            return target_job_data
        except Exception as err:
            job_logger.error(f"Occurred error during preparing job data: {err}")
            return None

    @classmethod
    def from_dict(cls, data: dict) -> JobType | None:
        try:
            prepared_job_base_data = cls._prepare_job_data(data)
            if prepared_job_base_data is None:
                return None

            dependencies = [cls._prepare_job_data(d) for d in prepared_job_base_data["dependencies"]]
            dependencies = list(filter(lambda x: x is not None, dependencies))
            dependencies = [cls(**d) for d in dependencies]
            prepared_job_base_data["dependencies"] = dependencies
            return cls(**data)
        except Exception as err:
            job_logger.error(f"Occurred error in \"from_dict\" function: {err}")
            return None


if __name__ == '__main__':
    # first_job = Job(
    #     idx=uuid.uuid4(),
    #     target_func=TARGET_FUNCS["calculate_func"],
    #     start_at=datetime.now() + timedelta(seconds=2),
    #     args=(1, 10),
    #     dependencies=[
    #         Job(idx=uuid.uuid4(), target_func=TARGET_FUNCS["error_func"]),
    #         Job(idx=uuid.uuid4(), target_func=TARGET_FUNCS["random_error_func"], tries=3),
    #         Job(idx=uuid.uuid4(), target_func=TARGET_FUNCS["web_func"]),
    #     ],
    # )
    # with open("backup.json", "w", encoding="utf8") as f:
    #     json.dump(first_job.to_dict(), fp=f, ensure_ascii=False, indent=4)

    with open("backup.json", "r", encoding="utf8") as f:
        backup_data = json.load(fp=f)

    first_job = Job.from_dict(data=backup_data)
    print(len(first_job.dependencies))
    print(first_job.dependencies)
