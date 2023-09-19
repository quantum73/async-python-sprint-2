import typing as tp
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from multiprocessing import Process, Queue

from core.utils import JobStatus, get_console_logger
from examples.tasks import TARGET_FUNCS

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
    is_stopped: bool = field(init=False, repr=False, default=False)
    status: JobStatus = field(init=False, repr=False, default=JobStatus.WAITING)
    target_func_name: str = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self.start_at = self.validate_start_at(value=self.start_at)
        self.max_working_time = self.validate_max_working_time(value=self.max_working_time)
        self.tries = self.validate_tries(value=self.tries)
        self._target_func_name = self.target_func.__name__

    def __str__(self) -> str:
        return f"Job[{self.idx}](target_func={self.target_func.__name__})"

    def __repr__(self) -> str:
        return f"Job[{self.idx}](target_func={self.target_func.__name__})"

    @staticmethod
    def validate_start_at(value: tp.Any) -> datetime:
        if not isinstance(value, datetime):
            raise ValueError(f"\"start_at\" field must be datetime.datetime. You passed - {type(value)}.")

        now = datetime.now().replace(microsecond=0)
        if value.replace(microsecond=0) < now:
            raise ValueError("\"start_at\" field must have the value of the current or future date.")
        return value

    @staticmethod
    def validate_max_working_time(value: tp.Any) -> int | None:
        if value is None:
            return value
        if not isinstance(value, int):
            raise ValueError(f"\"max_working_time\" field must be int. You passed - {type(value)}.")
        if value <= 0:
            raise ValueError("\"max_working_time\" field must be greater than 0.")
        return value

    @staticmethod
    def validate_tries(value: tp.Any) -> int:
        if not isinstance(value, int):
            raise ValueError(f"\"tries\" field must be int. You passed - {type(value)}")
        if value < 0:
            raise ValueError("\"tries\" field must be greater or equal than 0")
        return value

    @property
    def is_start_later(self) -> bool:
        return bool(self.start_at > datetime.now())

    @staticmethod
    def wrap_func_for_process(*, q: Queue, func: tp.Callable, args: tp.Sequence, kwargs: tp.MutableMapping) -> None:
        result = func(*args, **kwargs)
        q.put(result)

    def run_func_by_max_working_time(
            self,
            func: tp.Callable,
            timeout: int | None = None,
            args: tp.Sequence | None = None,
            kwargs: tp.MutableMapping | None = None,
    ) -> tp.Any:
        args = args or ()
        kwargs = kwargs or {}
        result_queue: Queue = Queue()
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

    def processing(self) -> None:
        """Функция, процессинга задачи перед её выполнением."""
        self.status = JobStatus.PROCESSING

    def run(self) -> None:
        """Функция, запускающая выполнение задачи."""
        if self.max_working_time:
            result = self.run_func_by_max_working_time(
                func=self.target_func,
                timeout=self.max_working_time,
                args=self.args,
                kwargs=self.kwargs,
            )
        else:
            result = self.target_func(*self.args, **self.kwargs)

        self.result = result
        self.status = JobStatus.COMPLETED

    def retry(self) -> None:
        """
        Функция перезапуска задачи, если она завершилась ошибкой.
        Если количество попыток больше 0, то делаем задаче ожидающий статус (WAITING),
        чтобы она затем запустилась снова. Иначе - ставим статус завершения с ошибкой (ERROR).
        """
        if self.tries > 0:
            self.tries -= 1
            self.status = JobStatus.WAITING
            job_logger.info(f'{self} left tries = {self.tries}')
        else:
            job_logger.info(f'{self} has no tries anymore. Stopping with error status.')
            self.stop(status=JobStatus.ERROR)

    def stop(self, status: JobStatus = JobStatus.STOPPED) -> None:
        """Функция остановки задачи, когда её останавливают методом stop или по истечению max_working_time."""
        self.is_stopped = True
        self.status = status

    @staticmethod
    def _job_to_dict(job: "Job") -> dict:
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
    def _prepare_job_data(target_job_data: tp.MutableMapping) -> tp.MutableMapping | None:
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

    @staticmethod
    def _prepare_dependencies(dependencies: tp.Sequence[tp.MutableMapping]) -> tp.Sequence[tp.MutableMapping]:
        prepared_dependencies = []
        for depend_job in dependencies:
            data = Job._prepare_job_data(depend_job)
            if data is None:
                continue
            prepared_dependencies.append(data)
        return prepared_dependencies

    @classmethod
    def from_dict(cls, data: dict) -> tp.Any:
        try:
            prepared_job_base_data = cls._prepare_job_data(data)
            if prepared_job_base_data is None:
                return None

            prepared_dependencies = cls._prepare_dependencies(prepared_job_base_data["dependencies"])
            new_dependencies = [cls(**d) for d in prepared_dependencies]
            prepared_job_base_data["dependencies"] = new_dependencies
            return cls(**data)
        except Exception as err:
            job_logger.error(f"Occurred error in \"from_dict\" function: {err}")
            return None
