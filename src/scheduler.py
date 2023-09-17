import json
import queue
import time
import typing as tp
import uuid
from datetime import datetime, timedelta
from pathlib import Path

from core.exceptions import MaxSchedulerSize
from core.task_examples import TARGET_FUNCS
from core.utils import coroutine, JobStatus, get_console_logger
from job import Job

scheduler_logger = get_console_logger(name=__name__)


class Scheduler:
    BACKUP_PATH = Path("backup.json")

    def __init__(self, pool_size: int = 10):
        self._pool_size = pool_size
        self._tasks: dict[uuid.UUID, Job] = {}
        self._completed_tasks: dict[uuid.UUID, Job] = {}
        self._is_stop = False
        self._scheduler_is_run = False

    @property
    def tasks(self):
        return self._tasks

    @property
    def completed_tasks(self):
        return self._completed_tasks

    @property
    def scheduler_is_full(self) -> bool:
        """Проверка на то полон ли планировщик"""
        return len(self._tasks) >= self._pool_size

    def _add_job(self, job: Job):
        """
        Метод добавления задачи в хранилище планировщика.
        Если планировщик полон - генерируем исключение, иначе - добавляем задачу.
        """
        if self.scheduler_is_full:
            raise MaxSchedulerSize("Scheduler is full!")
        self._tasks[job.idx] = job

    def schedule(self, job: Job):
        """Функция добавления задачи и её зависимостей"""
        self._add_job(job=job)
        for depend_job in job.dependencies:
            self._add_job(job=depend_job)

    def get_waiting_jobs(self) -> list:
        """Получаем задачи, которые ожидают своего выполнения"""
        return list(filter(lambda x: x[1].status == JobStatus.WAITING, self._tasks.items()))

    def complete_job(self, job: Job) -> None:
        """Удаляем завершенную задачу из основного хранилища и добавляем в хранилище завершенных задач"""
        self._tasks.pop(job.idx, None)
        self._completed_tasks[job.idx] = job

    def stop_job(self, job: Job) -> None:
        """Функция остановки задачи, когда её останавливают методом stop или по истечению max_working_time"""
        job.status = JobStatus.STOPPED
        self.complete_job(job=job)

    def retry_job(self, job: Job) -> None:
        """
        Функция перезапуска задачи, если она завершилась ошибкой.
        Если количество попыток больше 0, то делаем задача ожидающий статус (WAITING),
        чтобы она затем запустилась снова. Иначе - завершаем задачу со статусом ошибки (ERROR).
        """
        if job.tries > 0:
            job.tries -= 1
            job.status = JobStatus.WAITING
            scheduler_logger.info(f'{job} left tries = {job.tries}')
        else:
            job.status = JobStatus.ERROR
            self.complete_job(job=job)

    def run_job(self, job: Job) -> None:
        """Функция для запуска задачи и её завершение при корректном выполнении"""
        job.result = job.run()
        job.status = JobStatus.COMPLETED
        self.complete_job(job=job)

    def all_job_dependencies_finished(self, job: Job) -> bool:
        """Функция для проверки того что все задачи-зависимости завершены"""
        return bool(all(dependency_job.idx not in self._tasks for dependency_job in job.dependencies))

    def get_job(self, target_job) -> None:
        while not self._is_stop:
            waiting_jobs = self.get_waiting_jobs()
            if len(waiting_jobs) == 0:
                scheduler_logger.info("All jobs completed!")
                self._is_stop = True
                break

            for idx, job in waiting_jobs:
                if not self.all_job_dependencies_finished(job=job):
                    scheduler_logger.info(f'Not all {job} dependencies finished')
                    time.sleep(0.5)
                    continue

                if job.is_stopped:
                    scheduler_logger.info(f'Stopping {job} by stop command...')
                    self.stop_job(job=job)
                    scheduler_logger.info(f'{job} stopped!')
                    continue

                if job.is_paused:
                    scheduler_logger.info(f'{job} on pause.')
                    time.sleep(0.5)
                    continue

                if job.is_start_later:
                    scheduler_logger.info(f'{job} start later. At {job.start_at}')
                    time.sleep(0.5)
                    continue

                scheduler_logger.info(f'Processing {job}')
                job.status = JobStatus.PROCESSING
                target_job.send(job)

    @coroutine
    def execute_job(self):
        while job := (yield):
            try:
                scheduler_logger.info(f"Try to execute {job}...")
                self.run_job(job=job)
                scheduler_logger.info(f'{job} done!')
            except queue.Empty:
                scheduler_logger.info(f'Stopping {job} by max_working_time...')
                self.stop_job(job=job)
                scheduler_logger.info(f'{job} stopped!')
            except Exception as err:
                scheduler_logger.error(f'{job} has error: {err}')
                scheduler_logger.info(f'Try to retry {job}...')
                self.retry_job(job=job)

    def run(self):
        self._scheduler_is_run = True
        try:
            executed_job = self.execute_job()
            self.get_job(executed_job)
        except KeyboardInterrupt:
            scheduler_logger.info("You press Ctrl+C.")
            self._scheduler_is_run = False
            self.stop()

    def restart(self):
        """Рестарт планировщика."""
        scheduler_logger.info("Restarting scheduler...")
        if self._scheduler_is_run:
            self.stop()

        scheduler_logger.info("Restore data from backup...")
        with self.BACKUP_PATH.open("r", encoding="utf8") as f:
            backup_data = json.load(fp=f)

        for job_data in backup_data:
            job = Job.from_dict(data=job_data)
            if job is None:
                continue
            self.schedule(job=job)

        scheduler_logger.info("Backup data has been restored!")

        self.run()

    def _create_backup_data(self) -> tp.Sequence[tp.MutableMapping]:
        data_to_save = []
        added_ids = set()
        for idx, job in self._tasks.items():
            if idx in added_ids:
                continue

            new_dependencies = []
            for d in job.dependencies:
                if d.idx not in self._completed_tasks:
                    added_ids.add(d.idx)
                    new_dependencies.append(d)

            job.dependencies = new_dependencies
            data_to_save.append(job.to_dict())
            added_ids.add(idx)

        return data_to_save

    def stop(self):
        """Остановка и бэкап планировщика."""
        scheduler_logger.info("Stopping scheduler...")
        scheduler_logger.info("Creating backup file...")
        data_to_save = self._create_backup_data()
        with self.BACKUP_PATH.open("w", encoding="utf8") as f:
            json.dump(data_to_save, fp=f, ensure_ascii=False, indent=4)

    def summary(self) -> None:
        """Вывод итоговой информации после остановки планировщика"""
        print("Scheduler summary:")
        print("\tNot finished jobs:")
        for _, not_finished_job in self._tasks.items():
            print(f"\t\t- {not_finished_job}. Status: {not_finished_job.status}")

        print("\tFinished jobs:")
        for _, finished_job in self._completed_tasks.items():
            print(f"\t\t- {finished_job}. Status: {finished_job.status}. Result: {finished_job.result}")


def cold_restart_example() -> None:
    scheduler = Scheduler(pool_size=5)
    scheduler_logger.info('Restart scheduler...')
    scheduler.restart()
    scheduler_logger.info('Scheduler done!')
    scheduler.summary()


def restart_example() -> None:
    now = datetime.now()
    scheduler = Scheduler(pool_size=5)
    scheduler.schedule(
        job=Job(
            idx=uuid.uuid4(),
            target_func=TARGET_FUNCS["calculate_func"],
            start_at=now + timedelta(seconds=10),
            args=(1, 10),
            dependencies=[
                Job(idx=uuid.uuid4(), target_func=TARGET_FUNCS["error_func"]),
                Job(idx=uuid.uuid4(), target_func=TARGET_FUNCS["web_func"], kwargs={"url": "https://google.com"}),
            ],
        )
    )
    scheduler.schedule(
        job=Job(
            idx=uuid.uuid4(),
            target_func=scheduler.restart,
            start_at=now + timedelta(seconds=5),
        )
    )

    scheduler_logger.info('Running scheduler...')
    scheduler.run()
    scheduler_logger.info('Scheduler done!')

    scheduler.summary()


def run_example() -> None:
    scheduler = Scheduler(pool_size=5)
    scheduler.schedule(
        job=Job(
            idx=uuid.uuid4(),
            target_func=TARGET_FUNCS["calculate_func"],
            start_at=datetime.now() + timedelta(seconds=5),
            args=(1, 10),
            dependencies=[
                Job(idx=uuid.uuid4(), target_func=TARGET_FUNCS["error_func"]),
                Job(idx=uuid.uuid4(), target_func=TARGET_FUNCS["web_func"], kwargs={"url": "https://google.com"}),
            ],
        )
    )
    scheduler.schedule(job=Job(idx=uuid.uuid4(), target_func=TARGET_FUNCS["random_error_func"], tries=3))

    scheduler_logger.info('Running scheduler...')
    scheduler.run()
    scheduler_logger.info('Scheduler done!')

    scheduler.summary()


def run_example_with_max_size_error() -> None:
    scheduler = Scheduler(pool_size=2)
    scheduler.schedule(
        job=Job(
            idx=uuid.uuid4(),
            target_func=TARGET_FUNCS["calculate_func"],
            start_at=datetime.now() + timedelta(seconds=5),
            args=(1, 10),
            dependencies=[
                Job(idx=uuid.uuid4(), target_func=TARGET_FUNCS["error_func"]),
                Job(idx=uuid.uuid4(), target_func=TARGET_FUNCS["web_func"], kwargs={"url": "https://google.com"}),
            ],
        )
    )

    scheduler_logger.info('Running scheduler...')
    scheduler.run()
    scheduler_logger.info('Scheduler done!')

    scheduler.summary()


if __name__ == '__main__':
    # run_example()
    # run_example_with_max_size_error()
    # cold_restart_example()
    restart_example()
