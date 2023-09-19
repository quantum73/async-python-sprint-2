import json
import queue
import time
import typing as tp
import uuid

from config import BACKUP_PATH, SCHEDULER_SIZE
from core.exceptions import MaxSchedulerSize
from core.utils import coroutine, JobStatus, get_console_logger
from job import Job

scheduler_logger = get_console_logger(name=__name__)


class Scheduler:
    NOT_FINISHED_STATUSES = [JobStatus.WAITING, JobStatus.PROCESSING]

    def __init__(self, scheduler_size: int = SCHEDULER_SIZE) -> None:
        self._scheduler_size = scheduler_size
        self._tasks: dict[uuid.UUID, Job] = {}
        self._completed_tasks: dict[uuid.UUID, Job] = {}
        self._scheduler_is_run = False

    @property
    def tasks(self) -> dict[uuid.UUID, Job]:
        return self._tasks

    @property
    def completed_tasks(self) -> dict[uuid.UUID, Job]:
        return self._completed_tasks

    @property
    def scheduler_is_full(self) -> bool:
        """Проверка на то полон ли планировщик."""
        return len(self._tasks) >= self._scheduler_size

    def _add_job(self, job: Job) -> None:
        """
        Метод добавления задачи в хранилище планировщика.
        Если планировщик полон - генерируем исключение, иначе - добавляем задачу.
        """
        if self.scheduler_is_full:
            raise MaxSchedulerSize("Scheduler is full!")
        self._tasks[job.idx] = job

    def schedule(self, job: Job) -> None:
        """Функция добавления задачи и её зависимостей."""
        self._add_job(job=job)
        for depend_job in job.dependencies:
            self._add_job(job=depend_job)

    def _get_waiting_jobs(self) -> tp.Sequence[Job]:
        """Получаем задачи, которые ожидают своего выполнения."""
        return list(filter(lambda job: job.status == JobStatus.WAITING, self._tasks.values()))

    def all_job_dependencies_finished(self, job: Job) -> bool:
        """Функция для проверки того что все задачи-зависимости завершены."""
        return bool(all(item.status not in self.NOT_FINISHED_STATUSES for item in job.dependencies))

    def _managing_job(self, target_job: tp.Generator) -> None:
        """Функция-менеджер ожидающих задач."""
        while self._scheduler_is_run:
            waiting_jobs = self._get_waiting_jobs()
            if len(waiting_jobs) == 0:
                scheduler_logger.info("All jobs completed!")
                self._scheduler_is_run = False
                break

            for job in waiting_jobs:
                if not self.all_job_dependencies_finished(job=job):
                    scheduler_logger.info(f'Not all {job} dependencies finished')
                    time.sleep(0.5)
                    continue
                if job.is_start_later:
                    scheduler_logger.info(f'{job} start later. At {job.start_at}')
                    time.sleep(0.5)
                    continue

                target_job.send(job)

    @coroutine
    def execute_job(self) -> tp.Generator:
        """Корутина, выполняющая задачу."""
        while job := (yield):
            scheduler_logger.info(f'Processing {job}...')
            job.processing()
            try:
                scheduler_logger.info(f"Try to execute {job}...")
                job.run()
                scheduler_logger.info(f'{job} done!')
            except queue.Empty:
                scheduler_logger.info(f'Stopping {job} by max_working_time...')
                job.stop()
                scheduler_logger.info(f'{job} stopped!')
            except Exception as err:
                scheduler_logger.error(f'{job} has error: {err}')
                scheduler_logger.info(f'Try to retry {job}...')
                job.retry()

    def run(self) -> None:
        """Запуск планировщика."""
        self._scheduler_is_run = True
        try:
            execute_job_coroutine = self.execute_job()
            self._managing_job(execute_job_coroutine)
            self._actualize_tasks()
        except KeyboardInterrupt:
            scheduler_logger.info("You press Ctrl+C.")
            self._scheduler_is_run = False
            self.stop()

    def restart(self) -> None:
        """Рестарт планировщика."""
        scheduler_logger.info("Restarting scheduler...")
        if self._scheduler_is_run:
            self.stop()

        scheduler_logger.info("Restore data from backup...")
        with BACKUP_PATH.open("r", encoding="utf8") as f:
            backup_data = json.load(fp=f)
            for job_data in backup_data:
                job = Job.from_dict(data=job_data)
                if job is None:
                    continue
                self.schedule(job=job)
        scheduler_logger.info("Backup data has been restored!")

        scheduler_logger.info("Running scheduler again...")
        self.run()

    def _create_backup_data(self) -> tp.Sequence[tp.MutableMapping]:
        """Функция для сборки данных для бэкапа."""
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

    def _actualize_tasks(self):
        self._completed_tasks = {idx: job for idx, job in self._tasks.items()
                                 if job.status not in self.NOT_FINISHED_STATUSES}
        self._tasks = {idx: job for idx, job in self._tasks.items() if job.status in self.NOT_FINISHED_STATUSES}

    def stop(self) -> None:
        """Остановка и бэкап планировщика."""
        scheduler_logger.info("Stopping scheduler...")
        self._actualize_tasks()
        scheduler_logger.info("Creating backup file...")
        data_to_save = self._create_backup_data()
        with BACKUP_PATH.open("w", encoding="utf8") as f:
            json.dump(data_to_save, fp=f, ensure_ascii=False, indent=4)

    def summary(self) -> None:
        """Вывод итоговой информации после завершения планировщика."""
        print("Scheduler summary:")
        print("\tNot finished jobs:")
        for _, not_finished_job in self._tasks.items():
            print(f"\t\t- {not_finished_job}. Status: {not_finished_job.status}")

        print("\tFinished jobs:")
        for _, finished_job in self._completed_tasks.items():
            print(f"\t\t- {finished_job}. Status: {finished_job.status}. Result: {finished_job.result}")
