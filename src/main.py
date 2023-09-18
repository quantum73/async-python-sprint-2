import uuid
from datetime import datetime, timedelta

from core.task_examples import TARGET_FUNCS
from core.utils import get_console_logger
from job import Job
from scheduler import Scheduler

main_logger = get_console_logger(name=__name__)


def cold_restart_example() -> None:
    scheduler = Scheduler(pool_size=5)
    main_logger.info('Restart scheduler...')
    scheduler.restart()
    main_logger.info('Scheduler done!')
    scheduler.summary()


def hot_restart_example() -> None:
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
            target_func=TARGET_FUNCS["random_error_func"],
            tries=3,
        )
    )
    scheduler.schedule(
        job=Job(
            idx=uuid.uuid4(),
            target_func=scheduler.restart,
            start_at=now + timedelta(seconds=3),
        )
    )

    main_logger.info('Running scheduler...')
    scheduler.run()
    main_logger.info('Scheduler done!')

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

    main_logger.info('Running scheduler...')
    scheduler.run()
    main_logger.info('Scheduler done!')

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

    main_logger.info('Running scheduler...')
    scheduler.run()
    main_logger.info('Scheduler done!')

    scheduler.summary()


if __name__ == '__main__':
    run_example()
    # run_example_with_max_size_error()
    # cold_restart_example()
    # hot_restart_example()
