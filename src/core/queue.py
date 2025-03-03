import asyncio
import ctypes
import textwrap
import time
from multiprocessing import Manager, Pool

from sqlalchemy.future import select

from core import NUM_WORKERS, Task, TaskModel, logger


class RunningTaskStats(ctypes.Structure):
    _fields_ = [
        ("id", ctypes.c_int),
        ("create_time", ctypes.c_double),
        ("start_time", ctypes.c_double),
    ]


class TaskQueue:
    def __init__(self, db_session_factory):
        logger.info("Initializing the task queue")

        self.manager = Manager()
        # Some kind of cache to prevent unnecessary database query when starting a task execution
        self.active_tasks_cache = self.manager.list(
            [RunningTaskStats(-1, -1.0, -1.0)] * NUM_WORKERS
        )
        self.pool = Pool(processes=NUM_WORKERS)
        self.db = db_session_factory

    @staticmethod
    def _process_task(task: Task, active_tasks_cache):
        logger.info(f"Processing task {task.id}")

        for idx, elem in enumerate(active_tasks_cache):
            if elem.id == -1:
                active_tasks_idx = idx
                task_cor = task()
                active_tasks_cache[active_tasks_idx] = RunningTaskStats(
                    task.id,
                    task.create_time.timestamp(),
                    task.start_time.timestamp(),
                )
                break
        else:
            msg = textwrap.dedent("""\
                    No free slot available in [active_tasks].
                    It appears that one of the pool processes was terminated externally.
                """)
            logger.error(msg=msg)
            raise RuntimeError(msg)

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            _result = loop.run_until_complete(task_cor)
            task.time_to_execute = time.time() - task.start_time.timestamp()
        finally:
            active_tasks_cache[active_tasks_idx] = RunningTaskStats(-1, -1.0, -1.0)

        logger.info(f"Task {task.id} successfully processed")
        return {
            "id": task.id,
            "start_time": task.start_time,
            "time_to_execute": task.time_to_execute,
        }

    async def _store_task_in_db(self, task: Task):
        logger.info("Storing new task in the database...")
        async with self.db() as session:
            async with session.begin():
                task_record = TaskModel(
                    create_time=task.create_time,
                    start_time=task.start_time,
                    time_to_execute=task.time_to_execute,
                )
                session.add(task_record)
            await session.commit()
            task.id = task_record.id
        logger.info(f"Task {task.id} successfully stored in the database")
        return task.id

    async def _store_execution_stats(self, data):
        logger.info(
            f"Updating task {data['id']} record in the database with execution stats"
        )
        async with self.db() as session:
            result = await session.execute(
                select(TaskModel).filter(TaskModel.id == data["id"])
            )
            task_record = result.scalars().first()
            if task_record:
                task_record.start_time = data["start_time"]
                task_record.time_to_execute = data["time_to_execute"]
            await session.commit()

    def _store_execution_stats_callback(self, data):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self._store_execution_stats(data))
        loop.close()

    async def upload_task(self, task: Task):
        logger.info("Uploading new task...")
        task_id = await self._store_task_in_db(task)
        self.pool.apply_async(
            self._process_task,
            args=(task, self.active_tasks_cache),
            callback=self._store_execution_stats_callback,
        )
        logger.info(f"Task {task.id} successfully uploaded")
        return task_id

    def get_active_tasks(self):
        return [
            (stats.id, stats.create_time, stats.start_time)
            for stats in self.active_tasks_cache
            if stats.id != -1
        ]

    def shutdown(self):
        logger.info("Shutting down the task queue")
        self.pool.close()
        self.pool.join()
        self.active_tasks_cache = self.manager.list(
            [RunningTaskStats(-1, -1.0, -1.0)] * NUM_WORKERS
        )
