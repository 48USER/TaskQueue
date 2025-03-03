import random
import time
from contextlib import asynccontextmanager
from enum import Enum

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from sqlalchemy.future import select

from core import AsyncSessionLocal, Task, TaskModel, TaskQueue, init_db


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    app.state.queue = TaskQueue(db_session_factory=AsyncSessionLocal)
    yield
    # await drop_db()
    app.state.queue.shutdown()


app = FastAPI(lifespan=lifespan)


# @as_task
# def mock_task(delay=None):
#     if delay is None:
#         delay = random.randint(0, 10)
#     time.sleep(delay)


class MockTask(Task):
    def __init__(self, delay=None):
        super().__init__()
        self.delay = delay if delay is not None else random.randint(0, 10)

    def run(self):
        time.sleep(self.delay)


@app.post("/tasks/mock", tags=["Tasks"])
async def create_mock_task(delay: int = None):
    # task = mock_task(delay=delay)
    task = MockTask(delay=delay)
    task_id = await app.state.queue.upload_task(task)
    return {"task_id": task_id}


class TaskStatus(str, Enum):
    IN_QUEUE = "In Queue"
    RUNNING = "Run"
    COMPLETED = "Completed"


@app.get("/tasks/{task_id}", tags=["Tasks"])
async def get_task_status(task_id: int):
    active_tasks = app.state.queue.get_active_tasks()
    for id, create_time, start_time in active_tasks:
        if id == task_id:
            return JSONResponse(
                {
                    "task_id": task_id,
                    "status": TaskStatus.RUNNING,
                    "create_time": create_time,
                    "start_time": start_time,
                    "time_to_execute": None,
                }
            )
    else:
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(TaskModel).filter(TaskModel.id == task_id)
            )
            task_record = result.scalars().first()

        if task_record is None:
            raise HTTPException(
                status_code=404, detail=f"There is no task with such id - {task_id}"
            )

        if task_record.time_to_execute is None:
            status = TaskStatus.IN_QUEUE
            start_time = None
        else:
            status = TaskStatus.COMPLETED
            start_time = task_record.start_time.timestamp()

        return JSONResponse(
            {
                "task_id": task_id,
                "status": status,
                "create_time": task_record.create_time.timestamp(),
                "start_time": start_time,
                "time_to_execute": task_record.time_to_execute,
            }
        )
