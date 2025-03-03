import asyncio
import textwrap
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Coroutine
from zoneinfo import ZoneInfo

from core import logger


class Task(ABC):
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.create_time = datetime.now(ZoneInfo("Europe/Moscow"))
        self.id = None
        self.start_time = None
        self.time_to_execute = None

    def __repr__(self):
        return (
            f"<{self.__class__.__name__}("
            f"id={self.id!r}, "
            f"create_time={self.create_time!r}, "
            f"start_time={self.start_time!r}, "
            f"time_to_execute={self.time_to_execute!r}"
            f")>"
        )

    @abstractmethod
    def run(self) -> Any:
        """
        Executes the main logic of the task.

        Must be overridden by subclasses.
        """
        ...

    def __call__(self) -> Coroutine[Any, Any, Any]:
        if self.id is None:
            msg = textwrap.dedent("""\
                    Task ID is not set.
                    Make sure to store the task in the database before execution.
                    That is needed to get a unique id connected to the database.
                """)
            logger.exception(msg=msg)
            raise RuntimeError(msg)

        self.start_time = datetime.now(ZoneInfo("Europe/Moscow"))
        return asyncio.to_thread(self.run)


# Decorator [as_task] not implemented yet :(
# There are some troubles with [pickle] module

# class FunctionTask(Task):
#     def run(self):
#         return self.func(*self.args, **self.kwargs)


# def as_task(func):
#     """
#     Example:

#         @as_task
#         def my_task(x, y):
#             return x + y

#         task_instance = my_task(2, 3)
#         result = task_instance()
#         time_to_execute = task_instance.time_to_execute
#     """

#     def wrapper(*args, **kwargs):
#         task = FunctionTask(*args, **kwargs)
#         task.func = func
#         return task

#     return wrapper
