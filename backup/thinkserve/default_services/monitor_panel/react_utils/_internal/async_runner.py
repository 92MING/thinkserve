import time
import atexit
import logging
import asyncio
import inspect

from queue import Queue, Empty
from threading import Thread, Event as ThreadEvent
from dataclasses import dataclass, field
from typing import Callable, Any, Awaitable
from contextvars import copy_context, Context

if __name__ == "__main__":
    from nested_loop import get_nested_loop_policy
else:
    from .nested_loop import get_nested_loop_policy

_empty = inspect.Parameter.empty
_logger = logging.getLogger(__name__)

@dataclass
class AsyncTask:
    func: Callable[..., Awaitable[Any]]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    context: Context = field(default_factory=copy_context)
    
    result: Any = _empty
    error: Any = _empty
    
    @property
    def finished(self) -> bool:
        return (self.result is not _empty or self.error is not _empty)

    async def run(self):
        try:
            self.result = await self.context.run(self.func, *self.args, **self.kwargs)
        except Exception as e:
            self.error = e

    def wait_for_task(self):
        while not self.finished:
            time.sleep(0.01)
        if self.error != _empty:
            raise self.error
        return self.result

_async_task_queue = None
_async_task_runner_stop_event = None
_async_task_runner_thread = None

def _get_async_task_runner_stop_event():
    global _async_task_runner_stop_event
    if _async_task_runner_stop_event is None:
        _async_task_runner_stop_event = ThreadEvent()
    return _async_task_runner_stop_event

def _get_async_task_queue():
    global _async_task_queue
    if _async_task_queue is None:
        _async_task_queue = Queue()
    return _async_task_queue

def _init():
    global _async_task_queue, _async_task_runner_stop_event, _async_task_runner_thread
    _get_async_task_queue()
    _get_async_task_runner_stop_event()
    _async_task_runner_thread = Thread(target=_async_task_runner, daemon=True)
    _async_task_runner_thread.start()
    _logger.debug("Async task runner thread started.")

async def _handle_async_tasks():
    e = _get_async_task_runner_stop_event()
    q = _get_async_task_queue()
    while not e.is_set():
        try:
            task: AsyncTask = q.get(block=False)
            await task.run()
        except Empty:
            await asyncio.sleep(0.01)

def _async_task_runner():
    loop = get_nested_loop_policy().get_or_create_event_loop()
    loop.run_until_complete(_handle_async_tasks())

def _stop_async_runner():
    if _async_task_runner_stop_event is not None:
        _async_task_runner_stop_event.set()

atexit.register(_stop_async_runner)

def run_async_in_sync(
    async_func: Callable[..., Awaitable[Any]],
    *args,
    **kwargs
) -> Any:
    if _async_task_queue is None:
        _init()
    if (curr_loop := asyncio._get_running_loop()):
        if getattr(curr_loop, "_nest_patched", False):
            return curr_loop.run_until_complete(async_func(*args, **kwargs))  # type: ignore
    task = AsyncTask(async_func, args, kwargs)
    _get_async_task_queue().put(task)
    return task.wait_for_task()

def wait_coro_in_sync(coro: Awaitable[Any]) -> Any:
    return run_async_in_sync(lambda: coro)


__all__ = ['run_async_in_sync', 'wait_coro_in_sync']


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    
    async def async_task(delay, data: str) -> str:
        await asyncio.sleep(delay)
        print('async task done')
        return data

    async def test():
        print('start testing')
        r = run_async_in_sync(async_task, 1, "Hello, World!")
        print(r)
        
    async def test_wait_coro():
        print('start testing wait_coro_in_sync')
        r = wait_coro_in_sync(async_task(1, "Hello from wait_coro_in_sync!"))
        print(r)
        
    # asyncio.run(test())
    asyncio.run(test_wait_coro())