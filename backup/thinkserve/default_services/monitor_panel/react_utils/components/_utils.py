import inspect
import asyncio

from asyncio import _get_running_loop
from dataclasses import dataclass, field
from typing import Callable, List, Any, Awaitable
from concurrent.futures import ThreadPoolExecutor

type _Listener[**R, T] = Callable[R, T|Awaitable[T]]|Callable[[], T|Awaitable[T]]
_threadpool = None

def _get_threadpool() -> ThreadPoolExecutor:
    global _threadpool
    if _threadpool is None:
        _threadpool = ThreadPoolExecutor()
    return _threadpool

def _is_empty_func(f):
    try:
        return len(inspect.signature(f).parameters) == 0
    except ValueError:
        return False
    
def _invoke_listener(listener, *args, **kwargs):
    params = inspect.signature(listener).parameters
    if not params:
        return listener()
    return listener(*args, **kwargs)

def _invoke_async_listener(listener, *args, **kwargs):
    loop = _get_running_loop()
    if not loop:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    if _is_empty_func(listener):
        return loop.run_until_complete(listener())
    return loop.run_until_complete(listener(*args, **kwargs))
    
@dataclass
class Event[**P]:
    
    listeners: List[_Listener[P, Any]] = field(default_factory=list)

    def subscribe(self, listener: _Listener[P, Any]) -> None:
        self.listeners.append(listener)
        
    def unsubscribe(self, listener: _Listener[P, Any]) -> None:
        try:
            self.listeners.remove(listener)
        except ValueError:
            pass
    
    def emit(self, *args: P.args, **kwargs: P.kwargs) -> None:
        pool = _get_threadpool()
        for listener in self.listeners:
            if asyncio.iscoroutinefunction(listener):
                pool.submit(_invoke_async_listener, listener, *args, **kwargs)
            else:
                pool.submit(_invoke_listener, listener, *args, **kwargs)
    

__all__ = ['Event']