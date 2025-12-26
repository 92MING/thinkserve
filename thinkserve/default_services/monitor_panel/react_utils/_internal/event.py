import inspect
import asyncio

from dataclasses import dataclass, field
from typing import Callable, List, Any, Awaitable
from concurrent.futures import ThreadPoolExecutor

from .async_runner import run_async_in_sync


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
    if _is_empty_func(listener):
        if asyncio.iscoroutinefunction(listener):
            run_async_in_sync(listener)
        else:
            listener()
    else:
        if asyncio.iscoroutinefunction(listener):
            run_async_in_sync(listener, *args, **kwargs)
        else:
            listener(*args, **kwargs)

@dataclass
class Event[**P]:
    '''
    Event class for managing event listeners.
    
    Generic parameters:
        P: Parameter specification for the event listeners.
    
    When an event is emitted, all subscribed listeners are invoked
    with the provided arguments. If a listener is an asynchronous function,
    it will be awaited.
    '''

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
            pool.submit(_invoke_listener, listener, *args, **kwargs)
    

__all__ = ['Event']