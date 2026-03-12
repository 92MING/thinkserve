import time
import inspect
import logging
import asyncio

from dataclasses import dataclass, field
from typing import (TYPE_CHECKING, Callable, TypeVar, Any, Awaitable, ParamSpec)
from typing_extensions import Concatenate

from .comm import EventSocketClient, EventCommunicationBase

from ..common_utils.debug_utils import get_logger, Logger, LogMessage
from ..common_utils.type_utils import is_empty_method, pack_function_params
from ..common_utils.concurrent_utils import wait_coroutine, SyncOrAsyncFunc, is_async_callable, get_threadpool

if TYPE_CHECKING:
    from .register import EndpointInfo
    from .configs import ServiceConfigs

@dataclass(frozen=True)
class ServiceWorkerInfo:
    '''extra information for service worker instance.'''
    worker_name: str
    '''A unique name for this worker instance, i.e. `worker{worker_id}-{service_id}``.'''
    worker_id: int
    '''[0, max_worker_count -1], the unique id of this worker.'''
    start_time: float = field(default_factory=lambda: time.time())
    '''The timestamp when this worker started.'''
    manager_server_port: int|None = None
    '''The port number of the manager process. Can be None in `identifier` mode.'''
    manager_server_identifier: str|None = None
    '''The identifier of the manager process. Can be None in `port` mode.'''
    
    @staticmethod
    def CreateWorkerName(service_id: str, worker_id: int) -> str:
        return f'worker{worker_id}-{service_id}'
    
@dataclass
class Endpoint:
    func: Callable
    info: 'EndpointInfo'

    def update(self, config: "ServiceConfigs|None"=None, **extra_fields) -> None:
        if config:
            self.info.update_from_config(config)
        if extra_fields:
            self.info.update_from_kwargs(**extra_fields)

_F = TypeVar('_F', bound=Callable[..., Any])
_P = ParamSpec('_P')
_R = TypeVar('_R')

def _event(func: _F) -> _F:
    setattr(func, '_is_event', True)
    return func

async def _invoke_event(
    client: EventCommunicationBase, 
    to_client: str, 
    f: Callable[Concatenate[Any, _P], _R], 
    *args: _P.args, 
    **kwargs: _P.kwargs
) -> _R:
    params = pack_function_params(f, args, kwargs, return_detail=False)
    return_anno = inspect.signature(f).return_annotation
    return await client.invoke(to_client, f.__name__, params, return_type=return_anno)

class _LogCallbackHandler(logging.Handler):
    def __init__(self, callback: SyncOrAsyncFunc[[LogMessage], Any]):
        super().__init__()
        if is_async_callable(callback):
            self.callback = lambda log_msg: asyncio.run(callback(log_msg))  # type: ignore
        else:
            self.callback = callback
        self.threadpool = get_threadpool()
        
    def emit(self, record: logging.LogRecord) -> None:
        log_message = LogMessage(
            name=record.name,
            level=record.levelname,
            message=self.format(record)
        )
        self.threadpool.submit(self.callback, log_message)  # type: ignore
                
class ServiceWorker:
    
    info: ServiceWorkerInfo
    '''Information about this service instance. This field is immutable.'''
    endpoints: dict[str, Endpoint]
    '''{endpoint_name: _Endpoint} mapping of all endpoints registered in this service.'''
    event_client: EventSocketClient
    '''The event socket client for this service worker.'''
    
    def __init__(
        self, 
        info: ServiceWorkerInfo,
        configs: "ServiceConfigs", 
    ) -> None:
        self.info = info
        self.endpoints = {}
        
        # init endpoints
        for attr_name in dir(self):
            if hasattr(ServiceWorker, attr_name):
                continue    # skip inherited attributes
            attr = getattr(self, attr_name)
            if hasattr(attr, '_thinkserve_endpoint_info'):
                endpoint_info: "EndpointInfo" = getattr(attr, '_thinkserve_endpoint_info')
                endpoint_info.update_from_config(configs)
                self.endpoints[attr_name] = Endpoint(
                    func=attr,
                    info=endpoint_info
                )
        
        # register communication events
        if info.manager_server_port is not None:
            self.event_client = EventSocketClient(host='localhost', port=info.manager_server_port, name=self.name)
        else:
            self.event_client = EventSocketClient(host='localhost', identifier=info.manager_server_identifier, name=self.name)    # type: ignore
        for attr in dir(self.__class__):
            if attr.startswith('__'):
                continue
            method = getattr(self.__class__, attr)
            if hasattr(method, '_is_event'):
                self.event_client.event(name=method.__name__)(getattr(self, method.__name__))
        self.event_client.start()
        
        if not is_empty_method(self.initialization):
            r = self.initialization()
            if isinstance(r, Awaitable):
                wait_coroutine(r)
        
        manager_client_id = next(iter(self.event_client._clients.keys()))
        from .manager import ServiceManager
        wait_coroutine(_invoke_event(
            self.event_client,
            to_client=manager_client_id,
            f=ServiceManager._worker_started,
            worker_id=self.info.worker_id
        ))
    
    # region hook methods
    def initialization(self):
        '''
        A hook method called right after the worker is created.
        You can define any initialization logic here. If not overridden, this method does nothing.
        This method can be async or sync.
        '''
    # endregion
    
    @property
    def name(self)->str:
        return self.info.worker_name
    
    @property
    def worker_id(self)->int:
        return self.info.worker_id

    async def _send_log_to_manager(self, log_msg: LogMessage):
        from .manager import ServiceManager
        manager_client_id = next(iter(self.event_client._clients.keys()))
        await _invoke_event(
            self.event_client,
            to_client=manager_client_id,
            f=ServiceManager._worker_log,
            worker_id=self.worker_id,
            log_msg=log_msg
        )
    
    @property
    def logger(self)->Logger:
        if not (logger:=getattr(self, '_logger', None)):
            logger = self._logger = get_logger(self.name)
            if __name__ != '__main__':
                log_handler = _LogCallbackHandler(callback=self._send_log_to_manager)
                logger.addHandler(log_handler)
        return logger
    
    # region event methods
    @_event
    def _update_endpoint(
        self,
        endpoint: str,
        params: dict[str, Any]
    ):
        '''change configs of an endpoint.'''
        if not (endpoint_info := self.endpoints.get(endpoint, None)):
            self.logger.warning(f'No such endpoint "{endpoint}" to update.')
        else:
            endpoint_info.info.update_from_kwargs(**params)
            
    @_event
    def _update_config(self, params: dict[str, Any]):
        '''change configs of this service worker.'''
        ...
            
    @_event
    def _call_endpoint(
        self,
        endpoint: str,
        **params: Any
    ):
        ...
    # endregion
    
__all__ = ['ServiceWorker', 'ServiceWorkerInfo']