import time

from dataclasses import dataclass, field
from typing import (TYPE_CHECKING, Callable, TypeVar, Any, Awaitable)

from .manager import ServiceManager
from .comm import EventSocketClient

from ..common_utils.type_utils import is_empty_method
from ..common_utils.concurrent_utils import wait_coroutine
from ..common_utils.debug_utils import get_logger, Logger

if TYPE_CHECKING:
    from .register import EndpointInfo
    from .configs import ServiceConfigs


@dataclass(frozen=True)
class ServiceWorkerInfo:
    '''extra information for service worker instance.'''
    worker_name: str
    '''A unique name for this worker instance, usually `{service_id}-worker{worker_id}``.'''
    worker_id: int
    '''[0, worker_count -1], the unique id of this worker.'''
    start_time: float = field(default_factory=lambda: time.time())
    '''The timestamp when this worker started.'''
    manager_server_port: int|None = None
    '''The port number of the manager process. Can be None in `identifier` mode.'''
    manager_server_identifier: str|None = None
    '''The identifier of the manager process. Can be None in `port` mode.'''
    
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
def _event(func: _F) -> _F:
    setattr(func, '_is_event', True)
    return func

class ServiceWorker:
    
    info: ServiceWorkerInfo
    '''Information about this service instance. This field is immutable.'''
    manager: ServiceManager
    '''
    The manager that manages this service instance.
    NOTE: this is actually a multiprocess-proxy. Only certain attributes & methods are accessible.
    '''
    endpoints: dict[str, Endpoint]
    '''{endpoint_name: _Endpoint} mapping of all endpoints registered in this service.'''
    event_client: EventSocketClient
    '''The event socket client for this service worker.'''
    
    _name: str
    '''A unique name for this service worker. Not changeable.'''
    
    def __init__(
        self, 
        info: ServiceWorkerInfo, 
        manager: ServiceManager,
    ) -> None:
        self.info = info
        self.manager = manager
        configs = self.manager.get_configs()
        self._name = info.worker_name   # for convenience
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
        
        if not is_empty_method(self.initialization):
            r = self.initialization()
            if isinstance(r, Awaitable):
                wait_coroutine(r)
        
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
                event_name = method.__name__.strip('_')
                self.event_client.event(name=event_name)(getattr(self, method.__name__))
        self.event_client.start()
        
    # region hook methods
    def initialization(self):
        '''
        A hook method called right after the worker is created.
        You can define any initialization logic here. If not overridden, this method does nothing.
        NOTE: 
         - this method can be async or sync.
         - During this method, `event_client` has not started yet, so you CANNOT use event communication here.
        '''
    # endregion
    
    @property
    def name(self):
        return self._name
    
    @property
    def logger(self)->Logger:
        if not (logger:=getattr(self, '_logger', None)):
            logger = self._logger = get_logger(self.name)
        return logger
    
    @_event
    def _update_endpoint(
        self,
        endpoint: str,
        params: dict[str, Any]
    ):
        if not (endpoint_info := self.endpoints.get(endpoint, None)):
            self.logger.warning(f'No such endpoint "{endpoint}" to update.')
        else:
            endpoint_info.info.update_from_kwargs(**params)
            
    @_event
    def _call_endpoint(
        self,
        endpoint: str,
    ):
        ...
    
    
    
__all__ = ['ServiceWorker', 'ServiceWorkerInfo']