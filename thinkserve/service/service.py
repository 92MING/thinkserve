import uuid

from threading import Thread
from dataclasses import dataclass
from multiprocessing.queues import Queue
from typing import TYPE_CHECKING, Callable, TypedDict, TypeVar, ParamSpec

from .manager import ServerManager, _ReceivedEventOutputPool

if TYPE_CHECKING:
    from .register import EndpointInfo
    from .configs import ServiceConfigs

_P = ParamSpec('_P')
_R = TypeVar('_R')

@dataclass(frozen=True)
class ServiceInfo:
    worker_id: int
    '''[0, worker_count -1], the unique id of this worker.'''
    
    
@dataclass
class _Endpoint:
    func: Callable
    info: 'EndpointInfo'

    def update(self, config: "ServiceConfigs"):
        self.info._update(config)

class _EventInput(TypedDict):
    id: str
    event: str
    args: tuple
    kwargs: dict
    
class _EventOutput(TypedDict):
    id: str
    result: object|None
    exception: Exception|None
    
def _random_event_id() -> str:
    return str(uuid.uuid4())

class Service:
    
    info: ServiceInfo
    '''Information about this service instance. This field is immutable.'''
    manager: ServerManager
    '''
    The manager that manages this service instance.
    NOTE: this is actually a multiprocess-proxy. Only certain attributes & methods are accessible.
    '''
    endpoints: dict[str, _Endpoint]
    '''{endpoint_name: _Endpoint} mapping of all endpoints registered in this service.'''
    input_queue: Queue[_EventInput]
    '''Queue for accepting events from manager.'''
    output_queue: Queue[_EventOutput]
    '''Queue for sending event results back to manager.'''
    event_listener_thread: Thread
    '''Thread for listening to events from input_queue.'''
    
    def __init__(
        self, 
        info: ServiceInfo, 
        manager: ServerManager,
        input_queue: Queue,
        output_queue: Queue,
    ) -> None:
        self.info = info
        self.manager = manager
        self.input_queue = input_queue
        self.output_queue = output_queue
        configs = self.manager.get_configs()
        self.endpoints = {}
        
        self.event_listener_thread = Thread(target=self._queue_listener)
        self.event_listener_thread.daemon = True
        self.event_listener_thread.start()
        
        # init endpoints
        for attr_name in dir(self):
            if hasattr(Service, attr_name):
                continue    # skip inherited attributes
            attr = getattr(self, attr_name)
            if hasattr(attr, '_thinkserve_endpoint_info'):
                endpoint_info: "EndpointInfo" = getattr(attr, '_thinkserve_endpoint_info')
                endpoint_info._update(configs)
                self.endpoints[attr_name] = _Endpoint(
                    func=attr,
                    info=endpoint_info
                )
    
    def __init_subclass__(cls):
        pass
    
    # region events
    @classmethod
    async def _Invoke(
        cls, 
        func: Callable[_P, _R], 
        input_queue: Queue[_EventInput],
        output_pool: _ReceivedEventOutputPool,
        *args: _P.args, 
        **kwargs: _P.kwargs
    ) -> _R:
        '''
        Invoke an endpoint function in the service instance.
        This method will be called by manager. If any error occurs, it will also be raised.
        '''
        id = _random_event_id()
        event_input: _EventInput = {
            'id': id,
            'event': func.__name__,
            'args': args,
            'kwargs': kwargs,
        }
        input_queue.put(event_input)
        r = await output_pool.get(id)   # will raise TimeoutError if timeout
        if r['exception'] is not None:
            raise r['exception']
        return r['result']  # type: ignore
    
    def _queue_listener(self):
        ...
    
    def _stop(self):
        ...
        
    def _handle_task(self, ):
        ...
    # endregion
    
    
    
    
__all__ = ['Service', 'ServiceInfo']