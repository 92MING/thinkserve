import time
import asyncio

from pydantic import BaseModel
from typing import TYPE_CHECKING
from functools import partial
from multiprocessing import Process

from .configs import ServiceConfigs
from .register import EndpointInfo
from .comm import EventSocketServer, find_available_port

from ..common_utils.debug_utils import Logger, get_logger

if TYPE_CHECKING:
    from .service import ServiceWorker

class ServiceStat(BaseModel):
    '''runtime statistics of a service.'''
    total_requests: int = 0
    '''Total number of requests handled by this service.'''
    total_errors: int = 0
    '''Total number of errors occurred in this service.'''
    average_response_time_secs: float = 0.0
    '''Average response time (in seconds) of this service.'''

class ServiceManager:
    
    _service_type: type["ServiceWorker"]
    _configs: ServiceConfigs
    _stat: ServiceStat
    _server: EventSocketServer
    
    def __init__(
        self, 
        configs: ServiceConfigs, 
        service: type["ServiceWorker"],
    ) -> None:
        self._configs = configs
        self._service_type = service
        self._stat = ServiceStat()
        self._server = EventSocketServer(
            host='localhost',
            port=find_available_port(),
            name=f'manager-{configs.id}',
        )

    @property
    def logger(self)->Logger:
        if not (logger:=getattr(self, '_logger', None)):
            logger = self._logger = get_logger(self._configs.id)
        return logger

    # region shared methods
    def get_configs(self) -> ServiceConfigs:
        return self._configs
    
    def get_runtime_info(self) -> ServiceStat:
        return self._stat
    # endregion
    
    def start(self):
        ...
    
    
    
__all__ = [
    "ServiceManager",
    "ServiceStat",
]