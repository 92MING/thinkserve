import time
import asyncio

from dataclasses import dataclass, field
from pydantic import BaseModel
from typing import TYPE_CHECKING
from functools import partial

from .configs import ServiceConfigs
from .register import EndpointInfo
from .comm import MessageQueue

if TYPE_CHECKING:
    from .service import Service

class RuntimeInfo(BaseModel):
    ...

@dataclass
class ServiceWorkerProxy:
    
    input_message_pool: MessageQueue = field(default_factory=partial(MessageQueue, mode='send'))
    output_message_pool: MessageQueue = field(default_factory=partial(MessageQueue, mode='receive'))


class ServerManager:
    
    _service_type: type["Service"]
    _configs: ServiceConfigs
    _runtime_info: RuntimeInfo
    _endpoints: dict[str, EndpointInfo]
    
    workers: dict[int, ServiceWorkerProxy]
    '''{worker_id: ServiceWorkerProxy} mapping of all worker proxies.'''
    
    def __init__(
        self, 
        configs: ServiceConfigs, 
        service: type["Service"],
    ) -> None:
        self._configs = configs
        self._service_type = service
        self._endpoints = self._generate_endpoints(configs)
        self._runtime_info = RuntimeInfo()
        self._grpc_server = None
        self._grpc_service = None
    
    # region shared methods
    def get_configs(self) -> ServiceConfigs:
        return self._configs
    
    def get_runtime_info(self) -> RuntimeInfo:
        return self._runtime_info
    
    def get_endpoints(self) -> dict[str, EndpointInfo]:
        return self._endpoints
    # endregion
    
    def _generate_endpoints(self, config: ServiceConfigs)-> dict[str, EndpointInfo]:
        ...
    
    def _generate_service(self):
        if self._grpc_service is not None:
            return self._grpc_service
        ...
    
    
    
__all__ = [
    "ServerManager",
    "RuntimeInfo",
]