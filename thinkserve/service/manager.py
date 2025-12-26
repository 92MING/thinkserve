import os
import sys
import time
import asyncio
import multiprocessing as mp

from threading import Lock
from pydantic import BaseModel
from dataclasses import dataclass, field

from .configs import ServiceConfigs
from .comm import EventSocketServer, EventSocketClient, find_available_port
from .service import ServiceWorker, _invoke_event, _event, ServiceWorkerInfo

from ..common_utils.debug_utils import Logger, get_logger, LogMessage

class ServiceStat(BaseModel):
    '''runtime statistics of a service.'''
    total_requests: int = 0
    '''Total number of requests handled by this service.'''
    total_errors: int = 0
    '''Total number of errors occurred in this service.'''
    average_response_time_secs: float = 0.0
    '''Average response time (in seconds) of this service.'''

def _start_service_worker(
    service_type: type[ServiceWorker],
    worker_info: ServiceWorkerInfo,
    configs: ServiceConfigs,
):
    ...
    
@dataclass
class _ServiceWorker:
    info: ServiceWorkerInfo
    '''The information of this worker.'''
    process: mp.Process
    '''The process of this worker.'''
    pid: int
    '''The process ID of this worker.'''
    started: bool = False
    
    # stats
    handled_tasks: dict[str, int] = field(default_factory=dict)
    '''Number of tasks handled by each endpoint. Key is endpoint name.'''
    error_counts: dict[str, int] = field(default_factory=dict)
    '''Number of errors occurred in each endpoint. Key is endpoint name.'''
    
    @property
    def id(self) -> int:
        return self.info.worker_id


class ServiceManager:
    
    _service_type: type["ServiceWorker"]
    '''The type of the service worker managed by this manager.'''
    _configs: ServiceConfigs
    '''The configurations of this service manager.'''
    _stat: ServiceStat
    '''The runtime statistics of this service.'''
    _thinkserve_client: EventSocketClient
    '''The client to communicate with ThinkServe server.'''
    _server: EventSocketServer
    '''The event socket server for communication with workers.'''
    _name: str
    '''The name of this service manager.
    NOTE: this is not the service name, but `{service_name}-manager-{service_id}``.'''
    _workers: dict[int, _ServiceWorker]
    '''The workers managed by this service manager. Key is worker_id.'''
    _started: bool = False
    '''Whether the service manager has been started.'''
    
    # locks
    _worker_stat_locks: dict[int, dict[str, Lock]]
    '''Locks for updating worker statistics. {worker_id: {stat_name: Lock}}'''
    _stat_locks: dict[str, Lock]
    '''Locks for updating service statistics. {stat_name: Lock}'''
    
    def __init__(
        self, 
        configs: ServiceConfigs, 
        service: type["ServiceWorker"],
    ) -> None:
        self._configs = configs
        self._service_type = service
        self._stat = ServiceStat()
        self._workers = {}
        self._worker_stat_locks = {}
        self._name = f'{configs.name}-manager-{configs.id}'
        if os.name == 'nt':
            self._server = EventSocketServer(
                host='localhost',
                port=find_available_port(),
                name=self._name,
            )
        else:
            # AF_UNIX socket
            self._server = EventSocketServer(
                host='localhost',
                identifier=self._name,
                name=self._name,
            )

    @property
    def logger(self)->Logger:
        if not (logger:=getattr(self, '_logger', None)):
            logger = self._logger = get_logger(self._name)
            if __name__ != '__main__':
                ...
        return logger
    
    @property
    def name(self)->str:
        return self._name

    # region event methods
    @_event
    def _worker_started(self, worker_id: int):
        # called by worker when it started
        if worker_id in self._workers:
            self.logger.info(f'Worker {worker_id} has started.')
            self._workers[worker_id].started = True    
    
    @_event
    def _worker_log(self, worker_id: int, log_msg: LogMessage):
        ...
    
    def _get_configs(self) -> ServiceConfigs:
        return self._configs
    
    def _get_runtime_info(self) -> ServiceStat:
        return self._stat
    # endregion
    
    def start(self):
        if self._started:
            self.logger.warning("ServiceManager has already been started.")
            return
        self._started = True
        self._server.start()
        worker_copy_mode = self._configs.worker_copy_mode
        if os.name == 'nt' and worker_copy_mode == 'fork':
            self.logger.warning("On Windows, 'fork' mode is not supported. Using 'spawn' mode instead.")
            worker_copy_mode = 'spawn'
        timeout = self._configs.worker_startup_timeout_secs
        
        self._mp_ctx = mp.get_context(worker_copy_mode)
        for worker_id in range(self._configs.init_worker_count):
            worker_info = ServiceWorkerInfo(
                worker_id=worker_id,
                worker_name=ServiceWorkerInfo.CreateWorkerName(service_id=self._configs.id, worker_id=worker_id),
                manager_server_port=self._server.port,
                manager_server_identifier=self._server.identifier,
            )
            p = self._mp_ctx.Process(
                target=_start_service_worker,
                args=(
                    self._service_type,
                    worker_info,
                    self._configs,
                ),
            )
            p.start()
            self.logger.info(f'Starting worker {worker_id} (pid={p.pid})...')
            self._workers[worker_id] = _ServiceWorker(process=p, info=worker_info, pid=p.pid)   # type: ignore
    
    def stop(self):
        if self._started:
            self._server.stop()
            for wid, worker in self._workers.items():
                self.logger.info(f'Terminating worker {wid} (pid={worker.process.pid})...')
                worker.process.terminate()
            self._started = False
        
    def __del__(self):
        if self._started:
            self.stop()
    
__all__ = [
    "ServiceStat",
    "ServiceManager",
]