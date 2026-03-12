import sys
import random

from pathlib import Path
from functools import cache
from typing import Literal, Any, TYPE_CHECKING
from pydantic import BaseModel, Field, model_validator
from importlib.util import spec_from_file_location, module_from_spec

if TYPE_CHECKING:
    from .service import ServiceWorker

def _simplify_name(name: str) -> str:
    return name.lower().replace(' ', '').replace('_', '').replace('-', '').strip()

def _random_id(k=8) -> str:
    return ''.join(random.choices('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=k))

@cache
def _service_configs_field_name_mapper():
    mapper = {}
    for field_name in ServiceConfigs.model_fields:
        simple_name = _simplify_name(field_name)
        mapper[simple_name] = field_name
    return mapper

class ServiceConfigs(BaseModel):
    '''configurations for the Service class.'''
    
    # region thinkserve basic
    service_path: str
    '''The path to the service implementation file.'''
    service_include_paths: list[str] = Field(default_factory=list)
    '''Additional include paths for the service implementation.
    These paths will be added to `sys.path` when loading the service.'''
    thinkserve_host: str = ''
    '''The host address of the ThinkServe server to connect to.'''
    thinkserve_port: int = -1
    '''The port number of the ThinkServe server to connect to.'''
    thinkserve_auth: str|None = ''
    '''The authentication token for connecting the ThinkServe server.'''
    
    log_level: Literal['VERBOSE', 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']|str = ''
    '''The log level for this service.'''
    # endregion
    
    # region basic
    name: str
    '''
    The name of this service.
    NOTE: Services with the same name MUST having same input/output in same endpoint.
        But it is allowed to have fewer/more endpoints than others.
    '''
    id: str = ''
    '''a unique identifier for this service registration. If not given, it will be `{name}-{random id}``.'''
    category: str|None = None
    '''The category of this service. This will gives affects on routing.'''
    tags: list[str] = Field(default_factory=list)
    '''Tags for this service. If `category` is given, it will be added to tags automatically.'''
    priority: int = 0
    '''The priority of choosing this service when multiple services provide the same endpoint. lower is preferred.'''
    
    use_sandbox: bool = False
    '''If true, service manager and its workers will be run in a sandboxed environment.'''
    executor_type: Literal['python'] = 'python'
    '''The executor type for this service. Currently only 'python' is supported.
    This field will be detected automatically based on the service implementation.
    This field is reserved for future use.'''
    executor_path: str = ''
    '''The path to the executor for this service. If not given, default using the same 
    (python) interpreter as the monitor use.'''
    # endregion
    
    # region worker configs
    init_worker_count: int = 1
    '''
    Number of workers to launch when the service starts.
    If `worker_creation_strategy` is 'eager', this field will be ignored and `max_worker_count` workers will be created at start.
    You can set `0` to not create any worker at start.
    '''
    max_worker_count: int = 1
    '''
    Number of workers to launch for this service.
    NOTE: 1 worker does NOT mean it cannot handle concurrent requests. 
    Methods will be handled concurrently by async & threading.
    '''
    worker_restart_delay_secs: float = 0.0
    '''The delay in seconds before restarting a crashed worker.'''
    worker_copy_mode: Literal['fork', 'spawn'] = 'spawn'
    '''
    The process start mode for worker processes. Default is `spawn`, as this framework is 
    designed for GPU-based ML services, and `spawn` will avoid processes sharing same cuda context.
    NOTE: windows only supports `spawn` mode.
    '''
    worker_creation_strategy: Literal['eager', 'lazy'] = 'eager'
    '''The strategy for creating later worker processes(not including initial workers).
        - eager: create all workers when service starts.
        - lazy: create workers when more workers are needed.
    '''
    worker_scale_up_step: int = 1
    '''When more workers are needed, how many workers to create at once.
    This is only effective when `worker_creation_strategy` is 'lazy'.'''
    worker_startup_timeout_secs: float = 360.0
    '''The timeout in seconds for initializing the service.'''
    worker_startup_try_count: int = 1
    '''
    If a worker fails to start, it will be retried up to this number of times,
    e.g. 1 means try starting the worker once, if fails, try one more time.
    0 means no retry.
    If all retries fail, the service will fail to start.
    '''
    worker_idle_threshold_secs: float = 360.0
    '''
    If a worker has not received any task for the given timeout period (in seconds),
    it will be labeled as `idle`.
    '''
    worker_auto_terminate_strategy: Literal['never', 'kill-idle-immediately', 'kill-idle-on-resource-shortage'] = 'kill-idle-on-resource-shortage'
    '''The strategy for automatically terminating idle workers.
        - never: never terminate idle workers.
        - kill-idle-immediately: terminate idle workers immediately.
        - kill-idle-on-resource-shortage: terminate idle workers only when system resources are insufficient.
            When a new worker is needed and some errors occur (e.g. cuda-out-of-memory), system will try to
            terminate suitable idle workers to free resources.
    '''
    # endregion
    
    # region endpoint configurations
    default_max_task_count: int|None = None
    '''The default maximum number of tasks that can be handled concurrently. If None, no limit.'''
    default_worker_max_task_count: int|None = None
    '''Maximum number of tasks that can be handled concurrently by a single worker. If None, no limit.'''
    default_handle_timeout_secs: float|None = 360.0
    '''The default timeout in seconds for handling a request. If None, no timeout.'''
    default_batch_size: int = 10
    '''
    (for single worker) If >1, enable batching with the given batch size.
    Inputs will wait until enough inputs, or batch_interval_ms is reached.
    NOTE:
        - if max_task_count < batch_size, batch_size=max_task_count.
        - if worker_max_task_count < batch_size, batch_size=worker_max_task_count.
    '''
    default_batch_interval_ms: int = 500
    '''
    The maximum waiting time (in milliseconds) for batching. Default is 500ms.
    NOTE: no effect if batch_size <= 1.
    '''
    default_worker_max_concurrent_batches: int|None = None
    '''How many batches can be processed concurrently by a single worker. If None, no limit.'''
    default_batch_handle_timeout_secs: float|None = None  # type: ignore
    '''The timeout in seconds for handling a batch request. If None, no timeout.
    If not given, use handle_timeout_secs * batch_size'''
    # endregion
    
    # region GPU configs
    service_visible_gpus: list[int]|None = None
    '''
    The list of GPU IDs that this service can use. Currently only supports Nvidia/AMD GPUs.
    This will set the `CUDA_VISIBLE_DEVICES`/`HIP_VISIBLE_DEVICES` environment variable for worker processes.
    '''
    # endregion
    
    extra_configs: dict[str, Any] = Field(default_factory=dict)
    '''Extra configurations for custom use.'''
    
    @classmethod
    def TidyConfigFieldName(cls, name: str) -> str|None:
        '''Get the actual field name from a simplified name.'''
        simple_name = _simplify_name(name)
        mapper = _service_configs_field_name_mapper()
        return mapper.get(simple_name, None)
    
    @model_validator(mode='before')
    @classmethod
    def _PreValidator(cls, data):
        if isinstance(data, dict):
            mapper = _service_configs_field_name_mapper()
            new_data = {}
            extra_configs = data.pop('extra_configs', {})
            extra_configs_key = _simplify_name('extra_configs')
            for key, value in data.items():
                simple_key = _simplify_name(key)
                if simple_key in mapper:
                    new_key = mapper[simple_key]
                    if new_key == extra_configs_key:
                        extra_configs.update(value)
                    else:
                        new_data[new_key] = value
                else:
                    new_data[key] = value
            if 'id' not in new_data or not new_data['id']:
                name = new_data.get('name', None)
                if not name:
                    raise ValueError('No service name given')
                new_data['id'] = f"{name}-{_random_id()}"
            
            new_data['extra_configs'] = extra_configs
            return new_data
        return data
    
    def get_service_type(self)->type['ServiceWorker']:
        '''import `ServiceWorker` class from the service implementation file.
        This is for python executor case only.'''
        if not self.service_path:
            raise ValueError('No service implementation path given')
        if not Path(self.service_path).exists():
            raise FileNotFoundError(f'Service implementation file not found: {self.service_path}')
        for include_path in self.service_include_paths[::-1]:
            if include_path not in sys.path:
                sys.path.insert(0, include_path)
        spec = spec_from_file_location(f'service_impl_{self.id}', self.service_path)
        if not spec or not spec.loader:
            raise ImportError(f'Cannot load service implementation from {self.service_path}')
        module = module_from_spec(spec)
        spec.loader.exec_module(module)
        from thinkserve.service.service import ServiceWorker
        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if isinstance(attr, type) and issubclass(attr, ServiceWorker) and attr is not ServiceWorker:
                return attr
        raise ImportError(f'No ServiceWorker subclass found in {self.service_path}')
        
    def model_post_init(self, _) -> None:
        if not self.id:
            self.id = f"{self.name}-{_random_id()}"
        self.init_worker_count = min(self.init_worker_count, self.max_worker_count)
        if not self.executor_path:
            self.executor_path = sys.executable
            
        if not self.thinkserve_host:
            from ..common_utils.constants import THINKSERVE_HOST
            self.thinkserve_host = THINKSERVE_HOST
        if self.thinkserve_port <= 0:
            from ..common_utils.constants import THINKSERVE_PORT
            self.thinkserve_port = THINKSERVE_PORT
        if self.thinkserve_auth == '':
            from ..common_utils.constants import THINKSERVE_AUTH
            self.thinkserve_auth = THINKSERVE_AUTH
        if not self.log_level:
            from ..common_utils.constants import THINKSERVE_LOG_LEVEL
            self.log_level = THINKSERVE_LOG_LEVEL
        self.log_level = self.log_level.upper()
    
    def update(self, **kwargs) -> tuple[str, ...]:
        '''Update configurations from given keyword arguments.
        Return modified keys.'''
        extra = {}
        modified = []
        for key, value in kwargs.items():
            if (field_name := self.TidyConfigFieldName(key)):
                curr = getattr(self, field_name)
                if curr != value:
                    modified.append(field_name)
                    setattr(self, field_name, value)
            else:
                extra[key] = value
        if extra:
            self.extra_configs.update(extra)
            modified.append('extra_configs')
        self.model_post_init(None)
        return tuple(modified)
    
    def save_to(self, path: str|Path):
        '''Save this configuration to a JSON file.
        If path is a directory, save to `service_config.json` in that directory.'''
        data = self.model_dump_json(indent=4)
        path = Path(path)
        if not path.parent.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
        if path.is_dir():
            path = path / 'service_config.json'
        with open(path, 'w', encoding='utf-8') as f:
            f.write(data)
    
    
__all__ = ["ServiceConfigs"]