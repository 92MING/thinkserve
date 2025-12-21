import random

from typing import Literal
from functools import cache
from pydantic import BaseModel, Field, model_validator


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
    
    # region basic
    name: str
    '''
    The name of this service.
    NOTE: Services with the same name MUST having same input/output in same endpoint.
        But it is allowed to have fewer/more endpoints than others.
    '''
    id: str
    '''a unique identifier for this service registration. If not given, it will be `{name}-{random id}``.'''
    category: str|None = None
    '''The category of this service. This will gives affects on routing.'''
    tags: list[str] = Field(default_factory=list)
    '''Tags for this service. If `category` is given, it will be added to tags automatically.'''
    priority: int = 0
    '''The priority of choosing this service when multiple services provide the same endpoint. lower is preferred.'''
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
    '''The strategy for creating worker processes.
        - eager: create all workers when service starts.
        - lazy: create workers when more workers are needed.
    '''
    worker_scale_up_step: int = 1
    '''When more workers are needed, how many workers to create at once.
    This is only effective when `worker_creation_strategy` is 'lazy'.'''
    worker_startup_timeout_secs: float = 180.0
    '''The timeout in seconds for initializing the service.'''
    worker_startup_try_count: int = 1
    '''
    If a worker fails to start, it will be retried up to this number of times,
    e.g. 1 means try starting the worker once, if fails, try one more time.
    0 means no retry.
    If all retries fail, the service will fail to start.
    '''
    worker_idle_timeout_secs: float = 180.0
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
    default_handle_timeout_secs: float|None = 180.0
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
    The maximum waiting time (in milliseconds) for batching.
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
            for key, value in data.items():
                simple_key = _simplify_name(key)
                if simple_key in mapper:
                    new_key = mapper[simple_key]
                    new_data[new_key] = value
                else:
                    new_data[key] = value
            if 'id' not in new_data or not new_data['id']:
                name = new_data.get('name', None)
                if not name:
                    raise ValueError('No service name given')
                new_data['id'] = f"{name}-{_random_id()}"
            return new_data
        return data
    
    
__all__ = ["ServiceConfigs"]