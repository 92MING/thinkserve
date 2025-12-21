import inspect

from math import ceil
from dataclasses import dataclass
from functools import cache, partial
from typing import (TYPE_CHECKING, TypeAlias, Callable, AsyncIterator, Awaitable, TypeVar,
                    Sequence, TypedDict, get_origin, get_args, AsyncIterable, AsyncGenerator)
from typing_extensions import overload, TypeAliasType, Unpack

from ..common_utils.type_utils import is_serializable, deserialize, check_value_is
from ..common_utils.debug_utils import get_logger

if TYPE_CHECKING:
    from .message import Message, StreamMessage, ServeContext
    from .service import ServiceWorker
    from .configs import ServiceConfigs

_empty = inspect.Parameter.empty
_logger = get_logger(__name__)

def _simplify_name(name: str) -> str:
    return name.lower().replace(' ', '').replace('_', '').replace('-', '').strip()

@cache
def _endpoint_info_field_name_mapper():
    mapper = {}
    for field_name in EndpointInfo.__dataclass_fields__:
        simple_name = _simplify_name(field_name)
        mapper[simple_name] = field_name
    return mapper

@cache
def _endpoint_info_field_annos():
    annos = {}
    for field_name, field in EndpointInfo.__dataclass_fields__.items():
        annos[field_name] = field.type
    return annos

@cache
def _endpoint_info_field_serializer(field: str):
    if not (field := EndpointInfo.TidyFieldName(field)):    # type: ignore
        return None
    anno = _endpoint_info_field_annos().get(field, None)
    if not anno or anno == inspect.Parameter.empty:
        return None
    if not is_serializable(anno):
        return None
    return partial(deserialize, target_type=anno)
    
@dataclass
class EndpointInfo:
    
    name: str
    '''The name of this endpoint. If not given, use the function name.'''
    description: str|None = None
    '''
    The description of this endpoint. If not given, use the function docstring.
    This is for documentation purpose only.
    '''
    
    input_streaming: bool = False
    '''Whether the input is streaming.'''
    output_streaming: bool = False
    '''Whether the output is streaming.'''
    input_type: type['Message']|type['StreamMessage'] = None    # type: ignore
    '''The type of input message.'''
    output_type: type['Message']|type['StreamMessage'] = None    # type: ignore
    '''The type of output message.'''
    batch_input_type: type['list']|type['tuple']|None = None
    '''preferred batch input type (list or tuple).'''
    
    # task config
    max_task_count: int|None = _empty    # type: ignore
    '''
    Maximum number of tasks that can be handled concurrently. If None, no limit.
    NOTE: it counts on the sum of all workers.
    '''
    worker_max_task_count: int|None = _empty    # type: ignore
    '''
    Maximum number of tasks that can be handled concurrently by a single worker. If None, no limit.
    By default, it is calculated as ceil(max_task_count / max_worker_count) if max_task_count is not given.
    '''
    handle_timeout_secs: float|None = _empty    # type: ignore
    '''The timeout in seconds for handling a request. If None, no timeout.
    If not given, it will try to use `ServiceConfigs.default_handle_timeout_secs`'''
    
    # batch config
    batching_enabled: bool = False
    '''Whether batching is enabled. Use @register_batch to define a batch endpoint.'''
    batch_size: int = _empty    # type: ignore
    '''
    (for single worker) If >1, enable batching with the given batch size.
    Inputs will wait until enough inputs, or batch_interval_ms is reached.
    NOTE:
        - if max_task_count < batch_size, batch_size=max_task_count.
        - if worker_max_task_count < batch_size, batch_size=worker_max_task_count.
    '''
    batch_interval_ms: int = _empty # type: ignore
    '''
    The maximum waiting time (in milliseconds) for batching.
    NOTE: no effect if batch_size <= 1.
    '''
    worker_max_concurrent_batches: int|None = _empty    # type: ignore
    '''How many batches can be processed concurrently by a single worker. If None, no limit.'''
    batch_handle_timeout_secs: float|None = _empty  # type: ignore
    '''The timeout in seconds for handling a batch request. If None, no timeout.
    If not given, use handle_timeout_secs * batch_size'''
    
    @classmethod
    def TidyFieldName(cls, name: str) -> str|None:
        '''Get the actual field name from a simplified name.'''
        simple_name = _simplify_name(name)
        mapper = _endpoint_info_field_name_mapper()
        return mapper.get(simple_name, None)
    
    def update_from_config(self, service_config: "ServiceConfigs"):
        '''update the endpoint info based on the service config.'''
        def _get_default(attr_name: str, fallback):
            return getattr(service_config, f'default_{attr_name}', fallback)
        
        def _check_empty(attr_name: str, fallback):
            attr = getattr(self, attr_name, _empty)
            if attr is _empty:
                setattr(self, attr_name, _get_default(attr_name, fallback))
        
        _check_empty('max_task_count', None)
        if self.max_task_count is not None:
            default_worker_max_task_count = int(ceil(self.max_task_count / service_config.max_worker_count))
        else:
            default_worker_max_task_count = None
        _check_empty('worker_max_task_count', default_worker_max_task_count)
        _check_empty('handle_timeout_secs', 180)
        
        _check_empty('batch_size', 10)
        _check_empty('batch_interval_ms', 500)
        _check_empty('worker_max_concurrent_batches', None)
        if self.handle_timeout_secs is not None:
            default_batch_handle_timeout = self.handle_timeout_secs * self.batch_size
        else:
            default_batch_handle_timeout = None
        _check_empty('batch_handle_timeout_secs', default_batch_handle_timeout)
        
        if self.max_task_count is not None and self.batch_size > self.max_task_count:
            self.batch_size = self.max_task_count
        if self.worker_max_task_count is not None and self.batch_size > self.worker_max_task_count:
            self.batch_size = self.worker_max_task_count
    
    def update_from_kwargs(self, **extra_fields) -> None:
        tidied_fields = {}
        for k, v in extra_fields.items():
            if (tidied_k:=EndpointInfo.TidyFieldName(k)) is not None:
                tidied_fields[tidied_k] = v
        annos = _endpoint_info_field_annos()
        for k, v in tidied_fields.items():
            field_anno = annos[k]
            if not check_value_is(v, field_anno):
                if isinstance(v, str):
                    if serializer:=_endpoint_info_field_serializer(k):
                        try:
                            v = serializer(v)
                        except Exception as e:
                            _logger.warning(f"Cannot update EndpointInfo field '{k}': failed to deserialize value '{v}' to type `{field_anno}`. Skipped. Error: {e}")
                            continue
                    else:
                        _logger.warning(f"Cannot update EndpointInfo field '{k}': value '{v}' is not of type `{field_anno}`. Skipped.")
                        continue
                else:
                    _logger.warning(f"Cannot update EndpointInfo field '{k}': value '{v}' is not of type `{field_anno}`. Skipped.")
                    continue
            setattr(self, k, v)
    
_T = TypeVar('_T')
_R = TypeVar('_R')
_ContextFunc = TypeAliasType('_ContextFunc', Callable[['ServiceWorker', _T], _R]|Callable[['ServiceWorker', _T, 'ServeContext'], _R], type_params=(_T, _R)) 

ServiceHandler: TypeAlias = _ContextFunc[Message, Message]
ServiceAsyncHandler: TypeAlias = _ContextFunc[Message, Awaitable[Message]]
ServiceStreamInputHandler: TypeAlias = _ContextFunc[AsyncIterable['StreamMessage'], Awaitable['Message']]
ServiceStreamOutputHandler: TypeAlias = _ContextFunc[Message, AsyncIterable['StreamMessage']]
ServiceStreamHandler: TypeAlias = _ContextFunc[AsyncIterable['StreamMessage'], AsyncIterable['StreamMessage']]
ServiceHandlerType: TypeAlias = ServiceHandler | ServiceAsyncHandler | ServiceStreamInputHandler | ServiceStreamOutputHandler | ServiceStreamHandler

ServiceBatchHandler: TypeAlias = _ContextFunc[Sequence[Message], Sequence[Message]]
ServiceAsyncBatchHandler: TypeAlias = _ContextFunc[Sequence[Message], Awaitable[Sequence[Message]]]
ServiceStreamInputBatchHandler: TypeAlias = _ContextFunc[Sequence[AsyncIterable['StreamMessage']], Awaitable[Sequence['Message']]]
ServiceStreamOutputBatchHandler: TypeAlias = _ContextFunc[Sequence[Message], Awaitable[Sequence[AsyncIterable['StreamMessage']]]]
ServiceStreamBatchHandler: TypeAlias = _ContextFunc[AsyncIterable['StreamMessage'], Awaitable[Sequence[AsyncIterable['StreamMessage']]]]
ServiceBatchHandlerType: TypeAlias = ServiceBatchHandler | ServiceAsyncBatchHandler | ServiceStreamInputBatchHandler | ServiceStreamOutputBatchHandler | ServiceStreamBatchHandler

_F = TypeVar('_F', bound=ServiceHandlerType)
_BF = TypeVar('_BF', bound=ServiceBatchHandlerType)

class _RegisterCommonParams(TypedDict, total=False):
    name: str
    description: str
    input_type: type['Message']|type['StreamMessage']
    output_type: type['Message']|type['StreamMessage']
    max_task_count: int
    worker_max_task_count: int
    handle_timeout_secs: float
    
def _save_issubclass(cls, base):
    try:
        return issubclass(cls, base)
    except:
        return False

def _get_seq_inner_anno(anno):
    origin, args = get_origin(anno), get_args(anno)
    if not origin or not args:
        raise ValueError(f"Invalid batch input type annotation: {anno}")
    if origin in (list, Sequence):
        return args[0], list
    elif origin == tuple:
        if (len(args) == 2 and args[1] == ...) or (len(args) == 1):
            return args[0], tuple
    raise ValueError(f"Invalid batch input type annotation: {anno}")

def _partial_register(
    f,
    /,
    is_batch: bool=False,
    batch_size: int=8,
    batch_interval_ms: int=500,
    worker_max_concurrent_batches: int|None=None,
    **kwargs: Unpack[_RegisterCommonParams],
):
    name = kwargs.get('name', f.__name__)
    description = kwargs.get('description', inspect.getdoc(f)) or None
    input_type = kwargs.get('input_type', None)
    output_type = kwargs.get('output_type', None)
    is_stream_input, is_stream_output = False, False
    batch_input_type = None
    
    if not input_type or not output_type:
        if isinstance(f, staticmethod):
            sig = inspect.signature(f.__func__)
            inp_anno = list(sig.parameters.values())[0].annotation
        elif isinstance(f, classmethod):
            sig = inspect.signature(f.__func__)
            inp_anno = list(sig.parameters.values())[1].annotation
        else:   # instance method
            sig = inspect.signature(f)
            inp_anno = list(sig.parameters.values())[1].annotation
        
        if not input_type:
            if inp_anno == _empty:
                raise ValueError(f"Cannot infer input_type for endpoint '{name}'. Please specify it explicitly.")
            if is_batch:
                input_type, batch_input_type = _get_seq_inner_anno(inp_anno)
            else:
                input_type = inp_anno
            
        if not output_type:
            ret_anno = sig.return_annotation
            if ret_anno == _empty:
                raise ValueError(f"Cannot infer output_type for endpoint '{name}'. Please specify it explicitly.")
            if is_batch:
                ret_anno = _get_seq_inner_anno(ret_anno)
            else:
                output_type = ret_anno

    if input_type in (AsyncIterable, AsyncIterator, AsyncGenerator):
        raise TypeError(f'No inner type is specified for the streaming input of endpoint "{name}". Only got `{input_type}`.')
    if output_type in (AsyncIterable, AsyncIterator, AsyncGenerator):
        raise TypeError(f'No inner type is specified for the streaming output of endpoint "{name}". Only got `{output_type}`.')
    input_type_origin, input_type_args = get_origin(input_type), get_args(input_type)
    if _save_issubclass(input_type_origin, AsyncIterable):
        is_stream_input = True
        input_type = get_args(input_type)[0]    # no matter AsyncIterator or AsyncGenerator, first arg is the inner type
    output_type_origin, output_type_args = get_origin(output_type), get_args(output_type)
    if _save_issubclass(output_type_origin, AsyncIterable):
        is_stream_output = True
        output_type = get_args(output_type)[0]
        
    max_task_count = kwargs.get('max_task_count', None)
    worker_max_task_count = kwargs.get('worker_max_task_count', _empty) # if not given, will be calculated later

    endpoint_info = EndpointInfo(
        name=name,  # type: ignore
        description=description,    # type: ignore
        input_streaming=is_stream_input,
        output_streaming=is_stream_output,
        input_type=input_type,  # type: ignore
        output_type=output_type,    # type: ignore
        batch_input_type=batch_input_type,
        batching_enabled=is_batch,  # type: ignore
        batch_size=batch_size,
        batch_interval_ms=batch_interval_ms,
        worker_max_concurrent_batches=worker_max_concurrent_batches,
        max_task_count=max_task_count,
        worker_max_task_count=worker_max_task_count,    # type: ignore
    )
    return endpoint_info

@overload
def register(f: _F, /)->_F: ...
@overload
def register(**kwargs: Unpack[_RegisterCommonParams]) -> Callable[[_F], _F]: ...

def register(f=None, /, **kwargs): # type: ignore
    if f is not None:
        endpoint_info = _partial_register(f, is_batch=False, **kwargs)
        setattr(f, '_thinkserve_endpoint_info', endpoint_info)
        return f
    else:
        def decorator(func: _F) -> _F:
            endpoint_info = _partial_register(func, is_batch=False, **kwargs)
            setattr(func, '_thinkserve_endpoint_info', endpoint_info)
            return func
        return decorator

@overload
def register_batch(f: _BF, /)->_BF: ...
@overload
def register_batch(*,  \
    batch_size: int = 8,
    batch_interval_ms: int = 500,
    worker_max_concurrent_batches: int|None = None,
    **kwargs: Unpack[_RegisterCommonParams]
) -> Callable[[_BF], _BF]: ...

def register_batch(f= None, /, **kwargs): # type: ignore
    if f is not None:
        endpoint_info = _partial_register(
            f,
            is_batch=True,
            batch_size=kwargs.pop('batch_size', 8),
            batch_interval_ms=kwargs.pop('batch_interval_ms', 500),
            worker_max_concurrent_batches=kwargs.pop('worker_max_concurrent_batches', None),
            **kwargs,
        )
        setattr(f, '_thinkserve_endpoint_info', endpoint_info)
        return f
    else:
        def decorator(func: _BF) -> _BF:
            endpoint_info = _partial_register(
                func,
                is_batch=True,
                batch_size=kwargs.pop('batch_size', 8),
                batch_interval_ms=kwargs.pop('batch_interval_ms', 500),
                worker_max_concurrent_batches=kwargs.pop('worker_max_concurrent_batches', None),
                **kwargs,
            )
            setattr(func, '_thinkserve_endpoint_info', endpoint_info)
            return func
        return decorator
    
    
__all__ = [
    'EndpointInfo',
    
    'ServiceHandler',
    'ServiceAsyncHandler',
    'ServiceStreamInputHandler',
    'ServiceStreamOutputHandler',
    'ServiceStreamHandler',
    'ServiceHandlerType',
    
    'ServiceBatchHandler',
    'ServiceAsyncBatchHandler',
    'ServiceStreamInputBatchHandler',
    'ServiceStreamOutputBatchHandler',
    'ServiceStreamBatchHandler',
    'ServiceBatchHandlerType',
    
    'register',
    'register_batch',
]