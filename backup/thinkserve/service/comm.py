import os

if __name__.endswith('main__'): # for running this file for testing directly
    import sys
    _proj_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    sys.path.insert(0, _proj_root)
    __package__ = 'thinkserve.service'

import re
import copy
import time
import uuid
import socket
import orjson
import struct
import pickle
import inspect
import asyncio
import logging
import requests
import tempfile

if __name__.endswith('main__'): # for debugging
    logging.basicConfig(level=5, format='(%(process)d)|%(name)s|[%(levelname)s] %(asctime)s: %(message)s')

from pydantic import BaseModel
from threading import Thread
from abc import ABC, abstractmethod
from functools import partial, cache
from dataclasses import dataclass, field
from queue import Queue, Empty as QueueEmptyError
from typing_extensions import TypeAliasType, Unpack, overload, override
from typing import (TypedDict, Awaitable, Any, Callable, ParamSpec, TypeVar, get_origin, Coroutine,
                    AsyncGenerator, AsyncIterator, TYPE_CHECKING, get_args, Sequence, Annotated, Generator, 
                    Protocol)
from base64 import b64encode, b64decode

from ..common_utils.type_utils import (SerializableType, check_value_is, check_type_is, get_type_from_str, 
                                       serialize, is_serializable)
from ..common_utils.concurrent_utils import get_async_generator, run_any_func
from ..common_utils.debug_utils import get_logger, Logger

_P = ParamSpec('_P')
_T = TypeVar('_T')
_SyncOrAsyncFunc = TypeAliasType('_SyncOrAsyncFunc', Callable[_P, Awaitable[_T]]|Callable[_P, _T], type_params=(_P, _T))
if __name__.endswith('main__'):
    _logger = get_logger(__file__.rpartition(os.sep)[-1])
else:
    _logger = get_logger(__name__)

# region dataclasses for socket communication
if TYPE_CHECKING:
    _socket_dt_cls = dataclass
else:
    def _socket_dt_cls(cls):
        cls.__dt_index__ = len(SocketBaseData.__dt_matching__)
        SocketBaseData.__dt_matching__[len(SocketBaseData.__dt_matching__)] = cls
        
        origin_dump = cls.dump
        def dump(self):
            data: bytes = origin_dump(self)
            dt_index = struct.pack('I', cls.__dt_index__)
            return dt_index + data
        cls.dump = dump
        return dataclass(cls)

def _random_uuid() -> str:
    return str(uuid.uuid4()).replace('-', '')

class SocketBaseData(ABC):
    
    __dt_index__: int
    __dt_matching__: dict[int, type["SocketBaseData"]] = {}
    
    @abstractmethod
    def dump(self) -> bytes: ...
    
    @abstractmethod
    async def on_received(self, client: "EventCommunicationBase", from_client_id: str): ...
    
    @classmethod
    async def Parse(cls, raw: bytes, client: "EventCommunicationBase")->"SocketBaseData":
        dt_index = struct.unpack('I', raw[0:4])[0]
        if (dt_cls:=cls._FindDataType(dt_index)) is None:
            raise ValueError(f'No Socket Data Class found for index: {dt_index}')
        return await dt_cls.Parse(raw[4:], client)
    
    async def send(self, client: "EventCommunicationBase", to_client: str):
        try:
            await client.send(self.dump(), to_client)
        except Exception as e:
            _logger.error(f'Failed to send data to client `{to_client}`. {type(e).__name__}: {e}')
        
    def copy(self):
        return copy.copy(self)
    
    @staticmethod
    async def GetDataFromPipe(
        pipe: Queue, 
        timeout:float|None=None, 
        on_finished: Callable[[], Any]|None=None
    )->"SocketPipeData|AsyncGenerator[SocketPipeData, None]":
        async def get_from_queue(q: Queue, timeout: float|None=None):
            t, INTERVAL = 0.0, 0.05
            while True:
                try:
                    item = q.get_nowait()
                    yield item
                except QueueEmptyError:
                    await asyncio.sleep(INTERVAL)
                    if timeout is not None:
                        t += INTERVAL
                        if t >= timeout:
                            raise TimeoutError('Timeout waiting for data from pipe.')
                    
        gen = get_from_queue(pipe, timeout=timeout)
        first_item: "SocketPipeData" = await gen.__anext__()
        
        is_stream_key = getattr(first_item, '__is_stream_key__', None)
        stream_end_key = getattr(first_item, '__stream_end_key__', None)
        
        if (is_stream_key is None or stream_end_key is None):
            if on_finished is not None:
                on_finished()
            return first_item
        if getattr(first_item, is_stream_key, False):
            async def stream_generator():
                yield first_item
                async for item in gen:
                    if getattr(item, stream_end_key, False):
                        break   # end of stream, no need to yield this item
                    yield item
                if on_finished is not None:
                    on_finished()
            return stream_generator()
        else:
            if on_finished is not None:
                on_finished()
            return first_item
        
    @staticmethod
    @cache
    def _FindDataType(key: str|int):
        if isinstance(key, int):
            if (dt:=SocketBaseData.__dt_matching__.get(key, None)) is not None:
                return dt
        else:
            for cls in SocketBaseData.__subclasses__():
                if cls.__class__.__name__ == key:
                    return cls
        return None

class SocketPipeData(SocketBaseData):
    __is_stream_key__: str|None = None
    __stream_end_key__: str|None = None
    
    def __init_subclass__(cls, is_stream_key: str|None=None, stream_end_key: str|None=None):
        cls.__is_stream_key__ = is_stream_key
        cls.__stream_end_key__ = stream_end_key
    
    @property
    @abstractmethod
    def pipe_key(self)->str: ...
    
    @override
    async def on_received(self, client: "EventCommunicationBase", from_client_id: str):
        pipe_id = self.pipe_key
        pipes = client._pipes
        if (q:=pipes.get(pipe_id, None)) is None:
            pipes[pipe_id] = q = Queue()
        q.put(self)

def _dump_val(val):
    if isinstance(val, (set, frozenset, tuple)):
        return orjson.dumps(list(val))
    elif isinstance(val, bytes):
        return b64encode(val)
    elif isinstance(val, bytearray):
        return b64encode(bytes(val))
    elif isinstance(val, BaseModel):
        val = val.model_dump()
    return orjson.dumps(val)

@_socket_dt_cls
class EventData(SocketBaseData):
    '''dataclass for 1 event triggering'''
    id: str             # 32 uuid
    event: str          # <= 128 bytes(ascii)
    data: dict[str, SerializableType|AsyncIterator[SerializableType]]   # all fields for invoking the event

    @override
    def dump(self)->bytes:
        # struct:
        # [32 bytes id]
        # [128 bytes event name]
        # [4 bytes field count]
        # [for each field: 128 bytes field name] (no data, data is sent separately via pipes)
        
        buf = bytearray()
        id_bytes = self.id.encode('utf-8')
        id_bytes += b'\0' * (32 - len(id_bytes))
        buf += id_bytes
        event_bytes = self.event.encode('utf-8')
        event_bytes += b'\0' * (128 - len(event_bytes))
        buf += event_bytes
        # use 128 bytes for each field name
        field_count = len(self.data)
        buf += struct.pack('I', field_count)
        for field_name in self.data.keys():
            field_name_bytes = field_name.encode('utf-8')
            field_name_bytes += b'\0' * (128 - len(field_name_bytes))
            buf += field_name_bytes
        return bytes(buf)
    
    @classmethod
    @override
    async def Parse(cls, raw: bytes, client: "EventCommunicationBase"):
        id_bytes = raw[0:32]
        id_str = id_bytes.split(b'\0', 1)[0].decode('utf-8')
        event_bytes = raw[32:160]
        event_str = event_bytes.split(b'\0', 1)[0].decode('utf-8')
        field_count = struct.unpack('I', raw[160:164])[0]
        offset = 164
        
        async def wrap_get_from_pipe(field_name, pipe, on_finish)->tuple[str, SerializableType|AsyncGenerator[SerializableType, None]]:
            r: EventFieldData|AsyncGenerator[EventFieldData, None] = await cls.GetDataFromPipe(pipe, on_finished=on_finish)  # type: ignore
            if isinstance(r, AsyncGenerator):
                async def gen_wrapper(gen: AsyncGenerator[EventFieldData, None]):
                    async for item in gen:
                        if item.is_error:
                            raise EventInvokeError(item.data.decode('utf-8'))
                        yield orjson.loads(item.data)
                r = gen_wrapper(r)
            else:
                if r.is_error:
                    raise EventInvokeError(r.data.decode('utf-8'))
                r = orjson.loads(r.data)
            return field_name, r    # type: ignore
        
        coros = []
        pipes = client._pipes
        for _ in range(field_count):
            field_name_bytes = raw[offset:offset+128]
            field_name_str = field_name_bytes.split(b'\0', 1)[0].decode('utf-8')
            offset += 128
            pipe_key = cls.BuildPipeKey(id_str, event_str, field_name_str)
            if (pipe:=pipes.get(pipe_key, None)) is None:
                pipe = pipes[pipe_key] = Queue()
            coros.append(wrap_get_from_pipe(field_name_str, pipe, lambda: pipes.pop(pipe_key, None)))
        
        results: list[tuple[str, Any]] = await asyncio.gather(*coros)
        data = {k:v for k,v in results}
        return cls(
            id=id_str,
            event=event_str,
            data=data,
        )
        
    @staticmethod
    def BuildPipeKey(id: str, event: str, field: str) -> str:
        return f'{id}:{event}:{field}'
    
    @override
    async def send(self, client: "EventCommunicationBase", to_client: str):
        coros = []
        async def send_field(field_name:str, value):
            if isinstance(value, Generator):
                value = get_async_generator(value)
            if isinstance(value, AsyncIterator):
                async for item in value:
                    await EventFieldData(
                        id=self.id,
                        event=self.event,
                        field=field_name,
                        is_stream=True,
                        is_stream_end=False,
                        is_error=False,
                        data=_dump_val(item),
                    ).send(client, to_client)
                # to indicate stream end
                await EventFieldData(
                    id=self.id,
                    event=self.event,
                    field=field_name,
                    is_stream=True,
                    is_stream_end=True,
                    is_error=False,
                    data=b'',
                ).send(client, to_client)
            else:
                await EventFieldData(
                    id=self.id,
                    event=self.event,
                    field=field_name,
                    is_stream=False,
                    is_stream_end=False,
                    is_error=False,
                    data=_dump_val(value)
                ).send(client, to_client)
        
        for k, v in self.data.items():
            coros.append(send_field(k, v))
        coros.append(super().send(client, to_client))
        await asyncio.gather(*coros)

    @override
    async def on_received(self, client: "EventCommunicationBase", from_client_id: str):
        if not (event:=client._events.get(self.event, None)):
            _logger.warning(f'Received unknown event: {self.event} from client `{from_client_id}`')
            return
        # trigger event, and send result back
        try:
            params = event.pack_params(self.data)
            r = await event.invoke(*params.args, **params.kwargs)
        except BaseException as e:
            data = f'Error during event `{self.event}` handling. {type(e).__name__}: {e}.'
            data = data.encode('utf-8')
            await EventFieldData(
                id=self.id,
                event=self.event,
                field='__return__', # special field name for return value
                is_stream=False,
                is_stream_end=False,
                is_error=True,
                data=data,
            ).send(client, from_client_id)
        else:
            if isinstance(r, Generator):
                r = get_async_generator(r)
            if isinstance(r, AsyncIterator):
                while True:
                    try:
                        item = await r.__anext__()
                    except StopAsyncIteration:
                        break
                    except BaseException as e:
                        data = f'Error during event `{self.event}` handling. {type(e).__name__}: {e}.'
                        data = data.encode('utf-8')
                        await EventFieldData(
                            id=self.id,
                            event=self.event,
                            field='__return__', # special field name for return value
                            is_stream=False,
                            is_stream_end=False,
                            is_error=True,
                            data=data,
                        ).send(client, from_client_id)
                        break
                    else:
                        await EventFieldData(
                            id=self.id,
                            event=self.event,
                            field='__return__', # special field name for return value
                            is_stream=True,
                            is_stream_end=False,
                            is_error=False,
                            data=_dump_val(item)
                        ).send(client, from_client_id)
                # indicate stream end
                await EventFieldData(
                    id=self.id,
                    event=self.event,
                    field='__return__',
                    is_stream=True,
                    is_stream_end=True,
                    is_error=False,
                    data=b'',
                ).send(client, from_client_id)
                
            else:
                await EventFieldData(
                    id=self.id,
                    event=self.event,
                    field='__return__',
                    is_stream=False,
                    is_stream_end=False,
                    is_error=False,
                    data=_dump_val(r)
                ).send(client, from_client_id)

@_socket_dt_cls
class EventFieldData(SocketPipeData, is_stream_key='is_stream', stream_end_key='is_stream_end'):
    '''
    dataclass for 1 field of an event.
    NOTE: you cannot use `__return__` as field name, as it is reserved for return value.
    '''
    id: str     # 32 uuid
    event: str  # [128 bytes event name]
    field: str  # [128 bytes field name], for return case, it will be `__return__`
    
    # flags
    is_stream: bool
    is_stream_end: bool
    is_error: bool
    '''whether this field data indicates an error. If True, the `data` field contains the error message.'''
    
    data: bytes
    
    def __post_init__(self):
        if isinstance(self.data, bytearray):
            self.data = bytes(self.data)
    
    @override
    def dump(self) -> bytes:
        buf = bytearray()
        id_bytes = self.id.encode('utf-8')
        id_bytes += b'\0' * (32 - len(id_bytes))
        buf += id_bytes
        event_bytes = self.event.encode('utf-8')
        event_bytes += b'\0' * (128 - len(event_bytes))
        buf += event_bytes
        field_bytes = self.field.encode('utf-8')
        field_bytes += b'\0' * (128 - len(field_bytes))
        buf += field_bytes
        
        flags = 0
        if self.is_stream:
            flags |= 0x01
        if self.is_stream_end:
            flags |= 0x02
        if self.is_error:
            flags |= 0x04
        buf += struct.pack('B', flags)
        buf += self.data
        return bytes(buf)
    
    @classmethod
    @override
    async def Parse(cls, raw: bytes, client: "EventCommunicationBase"):
        id_bytes = raw[0:32]
        id_str = id_bytes.split(b'\0', 1)[0].decode('utf-8')
        event_bytes = raw[32:160]
        event_str = event_bytes.split(b'\0', 1)[0].decode('utf-8')
        field_bytes = raw[160:288]
        field_str = field_bytes.split(b'\0', 1)[0].decode('utf-8')
        
        flags = struct.unpack('B', raw[288:289])[0]
        is_stream = (flags & 0x01) != 0
        is_stream_end = (flags & 0x02) != 0
        is_error = (flags & 0x04) != 0
        data = raw[289:]
        return cls(
            id=id_str,
            event=event_str,
            field=field_str,
            is_stream=is_stream,
            is_stream_end=is_stream_end,
            is_error=is_error,
            data=data,
        )
    
    @property
    @override
    def pipe_key(self) -> str:
        return EventData.BuildPipeKey(self.id, self.event, self.field)

@_socket_dt_cls
class KeepAlive(SocketBaseData):
    '''dataclass for keep-alive message. Keep-alive messages are sent 
    from client to server periodically.'''
    
    timestamp: int = field(default_factory=lambda: int(time.time() * 1000))
    '''epoch time when the keep-alive message is sent.
    13 digits, in milliseconds.'''
    is_response: bool = False
    '''When server receives a keep-alive message, it will send back a 
    response with this field set to True.'''
    
    @classmethod
    @override
    async def Parse(cls, raw: bytes, client: "EventCommunicationBase"):
        timestamp = struct.unpack('Q', raw[0:8])[0]
        is_response = struct.unpack('B', raw[8:9])[0] == 1
        return cls(
            timestamp=timestamp,
            is_response=is_response,
        )
    
    @override
    def dump(self) -> bytes:
        buf = bytearray()
        buf += struct.pack('Q', self.timestamp)
        buf += struct.pack('B', 1 if self.is_response else 0)
        return bytes(buf)
    
    @override
    async def on_received(self, client: "EventCommunicationBase", from_client_id: str):
        _logger.debug(f'Received keep-alive from `{from_client_id}`, is_response={self.is_response}')
        if not (from_info := client.get_peer_info(from_client_id)):
            return
        from_info.last_alive_time = self.timestamp
        
        if not self.is_response:    # from client to server
            # send back response
            response = KeepAlive(is_response=True,)
            await response.send(client, from_info.id)
    
@_socket_dt_cls
class HandShake(SocketBaseData):
    '''the first message sent from client to server to establish connection.'''
    
    pipe_id: str
    '''id for later pipe communication.'''
    
    # info
    name: str
    '''custom name of the peer client/server, for identification(can be duplicate).'''
    description: str|None = None
    '''description of the peer client/server.'''
    host: str = 'localhost'
    '''IP address or hostname of the peer client/server.'''
    port: int|None = None
    '''port number of the peer (only when peer is a server and binding to a port).'''
    
    auth: str|None = None
    '''authentication token, if any.'''
    
    @classmethod
    @override
    async def Parse(cls, raw: bytes, client: "EventCommunicationBase"):
        offset = 0
        pipe_id_len = struct.unpack('I', raw[offset:offset+4])[0]
        offset += 4
        pipe_id_bytes = raw[offset:offset+pipe_id_len]
        pipe_id_str = pipe_id_bytes.decode('utf-8')
        offset += pipe_id_len
        # name
        name_len = struct.unpack('I', raw[offset:offset+4])[0]
        offset += 4
        name_bytes = raw[offset:offset+name_len]
        name_str = name_bytes.decode('utf-8')
        offset += name_len
        # description
        desc_len = struct.unpack('I', raw[offset:offset+4])[0]
        offset += 4
        if desc_len > 0:
            desc_bytes = raw[offset:offset+desc_len]
            desc_str = desc_bytes.decode('utf-8')
        else:
            desc_str = None
        offset += desc_len
        # host
        host_len = struct.unpack('I', raw[offset:offset+4])[0]
        offset += 4
        host_bytes = raw[offset:offset+host_len]
        host_str = host_bytes.decode('utf-8')
        offset += host_len
        # port
        port = struct.unpack('I', raw[offset:offset+4])[0]
        if port == 0:
            port = None
        offset += 4
        # auth
        auth_len = struct.unpack('I', raw[offset:offset+4])[0]
        offset += 4
        if auth_len > 0:
            auth_bytes = raw[offset:offset+auth_len]
            auth_str = auth_bytes.decode('utf-8')
        else:
            auth_str = None
        return cls(
            pipe_id=pipe_id_str,
            name=name_str,
            description=desc_str,
            host=host_str,
            port=port,
            auth=auth_str,
        )
    
    @override
    def dump(self) -> bytes:
        buf = bytearray()
        # pipe_id
        pipe_id_bytes = self.pipe_id.encode('utf-8')
        pipe_id_len = len(pipe_id_bytes)
        buf += struct.pack('I', pipe_id_len)
        buf += pipe_id_bytes
        # name
        name_bytes = self.name.encode('utf-8')
        name_len = len(name_bytes)
        buf += struct.pack('I', name_len)
        buf += name_bytes
        # description
        if self.description is not None:
            desc_bytes = self.description.encode('utf-8')
            desc_len = len(desc_bytes)
        else:
            desc_bytes = b''
            desc_len = 0
        buf += struct.pack('I', desc_len)
        buf += desc_bytes
        # host
        host_bytes = self.host.encode('utf-8')
        host_len = len(host_bytes)
        buf += struct.pack('I', host_len)
        buf += host_bytes
        # port
        if self.port is not None:
            buf += struct.pack('I', self.port)
        else:
            buf += struct.pack('I', 0)
        # auth
        if self.auth is not None:
            auth_bytes = self.auth.encode('utf-8')
            auth_len = len(auth_bytes)
        else:
            auth_bytes = b''
            auth_len = 0
        buf += struct.pack('I', auth_len)
        buf += auth_bytes
        return bytes(buf)
    
    @override
    async def on_received(self, client: "EventCommunicationBase", from_client_id: str):
        if client.is_server:
            server: "EventServerBase" = client  # type: ignore 
            # only server need to handle `HandShake` message
            succ = True
            if (server_auth := getattr(server, '_auth', None)):
                if server_auth != self.auth:
                    _logger.warning(f'Received handshake from client `{from_client_id}` with incorrect auth.')
                    succ = False
            if not succ:
                r = HandShakeResult(
                    pipe_id=self.pipe_id,
                    name=None,
                    description=None,
                    host=None,
                    port=None,
                    success=False,
                    fail_reason='Authentication failed.',
                )
            else:
                r = HandShakeResult(
                    pipe_id=self.pipe_id,
                    name=server.name,
                    description=server.description,
                    host=server.host,
                    port=getattr(server, 'port', None),
                    success=True,
                )
            server._client_validation_results[from_client_id] = (self, r.fail_reason, succ)
            await r.send(server, from_client_id)
                
@_socket_dt_cls
class HandShakeResult(SocketPipeData, is_stream_key=None, stream_end_key=None):
    
    pipe_id: str
    '''id for pipe communication.'''
    
    name: str|None
    '''the name of the response server. None if handshake failed.'''
    description: str|None = None
    '''description of the peer client/server. None if handshake failed.'''
    host: str|None = None
    '''IP address or hostname of the peer server. None if handshake failed.'''
    port: int|None = None
    '''port number of the peer (only when peer is a server and binding to a port). None if handshake failed.'''
    
    success: bool = False
    '''whether the handshake is successful.'''
    fail_reason: str|None = None
    '''reason for failure, if any.'''
    
    def __post_init__(self):
        self.name = self.name or None
        self.description = self.description or None
        self.host = self.host or None
        self.fail_reason = self.fail_reason or None

    @property
    @override
    def pipe_key(self) -> str:
        return self.pipe_id

    @override
    def dump(self) -> bytes:
        buf = bytearray()
        # pipe_id
        pipe_id_bytes = self.pipe_id.encode('utf-8')
        pipe_id_len = len(pipe_id_bytes)
        buf += struct.pack('I', pipe_id_len)
        buf += pipe_id_bytes
        # name
        if self.name is not None:
            name_bytes = self.name.encode('utf-8')
            name_len = len(name_bytes)
        else:
            name_bytes = b''
            name_len = 0
        buf += struct.pack('I', name_len)
        buf += name_bytes
        # description
        if self.description is not None:
            desc_bytes = self.description.encode('utf-8')
            desc_len = len(desc_bytes)
        else:
            desc_bytes = b''
            desc_len = 0
        buf += struct.pack('I', desc_len)
        buf += desc_bytes
        # host
        if self.host is not None:
            host_bytes = self.host.encode('utf-8')
            host_len = len(host_bytes)
        else:
            host_bytes = b''
            host_len = 0
        buf += struct.pack('I', host_len)
        buf += host_bytes
        # port
        if self.port is not None:
            buf += struct.pack('I', self.port)
        else:
            buf += struct.pack('I', 0)
        # success
        buf += struct.pack('B', 1 if self.success else 0)
        # fail_reason
        if self.fail_reason is not None:
            reason_bytes = self.fail_reason.encode('utf-8')
            reason_len = len(reason_bytes)
        else:
            reason_bytes = b''
            reason_len = 0
        buf += struct.pack('I', reason_len)
        buf += reason_bytes
        return bytes(buf)
    
    @classmethod
    @override
    async def Parse(cls, raw: bytes, client: "EventCommunicationBase"):
        offset = 0
        pipe_id_len = struct.unpack('I', raw[offset:offset+4])[0]
        offset += 4
        pipe_id_bytes = raw[offset:offset+pipe_id_len]
        pipe_id_str = pipe_id_bytes.decode('utf-8')
        offset += pipe_id_len
        # name
        name_len = struct.unpack('I', raw[offset:offset+4])[0]
        offset += 4
        if name_len > 0:
            name_bytes = raw[offset:offset+name_len]
            name_str = name_bytes.decode('utf-8')
        else:
            name_str = None
        offset += name_len
        # description
        desc_len = struct.unpack('I', raw[offset:offset+4])[0]
        offset += 4
        if desc_len > 0:
            desc_bytes = raw[offset:offset+desc_len]
            desc_str = desc_bytes.decode('utf-8')
        else:
            desc_str = None
        offset += desc_len
        # host
        host_len = struct.unpack('I', raw[offset:offset+4])[0]
        offset += 4
        if host_len > 0:
            host_bytes = raw[offset:offset+host_len]
            host_str = host_bytes.decode('utf-8')
        else:
            host_str = None
        offset += host_len
        # port
        port = struct.unpack('I', raw[offset:offset+4])[0]
        if port == 0:
            port = None
        offset += 4
        # success
        success = struct.unpack('B', raw[offset:offset+1])[0] == 1
        offset += 1
        # fail_reason
        reason_len = struct.unpack('I', raw[offset:offset+4])[0]
        offset += 4
        if reason_len > 0:
            reason_bytes = raw[offset:offset+reason_len]
            reason_str = reason_bytes.decode('utf-8')
        else:
            reason_str = None
        return cls(
            pipe_id=pipe_id_str,
            name=name_str,
            description=desc_str,
            host=host_str,
            port=port,
            success=success,
            fail_reason=reason_str,
        )


__all__ = [
    'SocketBaseData',
    'SocketPipeData',
    'EventData',
    'EventFieldData',
    'KeepAlive',
    'HandShake',
]
# endregion

def _check_port_available(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) != 0        

def find_available_port(start_port: int=30000, end_port: int=65536) -> int:
    for port in range(start_port, end_port):
        if _check_port_available(port):
            return port
    raise RuntimeError('No available port found.')

_ipv4_pattern = re.compile(r"(?:(?:[0-9]{1,3}\.){3}[0-9]{1,3})")
_ipv6_pattern = re.compile(r"((([0-9a-fA-F]{1,4}:){7}([0-9a-fA-F]{1,4}|:))|(([0-9a-fA-F]{1,4}:){1,7}:)|(([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4})|(([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2})|(([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3})|(([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4})|(([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5})|([0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6}))|(:((:[0-9a-fA-F]{1,4}){1,7}|:)))(%.+)?")
_KEEP_ALIVE_INTERVAL_SECONDS = 15

@cache
def _get_local_ip() -> str:
    hostname = socket.gethostname()
    local_ip = socket.gethostbyname(hostname)
    return local_ip

@cache
def _get_global_IP()->str|None:
    ip = None
    try:
        r = requests.get("https://api.ipify.org")
        ip = r.text.strip()
        if m:=re.search(_ipv4_pattern, ip):
            ip = m.group(0)
        elif m:=re.search(_ipv6_pattern, ip):
            ip = m.group(0)
        else:
            ip = None
        
    except requests.exceptions.ConnectionError: 
        # e.g. mainland China cannot access api.ipify.org
        r = requests.get("http://myip.ipip.net")
        if m:=re.search(_ipv4_pattern, r.text):
            ip = m.group(0)
        elif m:=re.search(_ipv6_pattern, r.text):
            ip = m.group(0)
        else:
            ip = None
    return ip

class PeerInfo(BaseModel):
    id: str
    '''a unique id created automatically for each peer client/server.
    This id is for one-side's internal use only, and will not be sent over the network.'''
    name: str
    '''custom name of the peer client/server, for identification(can be duplicate).'''
    description: str|None = None
    '''description of the peer client/server.'''
    host: str
    '''IP address or hostname of the peer client/server.'''
    port: int|None = None
    '''port number of the peer (only when peer is a server and binding to a port).'''
    last_alive_time: int = field(default_factory=lambda: int(time.time() * 1000))
    '''epoch time when the last keep-alive message is received.'''

    @property
    def alive(self)->bool:
        secs = (time.time() * 1000 - self.last_alive_time) / 1000.0
        return secs < _KEEP_ALIVE_INTERVAL_SECONDS * 2

class _ServerInitCommonParams(TypedDict, total=False):
    name: str
    '''A custom unique name for identifying the server instance.
    If not given, a random UUID string will be used.'''
    description: str
    '''Description of the server instance.'''
    host: str
    '''Host to bind/connect. Default is 'localhost'.'''
    auth: str|None
    '''authentication token for clients to connect.'''
    auth_timeout: float
    '''Timeout in seconds for authentication. Default is 5.0 seconds.'''
    chunk_size: int
    '''Size of each chunk to read/write. Default is 1 MB.'''
    on_received: _SyncOrAsyncFunc[[bytes, PeerInfo], Any]
    '''Callback function when a complete message is received.'''
    on_connected: _SyncOrAsyncFunc[[PeerInfo], Any]
    '''Callback function when a connection is established.'''
    on_disconnected: _SyncOrAsyncFunc[[PeerInfo], Any]
    '''Callback function when a connection is lost.'''
    
class _ClientInitCommonParams(_ServerInitCommonParams, total=False):
    retry_connection_interval: float
    '''Interval in seconds between retrying connection when connection is lost.
    Default is 6.0 seconds.'''
    
class ConnectionLostError(ConnectionError): ...
class ConnectionTimeoutError(ConnectionError): ...
class EventInvokeError(Exception): ...

__all__.extend([
    'find_available_port',
    'PeerInfo',
    'ConnectionLostError',
    'ConnectionTimeoutError',
    'EventInvokeError',
])

_DEFAULT_CHUNK_SIZE: int = 1 * 1024 * 1024  # 1 MB
_DEFAULT_COMM_TIMEOUT: float = 5 * 60.0  # 5 minutes

_F = TypeAliasType('_F', _SyncOrAsyncFunc[..., SerializableType|AsyncIterator[SerializableType]])
_RT = TypeVar('_RT', bound=SerializableType|AsyncIterator[SerializableType])

def _is_async_callable(func):
    if asyncio.iscoroutinefunction(func):
        return True
    if hasattr(func, '__call__'):
        return asyncio.iscoroutinefunction(func.__call__)
    return_anno = inspect.signature(func).return_annotation
    if get_origin(return_anno) in (Coroutine, Awaitable) or (return_anno in (Coroutine, Awaitable)):
        return True
    return False

def _get_origin_type(t):
    if isinstance(t, str):
        t = get_type_from_str(t)
    if not isinstance(t, str):
        o = get_origin(t) or t
        if o is Annotated:
            if (args := get_args(t)):
                return _get_origin_type(args[0])
        return o
    return t

def _convert_val(val, target_type):
    target_origin = _get_origin_type(target_type)
    if target_origin in (Any, Ellipsis, SerializableType, object):
        return val  # no conversion needed
    target_args = get_args(target_type)
    val_type = type(val)
    if not check_value_is(val, target_type):
        if target_origin in (list, set, frozenset, Sequence):
            if target_args:
                if check_value_is(val, target_args[0]):
                    # T -> [T,...]
                    to_origin = list if target_origin is Sequence else target_origin
                    return to_origin([val]) # type: ignore
                elif check_type_is(target_args[0], BaseModel) and isinstance(val, dict):
                    to_origin = list if target_origin is Sequence else target_origin
                    return to_origin([_get_origin_type(target_args[0]).model_validate(val)]) # type: ignore 
                elif check_type_is(val_type, Sequence) and not isinstance(val, (str, bytes, bytearray)):
                    tidied = [_convert_val(v, target_args[0]) for v in val]  # type: ignore
                    to_origin = list if target_origin is Sequence else target_origin
                    return to_origin(tidied) # type: ignore
            else:
                if check_type_is(val_type, Sequence) and not isinstance(val, (str, bytes, bytearray)):
                    to_origin = list if target_origin is Sequence else target_origin
                    return to_origin(val) # type: ignore
        elif target_origin is tuple:
            if target_args:
                if len(target_args) == 2 and target_args[1] is Ellipsis:
                    # Tuple[T, ...]
                    if check_type_is(val_type, Sequence) and not isinstance(val, (str, bytes, bytearray)):
                        tidied = [_convert_val(v, target_args[0]) for v in val]  # type: ignore
                        return tuple(tidied) # type: ignore
                    elif check_type_is(val_type, target_args[0]):
                        return (val,) # type: ignore
                elif check_type_is(val_type, Sequence) and not isinstance(val, (str, bytes, bytearray)):
                    if len(val) == len(target_args):
                        tidied = [_convert_val(v, t) for v, t in zip(val, target_args)]  # type: ignore
                        return tuple(tidied) # type: ignore
            else:
                if check_type_is(val_type, Sequence) and not isinstance(val, (str, bytes, bytearray)):
                    return tuple(val) # type: ignore
        elif target_origin is int:
            if isinstance(val, float):
                return int(val)
        elif target_origin is float:
            if isinstance(val, int):
                return float(val)
        elif target_origin is bytes:
            if isinstance(val, str):
                if len(val) % 4 == 0:
                    try:
                        return b64decode(val)
                    except:
                        pass
                return val.encode('utf-8')
            elif isinstance(val, bytearray):
                return bytes(val)
            elif is_serializable(val):
                return serialize(val).encode('utf-8')
            else:
                return pickle.dumps(val)
        elif target_origin is str:
            if isinstance(val, bytes):
                try:
                    return val.decode('utf-8')
                except UnicodeDecodeError:    
                    return str(b64encode(val), 'utf-8')
            elif isinstance(val, bytearray):
                try:
                    return bytes(val).decode('utf-8')
                except UnicodeDecodeError:
                    return str(b64encode(bytes(val)), 'utf-8')
            elif is_serializable(val):
                return serialize(val)
            else:
                return str(val)
        elif check_type_is(target_origin, BaseModel):
            if isinstance(val, dict):
                return target_origin.model_validate(val)  # type: ignore
            elif isinstance(val, (str, bytes)):
                try:
                    return target_origin.model_validate_json(val)  # type: ignore
                except:
                    pass  # not raising, still pass to the end
    return val

@dataclass
class EventHandlerInfo:
    func: _SyncOrAsyncFunc[..., SerializableType|AsyncIterator[SerializableType]]
    is_async: bool = False
    
    # will be initialized in __post_init__
    func_sig: inspect.Signature = None  # type: ignore
    func_params: dict[str, inspect.Parameter] = None  # type: ignore
    func_return_type: Any = None  # type: ignore
    
    def __post_init__(self):
        self.func_sig = inspect.signature(self.func)
        self.func_params = self.func_sig.parameters # type: ignore
        self.func_return_type = self.func_sig.return_annotation
    
    def pack_params(self, params: dict[str, Any])->inspect.BoundArguments:
        '''
        Pack and validate parameters for invoking the event handler.
        Raises `TypeError` if parameters do not match.
        '''
        bound = self.func_sig.bind(**params)
        for k, p in self.func_params.items():
            if p.annotation not in (inspect.Parameter.empty, Any):
                if p.kind == inspect.Parameter.VAR_POSITIONAL:
                    bound.arguments[k] = tuple(_convert_val(v, p.annotation) for v in bound.arguments[k])
                elif p.kind == inspect.Parameter.VAR_KEYWORD:
                    for vk in tuple(bound.arguments[k]):
                        bound.arguments[k][vk] = _convert_val(bound.arguments[k][vk], p.annotation)
                else:
                    bound.arguments[k] = _convert_val(bound.arguments[k], p.annotation)
        return bound
        
    async def invoke(self, params: dict[str, Any]):
        '''Invoke the event handler with the given parameters.
        NOTE: you should call `pack_params` first to ensure parameters are valid.'''
        if self.is_async:
            r = self.func(**params)
        else:
            loop = asyncio.get_running_loop()
            r = loop.run_in_executor(None, partial(self.func, **params))
        if isinstance(r, Awaitable):
            r = await r
        return r    # NOTE: can be async generator

class _ExpireDict(dict[str, Any]):
    '''A special dictionary that automatically deletes items after a certain expiration time,
    for preventing memory leaks in long-running services.'''
    
    _timestamps: dict[str, float]
    _expire_time: float = 10 * 60.0  # default 10 minutes
    _stop: bool = False
    
    def __new__(cls, *args, expire_time: float=_expire_time, **kwargs):
        obj = super().__new__(cls)
        obj._expire_time = expire_time
        obj._timestamps = {}
        return obj
    
    def __setitem__(self, key: str, value: Any) -> None:
        super().__setitem__(key, value)
        self._timestamps[key] = time.time()
        
    def __delitem__(self, key: str) -> None:
        super().__delitem__(key)
        self._timestamps.pop(key, None)
        
    def start(self, loop: asyncio.AbstractEventLoop):
        def save_del(d, key):
            try:
                dict.__delitem__(d, key)
            except:
                pass
        
        async def runner():
            while not self._stop:
                await asyncio.sleep(self._expire_time)
                now = time.time()
                to_delete = [k for k, t in self._timestamps.items() if now - t > self._expire_time]
                for k in to_delete:
                    save_del(self, k)   # this will not trigger `__delitem__`
                    save_del(self._timestamps, k)
                # no need locks, as no concurrent access in asyncio loop
                
        loop.create_task(runner())

    def stop(self):
        self._stop = True
        
    def __del__(self):
        self.stop()

class ChannelReader(Protocol):
    async def read(self, max_bytes: int) -> bytes: ...
    async def close(self) -> Any: ...
    
class ChannelWriter(Protocol):
    async def write(self, data: bytes) -> bytes: ...
    async def close(self) -> Any: ...

class AsyncioChannelReader:
    '''default implementation of ChannelReader using asyncio.StreamReader'''
    def __init__(self, reader: asyncio.StreamReader):
        self._reader = reader
        self._closed = False
    
    async def read(self, max_bytes: int) -> bytes:
        if self._closed:
            return b''
        try:
            return await self._reader.read(max_bytes)
        except ConnectionError:
            self._closed = True
            return b''
    
    async def close(self) -> Any:
        self._closed = True

class AsyncioChannelWriter:
    '''default implementation of ChannelWriter using asyncio.StreamWriter'''
    def __init__(self, writer: asyncio.StreamWriter):
        self._writer = writer
    
    async def write(self, data: bytes) -> bytes:
        self._writer.write(data)
        await self._writer.drain()
        return data
    
    async def close(self) -> Any:
        self._writer.close()
        try:
            await self._writer.wait_closed()
        except BaseException as e:
            _logger.debug(f'Error when closing writer. {type(e)}: {e}')

class EventCommunicationBase(ABC):
    
    # basic info
    _name: str
    '''unique name for identifying the server instance'''
    _description: str|None = None
    '''description of the server instance'''
    _chunk_size: int = _DEFAULT_CHUNK_SIZE
    '''maximum chunk size for sending/receiving raw data. Data larger
    than this size will be split into multiple chunks, to prevent blocking the event loop.'''
    _auth: str|None = None
    '''authentication token for server to verify clients/clients to connect to server.'''
    _auth_timeout: float = 5.0
    '''timeout in seconds for authentication. Default is 5.0 seconds.'''
    
    # communication internals
    _events: dict[str, EventHandlerInfo]
    '''registered event handlers. {event_name: EventHandlerInfo}'''
    _pipes: dict[str, Queue] 
    '''pipes for `SocketPipeData` communication. {pipe_id: Queue}.
    This dictionary is actually `_ExpireDict`
    '''
    _clients: dict[str, PeerInfo]
    '''
    {id: EventSocketPeerInfo} mapping of all connected clients.
    NOTE: 
    - for EventSocketServer, this is all clients connected to this server.
    - for EventSocketClient, this is always 1 entry for the server it is connected to.
    '''
    
    # runtime internals
    _stop_event: asyncio.Event
    '''event to signal stopping the server'''
    _streaming_chunks: dict[str, bytearray] 
    '''chunks being streamed. {id: bytearray}.
    This dictionary is actually `_ExpireDict`'''
    _started: bool = False
    '''whether the server is started'''
    _runner_thread: Thread|None = None
    '''thread for running the server'''
    _reader_writers: dict[str, tuple[ChannelReader, ChannelWriter]]
    '''reader/writer pairs for each connected client. {client_id: (reader, writer)}'''
    
    # extra custom callbacks
    _on_received: _SyncOrAsyncFunc[[bytes, PeerInfo], Any]|None = None
    _on_disconnected: _SyncOrAsyncFunc[[PeerInfo], Any]|None = None
    _on_connected: _SyncOrAsyncFunc[[PeerInfo], Any]|None = None
    _async_on_received: bool = False
    _async_on_disconnected: bool = False
    _async_on_connected: bool = False
    
    def __init__(self, /, **kwargs: Unpack[_ServerInitCommonParams]):
        self._stop_event = asyncio.Event()
        self._events = {}
        self._clients = {}
        self._reader_writers = {}
        self._streaming_chunks = _ExpireDict()
        self._pipes = _ExpireDict()
        self._auth = kwargs.get('auth', None)
        self._auth_timeout = kwargs.get('auth_timeout', 5.0)
        self._chunk_size = kwargs.get('chunk_size', _DEFAULT_CHUNK_SIZE)
        
        self._name = kwargs.get('name', _random_uuid())
        self._description = kwargs.get('description', None)
        self._host = kwargs.get('host', 'localhost')
        if self._host == '127.0.0.1':
            self._host = 'localhost'
        elif self._host == _get_local_ip() or self._host.lower() == _get_global_IP():
            self._host = '0.0.0.0'  # when binding, listen on all interfaces
        
        if (on_received:=kwargs.get('on_received', None)) is not None:
            self.set_on_received(on_received)
        if (on_disconnected:=kwargs.get('on_disconnected', None)) is not None:
            self.set_on_disconnected(on_disconnected)
        if (on_connected:=kwargs.get('on_connected', None)) is not None:
            self.set_on_connected(on_connected)
            
    def __repr__(self):
        return f'<{self.__class__.__name__}({self.name})>'
    
    __str__ = __repr__
    
    @property
    @abstractmethod
    def is_server(self)->bool:
        raise NotImplementedError
    
    # region properties
    @property
    def name(self) -> str:
        return self._name
    
    @property
    def description(self) -> str|None:
        return self._description
    
    @property
    def chunk_size(self) -> int:
        return self._chunk_size
    
    @property
    def host(self) -> str:
        return self._host
    
    @property
    def auth(self) -> str|None:
        '''Authentication token for clients to connect/ server to verify clients.'''
        return self._auth
    
    @property
    def auth_timeout(self) -> float:
        '''Timeout in seconds for authentication. Default is 5.0 seconds.'''
        return self._auth_timeout
    
    def get_peer_info(self, name_or_id:str, alive_only: bool=True)->"PeerInfo|None":
        if not (client := self._clients.get(name_or_id, None)):
            for c in self._clients.values():
                if c.name == name_or_id:
                    client = c
                    break
        if client and (alive_only and not client.alive):
            return None
        return client
    
    @property
    def logger(self)->Logger:
        if not (logger:=getattr(self, '_logger', None)):
            logger = get_logger(self.name)
            setattr(self, '_logger', logger)
        return logger
    # endregion
    
    async def _serve_loop(self, reader: ChannelReader, writer: ChannelWriter, client_id: str, stop_event: asyncio.Event|None=None):
        self.logger.debug(f'Starting serve loop for client {client_id}.')
        target_data_size = None
        buffer = bytearray()
        
        def stopped():
            if self._stop_event.is_set():
                return True
            if stop_event and stop_event!=self._stop_event and stop_event.is_set():
                return True
            return False
        
        while not stopped():
            buffer += await reader.read(self._chunk_size + 37)  # 4 bytes size + 32 bytes id + 1 byte is_end
            # the above line will raise if the connection is lost
            if len(buffer) == 0:
                await asyncio.sleep(0.0)
                continue
            
            if target_data_size is None:
                # first 4 bytes indicate the message size
                target_data_size = struct.unpack('I', buffer[0:4])[0]
            
            while target_data_size is not None and ((len(buffer)-37) >= target_data_size):
                id = buffer[4:36].split(b'\0', 1)[0].decode('utf-8')
                is_end = struct.unpack('B', buffer[36:37])[0]
                message_data = buffer[37:37 + target_data_size]
                current_chunks: bytearray|None = self._streaming_chunks.get(id, None)
                if is_end == 1:
                    if current_chunks is not None:
                        message_data = bytes(current_chunks + message_data)
                        del self._streaming_chunks[id]
                    asyncio.create_task(self.handle_received(message_data, client_id)) # handle in background
                else:
                    if current_chunks is None:
                        current_chunks = bytearray()
                        self._streaming_chunks[id] = current_chunks
                    current_chunks += message_data
                    
                buffer = buffer[37+target_data_size:]
                if len(buffer) >= 4:
                    target_data_size = struct.unpack('I', buffer[0:4])[0]
                else:
                    target_data_size = None
                    
        self.logger.debug(f'Stopping serve loop for client {client_id}.')
        try:
            await reader.close()
        except BaseException:
            self.logger.debug(f'Error when closing reader for client {client_id}.', exc_info=True)
        try:
            await writer.close()
        except BaseException:
            self.logger.debug(f'Error when closing writer for client {client_id}.', exc_info=True)
    
    @abstractmethod
    async def _internal_start(self):
        raise NotImplementedError
    
    def start(self):
        if self._started:
            return
        self._started = True
        
        self._stop_event.clear()
        self._streaming_chunks.clear()
        self._pipes.clear()
        self._clients.clear()
        
        self._runner_thread = Thread(target=lambda: asyncio.run(self._internal_start()), daemon=True)
        self._runner_thread.start()
    
    async def send(self, data: bytes|SocketBaseData, client: str|None=None):
        '''
        Send raw data to another server.
        If data size exceeds `chunk_size`, it will be split into multiple chunks.
        
        Args:
            - client: target client id or name. Can be None for client side.
            - data: raw bytes data to send.
        '''
        if not client:
            if self.is_server:
                raise ValueError('Client id/name must be provided when sending from server side.')
            if self._clients:
                client = next(iter(self._clients.keys()))
            else:
                raise ConnectionLostError('No connected server found. Cannot send data.')
        
        if isinstance(data, SocketBaseData):
            return await data.send(self, client)
        
        _, writer = self._reader_writers.get(client, (None, None))
        if not writer:
            if (info:=self.get_peer_info(client, alive_only=False)) is None:    
                raise ConnectionLostError(f'Client `{client}` is not connected. Cannot send data.')
            _, writer = self._reader_writers.get(info.id, (None, None))
        if not writer:
            raise ConnectionLostError(f'Client `{client}` is not connected. Cannot send data.')
        
        id = _random_uuid()
        id_bytes = id.encode('utf-8')   # 32 bytes
        id_bytes = id_bytes.ljust(32, b'\0')[:32]
        for i in range(0, len(data), self._chunk_size):
            chunk = data[i:i + self._chunk_size]
            is_end = 1 if i + self._chunk_size >= len(data) else 0
            header = struct.pack('I', len(chunk)) + id_bytes + struct.pack('B', is_end)
            try:
                await writer.write(header + chunk)
            except ConnectionError:
                raise ConnectionLostError(f'Connection to client {client} is lost during sending.')
    
    def stop(self):
        if not self._started:
            return
        self._started = False
        self._stop_event.set()
        
        async def _stop_reader_writers():
            coros = []
            for reader, writer in self._reader_writers.values():
                coros.append(reader.close())
                coros.append(writer.close())
            await asyncio.gather(*coros, return_exceptions=True)
        
        try:
            run_any_func(_stop_reader_writers)
        except BaseException as e:
            self.logger.error(f'Error stopping `{self.__class__.__name__}(name={self.name})`. Error: {type(e)}: {e}', exc_info=True)
        
        self._reader_writers.clear()
        self._clients.clear()
        self._streaming_chunks.clear()
        self._pipes.clear()
        if self._runner_thread and self._runner_thread.is_alive():
            try:
                self._runner_thread.join(timeout=5.0)
            except:
                self.logger.warning(f'Failed to join runner thread for `{self.__class__.__name__}(name={self.name})`.')
                pass
            finally:
                self._runner_thread = None
    
    @overload
    def event(self, f: _F, /) -> _F: ...
    @overload
    def event(self, /, name:str) -> Callable[[_F], _F]: ...
    
    def event(self, f=None, /, name=None):  # type: ignore
        '''
        Register an event handler.
        Event handler function's parameters/return value must be serializable,
        e.g. str/int/float/bool/bytes/dict/list/tuple/set/frozenset, or any pydantic BaseModel.
        
        It can also have streaming parameters/return value by using `AsyncIterator[T]` as the type hint.
        Its recommended to annotate all parameters and return value with proper type hints for better type conversion. 
        '''
        if f:
            f_is_async = _is_async_callable(f)
            name = name or f.__name__
            self._events[name] = EventHandlerInfo(func=f, is_async=f_is_async)
            return f
        else:
            def decorator(func: _F) -> _F:
                func_is_async = _is_async_callable(func)
                event_name = name or func.__name__
                self._events[event_name] = EventHandlerInfo(func=func, is_async=func_is_async)
                return func
            return decorator
    
    @overload
    async def invoke(
        self, 
        to_client: str, 
        event: str, 
        params:dict[str, SerializableType|AsyncIterator[SerializableType]],
        timeout: float|None=_DEFAULT_COMM_TIMEOUT,
    )->SerializableType|AsyncIterator[SerializableType]:...
    
    @overload
    async def invoke(
        self, 
        to_client: str, 
        event: str, 
        params:dict[str, SerializableType|AsyncIterator[SerializableType]], 
        return_type: type[_RT],
        timeout: float|None=_DEFAULT_COMM_TIMEOUT,
    )->_RT:...
    
    async def invoke(   # type: ignore
        self, 
        to_client: str, 
        event: str, 
        params:dict[str, SerializableType|AsyncIterator[SerializableType]], 
        return_type=None,
        timeout: float|None=_DEFAULT_COMM_TIMEOUT,
    ):   # type: ignore
        '''
        Invoke an event on another server. Event must be registered on it.
        If `return_type` is provided, it will try to convert the return value to the given type,
        and raise `TypeError` if conversion fails.
        
        NOTE:
            1. For streaming return value, the caller should provide `AsyncIterator[T]` as `return_type`.
            2. support sending streaming parameters using `AsyncIterator[T]` as parameter type.
        '''
        event_id = _random_uuid()
        if (client_info:=self.get_peer_info(to_client, alive_only=False)):
            to_client = client_info.id  # convert to id if name is given
        event_data = EventData(
            id=event_id,
            event=event,
            data=params,
        )
        await event_data.send(self, to_client)
        return_pipe_key = EventData.BuildPipeKey(event_id, event, '__return__')
        t = 0
        while (return_pipe:=self._pipes.get(return_pipe_key, None)) is None:
            await asyncio.sleep(0.1)
            t += 0.1
            if timeout and t >= timeout:
                raise ConnectionTimeoutError(f'Timeout waiting for event `{event}` return from client `{to_client}`.')
        r = await SocketBaseData.GetDataFromPipe(return_pipe, on_finished=lambda: self._pipes.pop(return_pipe_key, None))  # type: ignore
        # `r` is actually `EventFieldData` or `AsyncGenerator[EventFieldData]`
        if isinstance(r, EventFieldData) and r.is_error:
            raise EventInvokeError(f'Error invoking event `{event}` on client `{to_client}`: {r.data}')
        
        if return_type:
            need_convert = True
            rt_origin = _get_origin_type(return_type)
            if rt_origin in (Any, Ellipsis, SerializableType, object, None, type(None), inspect.Signature.empty):
                need_convert = False
            if need_convert:
                if isinstance(r, AsyncGenerator):
                    if not check_type_is(rt_origin, AsyncIterator):
                        self.logger.warning(f'Return type for event `{event}` is not AsyncIterator, but got `{rt_origin}`. No conversion will be performed.')
                        need_convert = False
                    else:
                        if (rt_args := get_args(return_type)):
                            return_type = rt_args[0]
                            if rt_origin in (Any, Ellipsis, SerializableType, object, None, type(None), inspect.Signature.empty):
                                need_convert = False
                        else:
                            need_convert = False
            if need_convert:
                if isinstance(r, AsyncGenerator):
                    async def gen_wrapper(gen: AsyncGenerator[EventFieldData, None]): # type: ignore
                        async for item in gen:
                            if item.is_error:
                                raise EventInvokeError(f'Error invoking event `{event}` on client `{to_client}`: {item.data}')
                            yield _convert_val(orjson.loads(item.data), return_type)
                    r = gen_wrapper(r)  # type: ignore
                else:
                    r = _convert_val(orjson.loads(r.data), return_type) # type: ignore
        else:
            if isinstance(r, AsyncGenerator):
                async def gen_wrapper(gen): # type: ignore
                    async for item in gen:
                        if item.is_error:
                            raise EventInvokeError(f'Error invoking event `{event}` on client `{to_client}`: {item.data}')
                        yield orjson.loads(item.data)
                r = gen_wrapper(r)
            else:
                r = orjson.loads(r.data) # type: ignore
        return r
    
    # region callbacks
    async def _call_handler(self, func, is_async, *args, **kwargs):
        try:
            if is_async:
                return await func(*args, **kwargs)
            else:
                loop = asyncio.get_running_loop()
                # prevent blocking the event loop
                return await loop.run_in_executor(None, func, *args, **kwargs)
        except asyncio.CancelledError:
            pass
        except BaseException as e:
            self.logger.error(f'Error in calling handler. {type(e)}: {e}')
            raise e
    
    async def handle_received(self, data: bytes, from_client_id: str):
        if (peer_info := self.get_peer_info(from_client_id, alive_only=False)):
            # only trigger `_on_received` for known clients
            asyncio.create_task(self._call_handler(self._on_received, self._async_on_received, data, peer_info))    # run in background
        try:
            socket_data = await SocketBaseData.Parse(data, self)
        except BaseException as e:
            self.logger.warning(f'Received invalid data `{data[:50]}...` from client `{from_client_id}`. Error: {type(e)}: {e}', exc_info=True)
        else:
            try:
                await socket_data.on_received(self, from_client_id)
            except BaseException as e:
                self.logger.error(f'Error handling received data `{type(socket_data).__name__}` from client `{from_client_id}`. Error: {type(e)}: {e}', exc_info=True)

    async def handle_disconnected(self, peer_info: PeerInfo):
        return await self._call_handler(self._on_disconnected, self._async_on_disconnected, peer_info)
        
    async def handle_connected(self, peer_info: PeerInfo):
        return await self._call_handler(self._on_connected, self._async_on_connected, peer_info)
        
    def set_on_received(self, callback: _SyncOrAsyncFunc[[bytes, PeerInfo], Any]):
        self._on_received = callback
        self._async_on_received = _is_async_callable(callback)
        
    def set_on_disconnected(self, callback: _SyncOrAsyncFunc[[PeerInfo], Any]):
        self._on_disconnected = callback
        self._async_on_disconnected = _is_async_callable(callback)
        
    def set_on_connected(self, callback: _SyncOrAsyncFunc[[PeerInfo], Any]):
        self._on_connected = callback
        self._async_on_connected = _is_async_callable(callback)
    # endregion

    def __del__(self):
        self.stop()

class EventClientBase(EventCommunicationBase):
    
    _retry_connection_interval: float
    '''interval in seconds between retrying connection when connection is lost.'''
    
    def __init__(self, /, **kwargs: Unpack[_ClientInitCommonParams]):
        super().__init__(**kwargs)
        self._retry_connection_interval = kwargs.get('retry_connection_interval', 6.0)
    
    @property
    def retry_connection_interval(self) -> float:
        return self._retry_connection_interval
    
    @property
    @override
    def is_server(self):
        return False
    
    @override
    async def _internal_start(self):
        server_id = _random_uuid()
        
        async def handshake_and_keepalive():
            timeout = self.auth_timeout
            h = HandShake(
                pipe_id=server_id,
                name=self.name,
                description=self.description,
                host=self.host,
                port=getattr(self, "port", None),
                auth=self.auth
            )
            self.logger.debug(f'Sending handshake to server...')
            await h.send(self, server_id)
            while not self._stop_event.is_set() and ((pipe:=self._pipes.get(server_id, None)) is None):
                await asyncio.sleep(0.1)
                timeout -= 0.1
                if timeout <= 0:
                    self.logger.warning('Handshake response timeout from server.')
                    raise ConnectionTimeoutError('Timeout waiting for handshake response from server.')
            response: HandShakeResult = await SocketBaseData.GetDataFromPipe(pipe, on_finished=lambda: self._pipes.pop(server_id, None))  # type: ignore
            if not response.success:
                self._stop_event.set()  # not able to connect, stop the client
                msg = f'Handshake failed: {response.fail_reason}. Cannot connect to server.'
                self.logger.error(msg)
                raise ConnectionError(msg)
            else:
                server_info = PeerInfo(
                    id=server_id,
                    name=response.name or server_id,
                    description=response.description,
                    host=response.host or self.host,
                    port=response.port,
                )
                self._clients[server_id] = server_info
                self.logger.info(f'Connected to server `{server_info.name}`({server_info.id}) at {server_info.host}:{server_info.port}.')
                await self.handle_connected(server_info)
                while not self._stop_event.is_set():
                    await asyncio.sleep(_KEEP_ALIVE_INTERVAL_SECONDS)
                    await KeepAlive().send(self, server_id)
        
        while not self._stop_event.is_set():
            try: 
                reader, writer = await self.open_connection(server_id)
                self._reader_writers[server_id] = (reader, writer)
                await asyncio.gather(
                    self._serve_loop(reader, writer, server_id),
                    handshake_and_keepalive(),
                )
            except ConnectionError:
                # fail to connect server, retry after a short delay
                if (info:=self.get_peer_info(server_id, alive_only=False)):
                    asyncio.create_task(self.handle_disconnected(info))  # run in background
                if self._stop_event.is_set():
                    break   # when failing in authentication, stop immediately
                await asyncio.sleep(self.retry_connection_interval)

    @abstractmethod
    async def open_connection(self, server_id: str)->tuple[ChannelReader, ChannelWriter]:
        '''define how to open connection to the server.'''
        raise NotImplementedError
    
class EventServerBase(EventCommunicationBase):
    '''Base class for event server.'''
    
    _client_validation_results: dict[str, tuple["HandShake|None", str|None, bool]]
    '''set of client names who waiting for handshake validation.
    {id: (HandShake|None, fail_reason|None, success:bool)}'''
    
    def __init__(self, /, **kwargs: Unpack[_ServerInitCommonParams]):
        super().__init__(**kwargs)
        self._client_validation_results = {}
    
    @abstractmethod
    async def start_server(self, on_channel_created: Callable[[ChannelReader, ChannelWriter], Awaitable[Any]]):
        '''
        Start the server listening for incoming connections.
        `on_channel_created` will be called when a new client connects, with [reader, writer] as parameters.
        
        NOTE: this method should be blocked until server is stopped.'''
        raise NotImplementedError
    
    async def on_channel_created(self, reader: ChannelReader, writer: ChannelWriter):
        client_id = _random_uuid()
        self.logger.debug(f'New connection incoming, assigned id: {client_id}.')
        self._reader_writers[client_id] = (reader, writer)
        stop_event = asyncio.Event()
        
        async def validate_client():
            timeout = self.auth_timeout
            while self._client_validation_results.get(client_id, None) is None and not self._stop_event.is_set():
                await asyncio.sleep(0.05)
                timeout -= 0.05
                if timeout <= 0 and not self._stop_event.is_set():
                    self.logger.info(f'Client {client_id} failed to validate in time. Closing connection.')
                    stop_event.set()
                    break
                
            if not self._stop_event.is_set() and not stop_event.is_set():
                result, reason, succ = self._client_validation_results.pop(client_id, (None, 'Unknown Reason', False))
                reason = reason or 'Unknown Reason'
                if not result:
                    self.logger.info(f'Client {client_id} failed to validate. Closing connection.')
                    stop_event.set()
                else:
                    if not succ:
                        self.logger.info(f'Client {client_id} failed to validate: {reason}. Closing connection.')
                        stop_event.set()
                    else:
                        client_info = PeerInfo(
                            id=client_id,
                            name=result.name or client_id,
                            description=result.description,
                            host=result.host,
                            port=result.port,
                        )
                        self._clients[client_id] = client_info
                        await self.handle_connected(client_info)
                        self.logger.info(f'Client `{client_info.name}`({client_info.id}) connected from {client_info.host}:{client_info.port}.')
 
        try:
            await asyncio.gather(
                self._serve_loop(reader, writer, client_id, stop_event),
                validate_client(),
            )
        finally:
            if (info:=self.get_peer_info(client_id, alive_only=False)):
                asyncio.create_task(self.handle_disconnected(info))  # run in background
            stop_event.set()
            self._reader_writers.pop(client_id, None)
            self._client_validation_results.pop(client_id, None)
            self._clients.pop(client_id, None)
            
    @property
    @override
    def is_server(self):
        return True
    
    @override
    async def _internal_start(self):
        try:
            await self.start_server(self.on_channel_created)
        except asyncio.CancelledError:
            pass

class _SocketBaseMixin:
    _port: int|None = None
    '''port to connect/listen on.'''
    _identifier: str|None = None
    '''identifier for AF_UNIX/AF_PIPE socket.'''
    
    def _init_socket_base(self, port, identifier):
        if (port is None) and (identifier is None):
            raise ValueError('Either port or identifier must be provided, but not both.')
        if port is not None:
            assert isinstance(port, int) and port >0 and port <65536, 'Port must be an integer between 1 and 65535.'
            self._port = port
        else:
            assert isinstance(identifier, str) and len(identifier) >0, 'Identifier must be a non-empty string.'
            self._identifier = identifier
    
    # region properties
    @property
    def port(self) -> int|None:
        return self._port
    
    @property
    def identifier(self) -> str|None:
        return self._identifier
    # endregion

class EventSocketClient(EventClientBase, _SocketBaseMixin):
    '''
    Event client by using sockets.
    
    TODO: 
     - support AF_PIPE on Windows
     - support AF_UNIX on Linux
     - support SSL/TLS encryption
    '''
    
    @overload
    def __init__(self, /, port: int, **kwargs: Unpack[_ServerInitCommonParams]): 
        '''connect to or create a local socket server on the given port.'''
    
    @overload
    def __init__(self, /, identifier: str, **kwargs: Unpack[_ServerInitCommonParams]):
        '''connect to or create a local socket server with the given identifier.'''
    
    def __init__(self, /, port: int|None=None, identifier: str|None=None, **kwargs: Unpack[_ServerInitCommonParams]): # type: ignore
        super().__init__(**kwargs)
        self._init_socket_base(port, identifier)
    
    # region properties
    @property
    def connected(self) -> bool:
        '''whether the client is connected to the server.'''
        if len(self._clients) > 0:
            client_id = next(iter(self._clients.keys()))
            if self.get_peer_info(client_id, alive_only=True):
                return True
        return False
    # endregion
    
    @override
    async def open_connection(self, server_id: str):
        if self._port is not None:
            asyncio_reader, asyncio_writer = await asyncio.open_connection(self._host, self._port)
            return AsyncioChannelReader(asyncio_reader), AsyncioChannelWriter(asyncio_writer)
        elif self._identifier is not None:
            if os.name == 'nt':
                raise NotImplementedError('AF_UNIX sockets are not supported on Windows yet.')
            else:
                asyncio_reader, asyncio_writer = await asyncio.open_unix_connection(self._identifier)
                return AsyncioChannelReader(asyncio_reader), AsyncioChannelWriter(asyncio_writer)
        else:
            raise ValueError('Either port or identifier must be provided to open connection.')

class EventSocketServer(EventServerBase, _SocketBaseMixin):
    '''
    Event server default implementation, by using sockets.
    
    TODO: 
     - support AF_PIPE on Windows
     - support AF_UNIX on Linux
     - support SSL/TLS encryption
    '''
    
    _server: asyncio.AbstractServer|None = None
    '''server instance, will be created in `internal_start()`'''

    @overload
    def __init__(self, /, port: int, **kwargs: Unpack[_ServerInitCommonParams]): 
        '''connect to or create a local socket server on the given port.'''
    
    @overload
    def __init__(self, /, identifier: str, **kwargs: Unpack[_ServerInitCommonParams]):
        '''connect to or create a local socket server with the given identifier.'''
    
    def __init__(self, /, port: int|None=None, identifier: str|None=None, **kwargs: Unpack[_ServerInitCommonParams]): # type: ignore
        super().__init__(**kwargs)
        self._init_socket_base(port, identifier)

    @override
    async def start_server(self, on_channel_created: Callable[[ChannelReader, ChannelWriter], Awaitable[Any]]):
        async def on_channel_created_wrapper(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
            channel_reader = AsyncioChannelReader(reader)
            channel_writer = AsyncioChannelWriter(writer)
            await on_channel_created(channel_reader, channel_writer)
        if self._identifier is not None:
            if os.name == 'nt':
                raise NotImplementedError('AF_UNIX sockets are not supported on Windows yet.')
            socket_path = tempfile.gettempdir() + os.sep + self._identifier
            self._server = await asyncio.start_unix_server(on_channel_created_wrapper, path=socket_path)
        else:
            self._server = await asyncio.start_server(on_channel_created_wrapper, self._host, self._port)
        async with self._server:
            await self._server.serve_forever()
        self._server = None


__all__.extend([
    'ChannelReader',
    'ChannelWriter',
    'AsyncioChannelReader',
    'AsyncioChannelWriter',
    
    'EventHandlerInfo',
    'EventCommunicationBase',
    'EventClientBase',
    'EventServerBase',
    'EventSocketServer',
    'EventSocketClient',
])


if __name__.endswith('main__'):
    def _test_local_p2p_server(port: int, is_server=True):
        import time
        
        prefix = f'|{os.getpid()}({"server" if is_server else "client"})|'
        if is_server:
            name = 'test-server'
            server = EventSocketServer(port=port, name=name)
            @server.event
            async def hello_event(data: int):
                print(f'{prefix} hello_event called with data: {data}(type={type(data)})')
                r = await server.invoke(
                    to_client='test-client',
                    event='plus',
                    params={'a': data, 'b': 10},
                    return_type=int,
                )
                print(f'{prefix} got result from plus: {r}(type={type(r)})')
                return r
            @server.event(name='test_stream_server')
            async def test_stream(data: list[int]):
                async def gen():
                    for i in data:
                        yield i
                        await asyncio.sleep(0.25)
                r = await server.invoke(
                    to_client='test-client',
                    event='test_stream',
                    params={'data': gen()},
                )
                final = []
                async for i in r:   # type: ignore
                    final.append(i)
                return final
        else:
            name = 'test-client'
            server = EventSocketClient(port=port, name=name)
            @server.event
            async def plus(a:int|float, b:int|float):
                await asyncio.sleep(0.5)
                return a + b
            @server.event
            async def test_stream(data: AsyncIterator[int]):
                async for i in data:
                    yield i + 1
            
        async def on_received(data: bytes, peer):
            print(f'{prefix} received: {data[:50]}... from `{peer.name}`.')
            
        async def on_connected(peer):
            print(f'{prefix} connected to peer: `{peer.name}`.')
            if not is_server:
                r = await server.invoke(
                    to_client=peer.name,
                    event='hello_event',
                    params={'data': 42},
                )
                print(f'{prefix} hello_event returned: {r}')
                
                r = await server.invoke(
                    to_client=peer.name,
                    event='test_stream_server',
                    params={'data': [1,2,3,4,5]},
                )
                print(f'{prefix} test_stream returned: {r}')    
            
        def on_disconnected(peer):
            print(f'{prefix} disconnected.')
        
        server.set_on_received(on_received)
        server.set_on_connected(on_connected)
        server.set_on_disconnected(on_disconnected)

        server.start()
        time.sleep(4)

        
if __name__ == '__main__':
    from multiprocessing import Process
    
    test_port = find_available_port()
    p1 = Process(target=_test_local_p2p_server, args=(test_port, True))
    p2 = Process(target=_test_local_p2p_server, args=(test_port, False))
    
    p1.start()
    p2.start()
    p1.join()
    p2.join()
    print(f'Main process({os.getpid()}) exiting...')