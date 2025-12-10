import time
import asyncio
import pickle
import struct
import random

from threading import Thread, Event
from multiprocessing.queues import Queue
from queue import Empty as EmptyQueueException, Queue as ThreadQueue
from typing import AsyncIterable, Any, Iterator, TypeAlias, Literal, overload, Callable, TypeVar

from ..common_utils.concurrent_utils import run_any_func

_DEFAULT_EXPIRY_TIMEOUT_SECS = 180.0
_DEFAULT_CHECK_EXPIRE_INTERVAL_SECS = 15.0
_DEFAULT_GET_TIMEOUT_SECS = 180.0

_Message: TypeAlias = dict[str, Any]
_T = TypeVar('_T')

def _random_id() -> str:
    return ''.join(random.choices('abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=16))

_MSG_NORMAL = b'\0'
_MSG_STREAM = b'\1'
_MSG_CUSTOM = b'\2'
_MSG_STREAM_END = b'\3'

_end_stream = object()

class MessagePool:
    
    # receive only fields
    receive_runner: Thread
    receive_runner_stop_event: Event
    received_messages: dict[str, tuple[float, bytes]] # {id: (timestamp, output)}
    received_stream_messages: dict[str, tuple[float, ThreadQueue[bytes]]] # {id: (timestamp, [stream_chunks, ..., end_stream])}
    
    message_queue: Queue[bytes]
    
    _mode: Literal['send', 'receive']
    _default_get_timeout: float = _DEFAULT_GET_TIMEOUT_SECS
    _expire_timeout_secs: float = _DEFAULT_EXPIRY_TIMEOUT_SECS
    _check_expire_interval_secs: float = _DEFAULT_CHECK_EXPIRE_INTERVAL_SECS
    _checking_expire: bool = False
    
    def __init__(
        self,
        mode: Literal['send', 'receive'],
        default_timeout: float=_DEFAULT_GET_TIMEOUT_SECS,
        expire_timeout_secs: float=_DEFAULT_EXPIRY_TIMEOUT_SECS,
        check_expire_interval_secs: float|None=None,
        message_queue: Queue[bytes]|None=None,
    ) -> None:
        self._default_get_timeout = default_timeout
        self._expire_timeout_secs = expire_timeout_secs
        if check_expire_interval_secs is None:
            check_expire_interval_secs = max(expire_timeout_secs / 4, 0.1)
        self._check_expire_interval_secs = check_expire_interval_secs
        self._mode = mode
        
        if not message_queue:
            self.message_queue = Queue()
        else:
            self.message_queue = message_queue
        
        if self._mode == 'receive':    
            self.received_messages = {}
            self.received_stream_messages = {}
            self.receive_runner_stop_event = Event()
            self.receive_runner = Thread(target=self._runner, daemon=True)
            self.receive_runner.start()
    
    # region send
    async def _async_stream_fields(self, *streams: tuple[str, str, AsyncIterable[Any], Callable[[Any], bytes]|None]):
        async def streamer(id, field, stream, serializer=None):
            async for chunk in stream:
                buf = self._build_stream_message_chunk(id, field, chunk)
                self.message_queue.put(buf)
            self.message_queue.put(self._build_stream_message_chunk(id, field, _end_stream))
        await asyncio.gather(*[streamer(id, field, stream, serializer) for id, field, stream, serializer in streams])
    
    def _stream_fields(self, *streams: tuple[str, str, Iterator[Any], Callable[[Any], bytes]|None]):
        for id, field, stream, serializer in streams:
            for chunk in stream:
                buf = self._build_stream_message_chunk(id, field, chunk, serializer)
                self.message_queue.put(buf)
            self.message_queue.put(self._build_stream_message_chunk(id, field, _end_stream, serializer))
            
    def _build_stream_message_chunk(self, id: str, field: str, chunk: Any, serializer: Callable[[Any], bytes]|None=None) -> bytes:
        buf = bytearray()
        if chunk is _end_stream:
            buf.extend(_MSG_STREAM_END)  # stream end message
        else:
            buf.extend(_MSG_STREAM)  # stream message
        # id
        buf.extend(id.encode('utf-8').ljust(16, b'\0'))
        # field name
        buf.extend(field.encode('utf-8').ljust(128, b'\0'))
        if chunk is _end_stream:
            data = b''
        else:
            if serializer is not None:
                data = serializer(chunk)
            else:
                data = pickle.dumps(chunk)
        buf.extend(struct.pack('!I', len(data)))
        buf.extend(data)
        return buf
    
    @overload
    def send(self, message: bytes, /, id: str|None=None)->str: ...
    @overload
    def send(self, message: _Message, /, serializer: dict[str, Callable[[Any], bytes]]|None=None, id: str|None=None)->str: ...
    @overload
    def send(self, message: Any, /, serializer: Callable[[Any], bytes], id: str|None=None)->str: ...
    
    def send(self, message: _Message, /, serializer=None, id: str|None=None)->str:  # type: ignore
        '''
        Send a message to the queue.
        
        Args:
            message: The message to send. It can be:
                - bytes: raw bytes to send as custom message.
                - dict: normal message with fields. Note that field values can be streams (Iterator or AsyncIterable).
                - Any: custom message serialized with the given serializer.
            serializer: The serializer to use for custom message. This is only for non-bytes message:
                        - for normal message (dict):
                            serializer is {field_name: serializer_function} mapping for serializing specific fields.
                            If a field is not in the mapping, pickle will be used.
                        - for other types:
                            serializer is a function that takes the message and returns bytes.
            id: The id of the message. If not given, a random id will be generated.
        
        Returns:
            The id of the sent message.
            
        normal message format:
        |-- 1 bytes type --| (type flag)
        |-- 16 bytes id --|
        |-- 128 bytes field name --||-- 1 bytes(is generator flag) --||-- 4 bytes data length(0 for generator) --||-- data --|
        |-- 128 bytes field name --||-- 1 bytes(is generator flag) --||-- 4 bytes data length(0 for generator) --||-- data --|
        ...
        
        stream message chunk format: (or STREAM END message)
        |-- 1 bytes type --| 
        |-- 16 bytes id --|
        |-- 128 bytes field name --||-- data --| (empty data for STREAM END)
        
        custom message format:
        |-- 1 bytes type --|
        |-- 16 bytes id --|
        |-- data --|
        '''
        assert self._mode == 'send', "MessagePool is not in send mode."
        if not id:
            id = _random_id()
        else:
            assert len(id) == 16, "id must be 16 characters long."
        
        buf = bytearray()
        if isinstance(message, bytes) or (serializer is not None and callable(serializer)):
            buf.extend(_MSG_CUSTOM)  # custom message
            buf.extend(id.encode('utf-8').ljust(16, b'\0'))
            if isinstance(message, bytes):
                data = message
            else:
                data: bytes = serializer(message)  # type: ignore
            buf.extend(data)
        else:
            buf.extend(_MSG_NORMAL)  # normal message
            buf.extend(id.encode('utf-8').ljust(16, b'\0'))
            async_streams = [] # [(field_name, stream, serializer), ...]
            streams = [] # [(field_name, stream, serializer), ...]
            
            for k,v in message.items():
                # field name
                buf.extend(k.encode('utf-8').ljust(128, b'\0'))
                # stream/normal
                field_serializer = serializer.get(k) if (serializer and isinstance(serializer, dict)) else None
                if isinstance(v, (AsyncIterable, Iterator)):
                    buf.extend(_MSG_STREAM) # to indicate it is a stream field
                    buf.extend(struct.pack('!I', 0)) # length 0 for stream field
                    if isinstance(v, AsyncIterable):
                        async_streams.append((k, v, field_serializer))
                    else:
                        streams.append((k, v, field_serializer))
                else:
                    buf.extend(_MSG_NORMAL)
                    if field_serializer is not None:
                        data = field_serializer(v)
                    else:
                        data = pickle.dumps(v)
                    buf.extend(struct.pack('!I', len(data)))
                    buf.extend(data)
            if async_streams:
                run_any_func(self._async_stream_fields, *((id, field, stream, serializer) for field, stream, serializer in async_streams))
            if streams:
                self._stream_fields(*((id, field, stream, serializer) for field, stream, serializer in streams))

        self.message_queue.put(buf)
        return id
    # endregion
    
    # region receive
    async def _async_receiver(self, id: str, field_name: str, timeout: float, deserializer: Callable[[bytes], Any]|None=None):
        key = f"{id}:{field_name}"
        t = time.time()
        while True:
            await asyncio.sleep(0.01)
            if key not in self.received_stream_messages or self._checking_expire:
                continue
            try:
                chunk = self.received_stream_messages[key][1].get(block=False)
                if chunk is _end_stream:
                    break
                if deserializer is not None:
                    yield deserializer(chunk)
                else:
                    yield pickle.loads(chunk)
            except EmptyQueueException:
                pass
            if time.time() - t >= timeout:
                raise TimeoutError(f"Timeout waiting for stream field '{field_name}' in message '{id}'")

    def _sync_receiver(self, id, field_name: str, timeout: float, deserializer: Callable[[bytes], Any]|None=None):
        key = f"{id}:{field_name}"
        t = time.time()
        while True:
            time.sleep(0.01)
            if key not in self.received_stream_messages or self._checking_expire:
                continue
            try:
                chunk = self.received_stream_messages[key][1].get(block=False)
                if chunk is _end_stream:
                    break
                if deserializer is not None:
                    yield deserializer(chunk)
                else:
                    yield pickle.loads(chunk)
            except EmptyQueueException:
                pass
            if time.time() - t >= timeout:
                raise TimeoutError(f"Timeout waiting for stream field '{field_name}' in message '{id}'")
    
    def _parse_message(self, id: str, data: bytes, deserializer=None, _async: bool=False):
        msg_type = data[0:1]
        id = data[1:17].rstrip(b'\0').decode('utf-8')
        if msg_type == _MSG_CUSTOM:
            content = data[17:]
            if deserializer is not None:
                return deserializer(content)
            else:
                return content  # type: ignore
        elif msg_type == _MSG_NORMAL:
            pos = 17
            message: _Message = {}
            while pos < len(data):
                field_name = data[pos:pos+128].rstrip(b'\0').decode('utf-8')
                pos += 128
                field_type = data[pos:pos+1]
                pos += 1
                length = struct.unpack('!I', data[pos:pos+4])[0]
                pos += 4
                field_serializer = deserializer.get(field_name) if (deserializer and isinstance(deserializer, dict)) else None
                if field_type == _MSG_STREAM:
                    # stream field
                    if _async:
                        message[field_name] = self._async_receiver(id, field_name, self._default_get_timeout, field_serializer)
                    else:
                        message[field_name] = self._sync_receiver(id, field_name, self._default_get_timeout, field_serializer)
                else:
                    field_data = data[pos:pos+length]
                    pos += length
                    if field_serializer is not None:
                        value = field_serializer(field_data)
                    else:
                        value = pickle.loads(field_data)
                    message[field_name] = value
            return message
        else:
            raise ValueError(f"Unknown message type: {msg_type}")
    
    @overload
    def get(self, id: str, /, deserializer: dict[str, Callable[[bytes], Any]]|None=None, timeout: float|None=None)->_Message:...
    @overload
    def get(self, id: str, /, deserializer: Callable[[bytes], _T], timeout: float|None=None)->_T:...
    
    def get(self, id:str, /, deserializer=None, timeout: float|None=None):
        '''
        Get a message from the pool by id.
        
        Args:
            id: The id of the message to get.
            deserializer: The deserializer to use for the message. This is only for non-bytes message:
                        - for normal message (dict):
                            deserializer is {field_name: deserializer_function} mapping for deserializing specific fields.
                            If a field is not in the mapping, pickle will be used. 
                        - for other types:
                            deserializer is a function that takes bytes and returns the message.
            timeout: The timeout in seconds to wait for the message. If None, use default timeout.
        
        Returns:
            The message with the given id.
        '''
        assert self._mode == 'receive', "MessagePool is not in receive mode."
        if timeout is None:
            timeout = self._default_get_timeout
            
        t = time.time()
        while True:
            if not self._checking_expire:
                if id in self.received_messages:
                    _, data = self.received_messages.pop(id)
                    return self._parse_message(id, data, deserializer, _async=False)
            time.sleep(0.01)
            if time.time() - t >= timeout:
                break
        raise TimeoutError(f"Timeout waiting for message with id '{id}'")
    
    @overload
    async def aget(self, id: str, /, deserializer: dict[str, Callable[[bytes], Any]]|None=None, timeout: float|None=None)->_Message:...
    @overload
    async def aget(self, id: str, /, deserializer: Callable[[bytes], _T], timeout: float|None=None)->_T:...
    
    async def aget(self, id:str, /, deserializer=None, timeout: float|None=None):
        '''
        Asynchronously get a message from the pool by id.
        
        Args:
            id: The id of the message to get.
            deserializer: The deserializer to use for the message. This is only for non-bytes message:
                        - for normal message (dict):
                            deserializer is {field_name: deserializer_function} mapping for deserializing specific fields.
                            If a field is not in the mapping, pickle will be used.
                        - for other types:
                            deserializer is a function that takes bytes and returns the message.
            timeout: The timeout in seconds to wait for the message. If None, use default timeout.
        
        Returns:
            The message with the given id.
        '''
        assert self._mode == 'receive', "aget can only be called in 'receive' mode."
        if timeout is None:
            timeout = self._default_get_timeout
            
        t = time.time()
        while True:
            if not self._checking_expire:
                if id in self.received_messages:
                    _, data = self.received_messages.pop(id)
                    return self._parse_message(id, data, deserializer, _async=True)
            await asyncio.sleep(0.01)
            if time.time() - t >= timeout:
                break
        raise TimeoutError(f"Timeout waiting for message with id '{id}'")
    # endregion
    
    def stop(self):
        if self._mode == 'receive':
            self.receive_runner_stop_event.set()
            self.receive_runner.join()
    
    def _handle_received_message(self, data: bytes):
        msg_type = data[0:1]
        id = data[1:17].rstrip(b'\0').decode('utf-8')
        if msg_type == _MSG_STREAM or msg_type == _MSG_STREAM_END:
            field_name = data[17:145].rstrip(b'\0').decode('utf-8')
            key = f"{id}:{field_name}"
            if msg_type == _MSG_STREAM_END:
                content = _end_stream
            else:
                content = data[145:]
            if key not in self.received_stream_messages:
                self.received_stream_messages[key] = (time.time(), ThreadQueue())
            self.received_stream_messages[key][1].put(content)  # type: ignore
        else:
            # normal or custom message
            self.received_messages[id] = (time.time(), data)
    
    def _runner(self):
        last_expire_check = time.time()
        while not self.receive_runner_stop_event.is_set():
            try:
                output = self.message_queue.get(block=False)
                self._handle_received_message(output)
            except EmptyQueueException:
                pass
            # check expire
            if not self._checking_expire and (time.time() - last_expire_check >= self._check_expire_interval_secs):
                self._checking_expire = True
                last_expire_check = time.time()
                now = time.time()
                for id, (ts, _) in tuple(self.received_messages.items()):
                    if now - ts >= self._expire_timeout_secs:
                        try:
                            del self.received_messages[id]
                        except KeyError:
                            pass
                for id_field, (ts, _) in tuple(self.received_stream_messages.items()):
                    if now - ts >= self._expire_timeout_secs:
                        try:
                            del self.received_stream_messages[id_field]
                        except KeyError:
                            pass
                self._checking_expire = False
            time.sleep(0.01)
            
    def __del__(self):
        self.stop()
        
        
__all__ = ['MessagePool']