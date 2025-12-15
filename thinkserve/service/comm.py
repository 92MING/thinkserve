import os
import socket
import inspect
import asyncio
import struct
import pickle
import random
import logging

if __name__.endswith('main__'): # for debugging
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s | %(levelname)s | %(message)s')

from threading import Thread
from dataclasses import dataclass
from typing_extensions import TypeAliasType
from typing import Protocol, Awaitable, Any, Callable, ParamSpec, TypeVar, overload, get_origin, Coroutine

_P = ParamSpec('_P')
_T = TypeVar('_T')
_SyncOrAsyncFunc = TypeAliasType('_SyncOrAsyncFunc', Callable[_P, Awaitable[_T]]|Callable[_P, _T], type_params=(_P, _T))
_logger = logging.getLogger(__name__)

@dataclass
class P2PMessage:
    id: str
    event: str
    is_response: bool
    
    # stream info
    is_stream: bool
    is_stream_end: bool
    
    data: bytes
    
    def __post_init__(self):
        assert len(self.id) >0 and len(self.id) <= 32, "Invalid QueueMessage id. It must be 1-32 characters." 
        assert len(self.event) >0 and len(self.event) <= 64, "Invalid QueueMessage event. It must be 1-64 characters."
    
    def dump(self) -> bytes:
        buf = bytearray()
        id_bytes = self.id.encode('utf-8')
        id_bytes += b'\0' * (32 - len(id_bytes))
        buf += id_bytes
        event_bytes = self.event.encode('utf-8')
        event_bytes += b'\0' * (64 - len(event_bytes))
        buf += event_bytes
        flags = 0
        if self.is_response:
            flags |= 0x01
        if self.is_stream:
            flags |= 0x02
        if self.is_stream_end:
            flags |= 0x04
        buf += struct.pack('B', flags)
        buf += self.data
        return bytes(buf)
    
    @classmethod
    def Parse(cls, raw: bytes):
        id_bytes = raw[0:32]
        id_str = id_bytes.split(b'\0', 1)[0].decode('utf-8')
        event_bytes = raw[32:96]
        event_str = event_bytes.split(b'\0', 1)[0].decode('utf-8')
        flags = struct.unpack('B', raw[96:97])[0]
        is_response = (flags & 0x01) != 0
        is_stream = (flags & 0x02) != 0
        is_stream_end = (flags & 0x04) != 0
        data = raw[97:]
        return cls(
            id=id_str,
            event=event_str,
            is_response=is_response,
            is_stream=is_stream,
            is_stream_end=is_stream_end,
            data=data,
        )

class AbstractP2PServer(Protocol):
    '''Abstract protocol for a peer-to-peer message server.'''
    
    async def send(self, data: bytes): 
        '''Send 1 message to another server.'''

    def start(self):
        '''Start the message server.'''
    
    def stop(self):
        '''Stop the message server.'''
        
    def try_reconnect(self) -> bool:  # type: ignore
        '''Try to reconnect to the server. Return True if successful, False otherwise.'''
    
    def set_on_received(self, callback: _SyncOrAsyncFunc[[bytes], Any]): 
        '''Set a callback function to be called when a new message is received.
        The callback function can be either synchronous or asynchronous.
        '''
    def set_on_disconnected(self, callback: _SyncOrAsyncFunc[[], Any]): 
        '''Set a callback function to be called when the server is disconnected.
        The callback function can be either synchronous or asynchronous.
        '''
    def set_on_connected(self, callback: _SyncOrAsyncFunc[[], Any]): 
        '''Set a callback function to be called when the server is connected.
        The callback function can be either synchronous or asynchronous.
        '''

class P2PServerEventMixin:
    
    _on_received: _SyncOrAsyncFunc[[bytes], Any]|None = None
    _on_disconnected: _SyncOrAsyncFunc[[], Any]|None = None
    _on_connected: _SyncOrAsyncFunc[[], Any]|None = None
    _async_on_received: bool = False
    _async_on_disconnected: bool = False
    _async_on_connected: bool = False
    
    @staticmethod
    def _is_async_callable(func):
        if asyncio.iscoroutinefunction(func):
            return True
        if hasattr(func, '__call__'):
            return asyncio.iscoroutinefunction(func.__call__)
        return_anno = inspect.signature(func).return_annotation
        if get_origin(return_anno) in (Coroutine, Awaitable) or (return_anno in (Coroutine, Awaitable)):
            return True
        return False
    
    async def _call_handler(self, func, is_async, *args, **kwargs):
        if is_async:
            return await func(*args, **kwargs)
        else:
            loop = asyncio.get_running_loop()
            # prevent blocking the event loop
            return await loop.run_in_executor(None, func, *args, **kwargs)
    
    async def handle_received(self, data: bytes):
        return await self._call_handler(self._on_received, self._async_on_received, data)
            
    async def handle_disconnected(self):
        return await self._call_handler(self._on_disconnected, self._async_on_disconnected)
        
    async def handle_connected(self):
        return await self._call_handler(self._on_connected, self._async_on_connected)
        
    def set_on_received(self, callback: _SyncOrAsyncFunc[[bytes], Any]):
        self._on_received = callback
        self._async_on_received = self._is_async_callable(callback)
        
    def set_on_disconnected(self, callback: _SyncOrAsyncFunc[[], Any]):
        self._on_disconnected = callback
        self._async_on_disconnected = self._is_async_callable(callback)
        
    def set_on_connected(self, callback: _SyncOrAsyncFunc[[], Any]):
        self._on_connected = callback
        self._async_on_connected = self._is_async_callable(callback)


def _check_port_available(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) != 0        

def _find_available_port(start_port: int=30000, end_port: int=65536) -> int:
    for port in range(start_port, end_port):
        if _check_port_available(port):
            return port
    raise RuntimeError('No available port found.')

class LocalP2PServer(P2PServerEventMixin):
    '''
    P2P server for localhost communication using local sockets.
    This is for inter-process communication on the same machine.
    # TODO: 
    #   - support AF_UNIX on Unix systems
    #   - support AF_PIPES on Windows
    '''
    
    _stop_event: asyncio.Event
    _streaming_chunks: dict[str, bytearray] # {id: bytearray}
    
    _buffer: bytearray
    _target_data_size: int|None = None
    '''target size for the next complete message'''
    
    _port: int|None = None
    _identifier: str|None = None
    
    _started: bool = False
    _DEFAULT_CHUNK_SIZE: int = 1 * 1024 * 1024  # 1 MB
    _chunk_size: int = _DEFAULT_CHUNK_SIZE
    _thread: Thread|None = None
    
    _reader: asyncio.StreamReader|None = None
    _writer: asyncio.StreamWriter|None = None
    _server: asyncio.AbstractServer|None = None
    
    @overload
    def __init__(self, *, port: int, chunk_size: int=_DEFAULT_CHUNK_SIZE): 
        '''connect to or create a local socket server on the given port.'''
    
    @overload
    def __init__(self, *, identifier: str, chunk_size: int=_DEFAULT_CHUNK_SIZE): 
        '''connect to or create a local socket server with the given identifier.'''
    
    def __init__(self, *, port: int|None=None, identifier: str|None=None, chunk_size: int=_DEFAULT_CHUNK_SIZE):
        if (port is None) == (identifier is None):
            raise ValueError('Either port or identifier must be provided, but not both.')
        if port is not None:
            assert isinstance(port, int) and port >0 and port <65536, 'Port must be an integer between 1 and 65535.'
            assert _check_port_available(port), f'Port {port} is already in use.'
            self._port = port
        else:
            assert isinstance(identifier, str) and len(identifier) >0, 'Identifier must be a non-empty string.'
            assert identifier.isidentifier(), 'Identifier must be a valid identifier.'
            self._identifier = identifier
        self._chunk_size = chunk_size
        self._buffer = bytearray()
        self._stop_event = asyncio.Event()
        self._streaming_chunks = {}
    
    async def send(self, data: bytes):
        '''
        Send 1 message to another server.
        Message format:
        [4 bytes size prefix][8 bytes id][1 bytes is_end][data bytes]
        '''
        if self._writer is None:
            raise RuntimeError('P2P server is not connected.')
        id = ''.join(random.choices('abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=8))
        id_bytes = id.encode('utf-8')
        id_bytes += b'\0' * (8 - len(id_bytes))
        for i in range(0, len(data), self._chunk_size):
            chunk = data[i:i + self._chunk_size]
            is_end = 1 if i + self._chunk_size >= len(data) else 0
            header = struct.pack('I8sB', len(chunk) +9, id_bytes, is_end)
            self._writer.write(header + chunk)
            await self._writer.drain()
    
    def stop(self):
        self._started = False
        self._stop_event.set()
        self._streaming_chunks.clear()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5.0)
    
    def start(self):
        if self._started:
            return
        
        self._started = True
        self._stop_event.clear()
        
        self._thread = Thread(target=lambda: asyncio.run(self._start()), daemon=True)
        self._thread.start()
        
    async def _server_loop(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self._reader, self._writer = reader, writer
        await self.handle_connected()
        try:
            while not self._stop_event.is_set():
                try:
                    self._buffer += await reader.read(self._chunk_size)
                except ConnectionError: # connection lost
                    break
                if len(self._buffer) == 0:
                    await asyncio.sleep(0.0)
                    continue
                
                if self._target_data_size is None:
                    # first 4 bytes indicate the message size
                    self._target_data_size = struct.unpack('I', self._buffer[0:4])[0]
                    
                while self._target_data_size is not None and len(self._buffer) -4 >= self._target_data_size:
                    message_data = self._buffer[4:4 + self._target_data_size]
                    id = message_data[0:8].split(b'\0', 1)[0].decode('utf-8')
                    is_end = struct.unpack('B', message_data[8:9])[0]
                    message_data = message_data[9:]
                    current_chunks: bytearray|None = self._streaming_chunks.get(id, None)
                    
                    if is_end == 1:
                        if current_chunks is not None:
                            message_data = bytes(current_chunks + message_data)
                            del self._streaming_chunks[id]
                        asyncio.create_task(self.handle_received(message_data)) # handle in background
                    else:
                        if current_chunks is None:
                            current_chunks = bytearray()
                            self._streaming_chunks[id] = current_chunks
                        current_chunks += message_data
                        
                    self._buffer = self._buffer[4 + self._target_data_size:]
                    if len(self._buffer) >= 4:
                        self._target_data_size = struct.unpack('I', self._buffer[0:4])[0]
                    else:
                        self._target_data_size = None
                
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass
        finally:
            await self.handle_disconnected()
    
    async def _start(self):
        if self._port is not None:
            if not _check_port_available(self._port):
                # already started by another process
                _logger.debug(f'Trying to connect peer server on port {self._port}...')
                reader, writer = await asyncio.open_connection('localhost', self._port)
                await self._server_loop(reader, writer)
            else:
                _logger.debug(f'Port {self._port} is available, starting a new server...')
                self._server = await asyncio.start_server(self._server_loop, 'localhost', self._port)
                async with self._server:
                    await self._server.serve_forever()
        else:
            raise NotImplementedError('LocalP2PServer with identifier is not implemented yet.')
    
    def __del__(self):
        self.stop()

if __name__.endswith('main__'):
    def _test_local_p2p_server(messages: list[object], port: int):
        import time
        server = LocalP2PServer(port=port)
        
        async def on_received(data: bytes):
            msg = pickle.loads(data)
            print(f'|{os.getpid()}| received: {msg}')
            if not isinstance(msg, str) or not msg.startswith('ECHO:'):
                await server.send(pickle.dumps(f'ECHO: {msg}'))
            
        async def on_connected():
            print(f'|{os.getpid()}| connected.')
            for msg in messages:
                print(f'|{os.getpid()}| sending: {msg}')
                await server.send(pickle.dumps(msg))
        
        def on_disconnected():
            print(f'|{os.getpid()}| disconnected.')
        
        server.set_on_received(on_received)
        server.set_on_connected(on_connected)
        server.set_on_disconnected(on_disconnected)

        server.start()
        print(f'|{os.getpid()}| LocalP2PServer started on port {port}.')
        time.sleep(5)
        
if __name__ == '__main__':
    import time
    from multiprocessing import Process
    
    test_port = _find_available_port()
    messages1 = [f'Hello from process 1 - {i}' for i in range(5)]
    messages2 = [f'Hello from process 2 - {i}' for i in range(5)]
    p1 = Process(target=_test_local_p2p_server, args=(messages1, test_port))
    p2 = Process(target=_test_local_p2p_server, args=(messages2, test_port))
    
    p1.start()
    time.sleep(0.5)
    p2.start()
    time.sleep(5)
    print('Main process exiting...')