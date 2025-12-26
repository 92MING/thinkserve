import os
import requests
import base64

from io import BytesIO
from pathlib import Path
from typing import Literal, TypeAlias, BinaryIO, TypeVar, overload

_SourceType: TypeAlias = Literal['path', 'url', 'base64', 'bytes']
_T = TypeVar('_T', bound=BinaryIO)

def detect_source_type(data: str|bytes|Path) -> _SourceType:
    """Detect the source type of the given data string."""
    if isinstance(data, str):
        if data.startswith('data:'):
            return 'base64'
        url_prefixes = ('http://', 'https://', 'ftp://', 'ftps://')
        for prefix in url_prefixes:
            if data.startswith(prefix):
                return 'url'
        if '/' in data or '\\' in data:
            if os.path.exists(data):
                return 'path'
        elif (' ' not in data) and (len(data) % 4 == 0):
            return 'base64'
    elif isinstance(data, bytes):
        return 'bytes'
    elif isinstance(data, Path):
        return 'path'
    raise ValueError("Unable to detect source type from the given data.")

@overload
def load_file_data(
    source: str|bytes|Path,
    max_size: int|None=None,
    timeout: int|float|None=None,
)->BinaryIO: ...
    
@overload
def load_file_data(
    source: str|bytes|Path,
    io: _T|None=None,
    max_size: int|None=None,
    timeout: int|float|None=None,
)->_T: ...

def load_file_data(     # type: ignore
    source: str|bytes|Path,
    io: BinaryIO|None=None,
    max_size: int|None=None,
    timeout: int|float|None=None,
):
    source_type = detect_source_type(source)
    if io is None:
        io = BytesIO()
    size = 0
    if source_type == 'path':
        with open(source, 'rb') as f:
            while True:
                chunk = f.read(4096)
                if not chunk:
                    break
                io.write(chunk)
                size += len(chunk)
                if max_size is not None and size > max_size:
                    raise ValueError("File size exceeds maximum allowed size.")
    elif source_type == 'url':
        response = requests.get(source, stream=True, timeout=timeout)   # type: ignore
        response.raise_for_status()
        for chunk in response.iter_content(chunk_size=4096):
            if not chunk:
                break
            io.write(chunk)
            size += len(chunk)
            if max_size is not None and size > max_size:
                raise ValueError("File size exceeds maximum allowed size.")
    elif source_type == 'base64':
        if isinstance(source, str):
            _, encoded = source.split(',', 1) if ',' in source else ('', source)
            decoded = base64.b64decode(encoded)
            if max_size is not None and len(decoded) > max_size:
                raise ValueError("File size exceeds maximum allowed size.")
            io.write(decoded)
        else:
            raise TypeError("Base64 source must be a string.")
    elif source_type == 'bytes':
        if isinstance(source, bytes):
            if max_size is not None and len(source) > max_size:
                raise ValueError("File size exceeds maximum allowed size.")
            io.write(source)
        else:
            raise TypeError("Bytes source must be of type bytes.")
    else:
        raise ValueError("Unsupported source type.")
    if isinstance(io, BytesIO):
        io.seek(0)
    return io

__all__ = ['detect_source_type', 'load_file_data']