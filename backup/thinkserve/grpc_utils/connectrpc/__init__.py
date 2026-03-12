# Copyright 2025 Gaudiy Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# SPDX-License-Identifier: Apache-2.0

"""Connect-Python: A Python implementation of the Connect protocol."""

from .call_options import CallOptions
from .client import Client, ClientConfig
from .code import Code
from .codec import Codec, ProtoBinaryCodec, ProtoJSONCodec
from .compression import Compression, GZipCompression
from .connect import (
    Peer,
    Spec,
    StreamingClientConn,
    StreamingHandlerConn,
    StreamRequest,
    StreamResponse,
    StreamType,
    UnaryRequest,
    UnaryResponse,
)
from .content_stream import AsyncByteStream
from .error import ConnectError
from .handler import Handler
from .handler_context import HandlerContext
from .headers import Headers
from .idempotency_level import IdempotencyLevel
from .middleware import ConnectMiddleware
from .options import ClientOptions, HandlerOptions
from .protocol import Protocol
from .request import Request
from .response import Response as HTTPResponse
from .response import StreamingResponse
from .response_writer import ServerResponseWriter
from .version import __version__

__all__ = [
    "__version__",
    "AsyncByteStream",
    "CallOptions",
    "Client",
    "ClientConfig",
    "ClientOptions",
    "Code",
    "Codec",
    "Compression",
    "ConnectError",
    "ConnectMiddleware",
    "HandlerOptions",
    "GZipCompression",
    "Handler",
    "HandlerContext",
    "Headers",
    "HTTPResponse",
    "IdempotencyLevel",
    "Peer",
    "Protocol",
    "ProtoBinaryCodec",
    "ProtoJSONCodec",
    "Request",
    "ServerResponseWriter",
    "Spec",
    "StreamingClientConn",
    "StreamingHandlerConn",
    "StreamingResponse",
    "StreamRequest",
    "StreamResponse",
    "StreamType",
    "UnaryRequest",
    "UnaryResponse",
]
