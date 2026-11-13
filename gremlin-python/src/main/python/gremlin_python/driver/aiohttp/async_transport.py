#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
import asyncio
import sys
import aiohttp

from gremlin_python.driver.aiohttp.transport import (
    DEFAULT_COMPRESSION,
    DEFAULT_CONNECT_TIMEOUT_MILLIS,
    DEFAULT_IDLE_TIMEOUT_MILLIS,
    DEFAULT_KEEP_ALIVE_TIME_MILLIS,
    _keep_alive_socket_factory,
    _normalize_compression,
    _resolve_timeout_seconds,
)
from gremlin_python.driver.exceptions import ReadTimeoutError

if sys.version_info >= (3, 11):
    import asyncio as async_timeout
else:
    import async_timeout


class _NeedMoreData(Exception):
    """Internal signal raised by :class:`AsyncBufferedStream` when a synchronous
    ``read(n)`` call from ``GraphBinaryReader`` requires more bytes than are
    currently buffered in memory and the underlying HTTP stream has not yet
    reached EOF."""

    def __init__(self, missing: int):
        super().__init__(missing)
        self.missing = missing


class AsyncBufferedStream:
    """Wraps aiohttp's async ``StreamReader`` with an in-memory buffer and
    lightweight position checkpointing.

    ``GraphBinaryReader.to_object(stream)`` performs many small synchronous
    ``stream.read(n)`` calls per object. Rather than duplicating all
    GraphBinary deserializers as ``async def`` coroutines, this stream:

    1. Pulls HTTP response chunks asynchronously in 64 KB blocks on the caller's
       running event loop (:meth:`fill_more`).
    2. Serves ``read(n)`` synchronously from the in-memory ``bytearray`` buffer.
    3. Raises :class:`_NeedMoreData` if an object straddles a chunk boundary so
       ``deserialize_async_response_stream`` can rewind to the last
       :meth:`checkpoint`, ``await fill_more()``, and retry once more bytes
       have arrived.
    4. Raises :class:`asyncio.IncompleteReadError` once the underlying HTTP
       response reaches EOF without satisfying ``read(n)``.
    """

    _FILL_SIZE = 64 * 1024

    def __init__(self, response, read_timeout=None):
        self._response = response
        self._read_timeout = read_timeout
        self._buf = bytearray()
        self._pos = 0
        self._eof = False

    def checkpoint(self) -> int:
        """Return the current read offset so the caller can rewind if needed."""
        return self._pos

    def restore(self, pos: int) -> None:
        """Rewind the read offset to a previously saved checkpoint."""
        self._pos = pos

    def compact(self) -> None:
        """Discard already-consumed bytes to keep buffer memory bounded."""
        if self._pos == len(self._buf):
            self._buf.clear()
            self._pos = 0
        elif self._pos >= self._FILL_SIZE:
            del self._buf[:self._pos]
            self._pos = 0

    def read(self, n: int) -> bytes:
        """Synchronous read used by ``GraphBinaryReader.to_object``."""
        if n <= 0:
            return b""
        available = len(self._buf) - self._pos
        if available < n:
            if self._eof:
                partial = bytes(self._buf[self._pos:])
                self._buf.clear()
                self._pos = 0
                raise asyncio.IncompleteReadError(partial=partial, expected=n)
            raise _NeedMoreData(n - available)
        end = self._pos + n
        out = bytes(self._buf[self._pos:end])
        self._pos = end
        return out

    async def read_exactly(self, n: int) -> bytes:
        """Async helper that guarantees ``n`` bytes are buffered and returns them."""
        if n <= 0:
            return b""
        while len(self._buf) - self._pos < n and not self._eof:
            await self.fill_more(n - (len(self._buf) - self._pos))
        out = self.read(n)
        self.compact()
        return out

    async def fill_more(self, min_bytes: int = 1) -> None:
        """Pull chunks from the aiohttp response until at least ``min_bytes``
        new bytes have been appended to the buffer or EOF is reached."""
        appended = 0
        while appended < min_bytes and not self._eof:
            chunk = await self._read_chunk()
            if not chunk:
                self._eof = True
                break
            self._buf.extend(chunk)
            appended += len(chunk)

    async def _read_chunk(self) -> bytes:
        try:
            return await self._response.content.read(self._FILL_SIZE)
        except (aiohttp.ServerTimeoutError, asyncio.TimeoutError) as err:
            raise ReadTimeoutError(
                f"Read timed out after {self._read_timeout}s waiting for response data."
            ) from err


class AsyncAiohttpHTTPTransport:
    """Native ``asyncio`` HTTP transport for Gremlin Server 4.x using ``aiohttp``.

    Unlike :class:`~gremlin_python.driver.aiohttp.transport.AiohttpHTTPTransport`,
    this transport attaches directly to the caller's running event loop, never
    creates private event loops, and never invokes ``nest_asyncio``.
    """

    def __init__(
        self,
        write_timeout=None,
        connect_timeout_millis=None,
        connect_timeout=None,
        idle_timeout_millis=None,
        idle_timeout=None,
        read_timeout_millis=None,
        read_timeout=None,
        keep_alive_time_millis=None,
        keep_alive_time=None,
        compression=DEFAULT_COMPRESSION,
        proxy=None,
        trust_env=False,
        max_connections=None,
        **kwargs,
    ):
        # Strip legacy/sync-only call_from_event_loop kwarg if passed by caller
        kwargs.pop("call_from_event_loop", None)

        self._client_session = None
        self._http_req_resp = None
        self._enable_ssl = False
        self._ssl_context = None
        self._url = None

        self._aiohttp_kwargs = kwargs
        self._write_timeout = write_timeout

        self._read_timeout = _resolve_timeout_seconds(
            read_timeout_millis, read_timeout, None
        )
        self._connect_timeout = _resolve_timeout_seconds(
            connect_timeout_millis, connect_timeout, DEFAULT_CONNECT_TIMEOUT_MILLIS
        )
        self._idle_timeout = _resolve_timeout_seconds(
            idle_timeout_millis, idle_timeout, DEFAULT_IDLE_TIMEOUT_MILLIS
        )
        self._keep_alive_time = _resolve_timeout_seconds(
            keep_alive_time_millis, keep_alive_time, DEFAULT_KEEP_ALIVE_TIME_MILLIS
        )
        self._max_connections = max_connections
        self._compression = _normalize_compression(compression)
        self._proxy = proxy
        self._trust_env = trust_env

        if "ssl" in self._aiohttp_kwargs:
            self._ssl_context = self._aiohttp_kwargs.pop("ssl")
            self._enable_ssl = True

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def connect(self, url, headers=None):
        """Initialize the underlying :class:`aiohttp.ClientSession` on the
        currently running event loop."""
        self._url = url
        connector_kwargs = {}
        if self._enable_ssl:
            connector_kwargs["ssl_context"] = self._ssl_context
        if self._idle_timeout is not None:
            connector_kwargs["keepalive_timeout"] = self._idle_timeout
        if self._keep_alive_time is not None:
            self._apply_keep_alive(connector_kwargs)
        if self._max_connections is not None:
            connector_kwargs["limit"] = self._max_connections

        session_kwargs = {
            "headers": headers,
            "trust_env": self._trust_env,
        }
        timeout_kwargs = {}
        if self._connect_timeout is not None:
            timeout_kwargs["sock_connect"] = self._connect_timeout
        if self._read_timeout is not None:
            timeout_kwargs["sock_read"] = self._read_timeout
        if timeout_kwargs:
            session_kwargs["timeout"] = aiohttp.ClientTimeout(**timeout_kwargs)
        if connector_kwargs:
            session_kwargs["connector"] = aiohttp.TCPConnector(**connector_kwargs)

        self._client_session = aiohttp.ClientSession(**session_kwargs)

    def _apply_keep_alive(self, connector_kwargs):
        connector_kwargs["socket_factory"] = _keep_alive_socket_factory(
            self._keep_alive_time
        )

    async def write(self, message):
        """Send an HTTP POST request with ``message['payload']`` and ``message['headers']``."""
        headers = message["headers"]
        has_accept_encoding = any(k.lower() == "accept-encoding" for k in headers)
        skip_auto_headers = None
        if self._compression == "deflate":
            if not has_accept_encoding:
                headers["accept-encoding"] = "deflate"
        elif not has_accept_encoding:
            skip_auto_headers = ["Accept-Encoding"]

        post_kwargs = dict(self._aiohttp_kwargs)
        if self._proxy is not None:
            post_kwargs["proxy"] = self._proxy
        if skip_auto_headers is not None:
            post_kwargs["skip_auto_headers"] = skip_auto_headers

        async with async_timeout.timeout(self._write_timeout):
            self._http_req_resp = await self._client_session.post(
                url=self._url,
                data=message["payload"],
                headers=headers,
                **post_kwargs,
            )

    def get_stream(self) -> AsyncBufferedStream:
        """Return an :class:`AsyncBufferedStream` for the current HTTP response."""
        return AsyncBufferedStream(self._http_req_resp, self._read_timeout)

    @property
    def content_type(self) -> str:
        """Return the ``Content-Type`` header of the HTTP response."""
        if self._http_req_resp is not None:
            return self._http_req_resp.headers.get("content-type", "")
        return ""

    @property
    def status_code(self):
        """Return the HTTP status code of the response."""
        if self._http_req_resp is not None:
            return self._http_req_resp.status
        return None

    async def read_body(self) -> bytes:
        """Read the entire HTTP response body as bytes."""
        try:
            return await self._http_req_resp.read()
        except (aiohttp.ServerTimeoutError, asyncio.TimeoutError) as err:
            raise ReadTimeoutError(
                f"Read timed out after {self._read_timeout}s waiting for response data."
            ) from err

    def evict_response(self) -> None:
        """Close the current HTTP response and evict its connection from aiohttp's pool."""
        if self._http_req_resp is not None:
            self._http_req_resp.close()
            self._http_req_resp = None

    async def close(self) -> None:
        """Close the underlying :class:`aiohttp.ClientSession`."""
        if self._client_session is not None and not self._client_session.closed:
            await self._client_session.close()
            self._client_session = None

    @property
    def closed(self) -> bool:
        return self._client_session is None or self._client_session.closed
