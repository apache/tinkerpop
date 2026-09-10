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
from aiohttp import ClientResponseError

from gremlin_python.driver.transport import AbstractBaseTransport

if sys.version_info >= (3, 11):
    from asyncio import timeout as _async_timeout
else:
    from async_timeout import timeout as _async_timeout

__author__ = 'Apache TinkerPop (dev@tinkerpop.apache.org)'


class AsyncAiohttpWSTransport(AbstractBaseTransport):
    """Native-async WebSocket transport built on aiohttp.

    Unlike :class:`~gremlin_python.driver.aiohttp.transport.AiohttpTransport`,
    every I/O method is a coroutine.  The transport does **not** create its own
    event loop; callers must already be running inside one (``async def``
    contexts, ``asyncio.run()``, etc.).

    Parameters
    ----------
    read_timeout:
        Maximum seconds to wait for a WebSocket frame.  ``None`` disables the
        timeout.
    write_timeout:
        Maximum seconds to wait when sending a frame.  ``None`` disables the
        timeout.
    **kwargs:
        Additional keyword arguments forwarded verbatim to
        :meth:`aiohttp.ClientSession.ws_connect`.  ``max_content_length`` is
        transparently remapped to ``max_msg_size``; ``ssl_options`` is
        remapped to ``ssl``.
    """

    def __init__(self, read_timeout=None, write_timeout=None, **kwargs):
        self._websocket = None
        self._client_session = None
        self._read_timeout = read_timeout
        self._write_timeout = write_timeout
        self._aiohttp_kwargs = kwargs
        if "max_content_length" in self._aiohttp_kwargs:
            self._aiohttp_kwargs["max_msg_size"] = self._aiohttp_kwargs.pop(
                "max_content_length"
            )
        if "ssl_options" in self._aiohttp_kwargs:
            self._aiohttp_kwargs["ssl"] = self._aiohttp_kwargs.pop("ssl_options")

    async def connect(self, url, headers=None):
        """Open the WebSocket connection to *url*.

        Parameters
        ----------
        url:
            WebSocket endpoint, e.g. ``ws://localhost:8182/gremlin``.
        headers:
            Optional extra HTTP headers sent during the WebSocket handshake.
        """
        self._client_session = aiohttp.ClientSession()
        try:
            self._websocket = await self._client_session.ws_connect(
                url, headers=headers, **self._aiohttp_kwargs
            )
        except ClientResponseError as err:
            if err.status == 403:
                raise Exception(
                    "Failed to connect to server: HTTP Error code 403 - Forbidden."
                )
            raise

    async def write(self, message):
        """Send *message* (bytes) as a WebSocket binary frame."""
        async with _async_timeout(self._write_timeout):
            await self._websocket.send_bytes(message)

    async def read(self):
        """Receive the next WebSocket message and return its payload as bytes.

        Text frames are UTF-8 encoded before returning.  Close/error frames
        raise :class:`RuntimeError`.
        """
        async with _async_timeout(self._read_timeout):
            msg = await self._websocket.receive()

        if msg.type == aiohttp.WSMsgType.CLOSE:
            await self.close()
            raise RuntimeError("Connection was closed by server.")
        if msg.type == aiohttp.WSMsgType.CLOSED:
            raise RuntimeError("Connection was already closed.")
        if msg.type == aiohttp.WSMsgType.ERROR:
            raise RuntimeError("Received error on read: '" + str(msg.data) + "'")
        if msg.type == aiohttp.WSMsgType.TEXT:
            return msg.data.strip().encode("utf-8")
        return msg.data

    async def ping(self):
        """Send a WebSocket ping frame to keep the connection alive."""
        await self._websocket.ping()

    async def close(self):
        """Close the WebSocket and the underlying HTTP session gracefully.

        Both resources are closed concurrently and each close is shielded from
        external cancellation so that the connection is always cleaned up even
        if the caller's coroutine is cancelled.
        """
        tasks = []
        if self._websocket is not None and not self._websocket.closed:
            tasks.append(asyncio.ensure_future(self._websocket.close()))
        if self._client_session is not None and not self._client_session.closed:
            tasks.append(asyncio.ensure_future(self._client_session.close()))
        if tasks:
            await asyncio.gather(*[asyncio.shield(t) for t in tasks])

    @property
    def closed(self):
        """``True`` if the WebSocket or the underlying session is closed."""
        return (
            self._websocket is None
            or self._websocket.closed
            or self._client_session is None
            or self._client_session.closed
        )
