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
"""Experimental API. This API may have breaking changes or be removed in a subsequent minor release.

Native-async WebSocket connection managed by AsyncClient.
"""
import asyncio
import logging
import uuid

from gremlin_python.driver import useragent
from gremlin_python.driver.async_resultset import AsyncResultSet

log = logging.getLogger("gremlinpython")


class AsyncConnection:
    """Experimental API. This API may have breaking changes or be removed in a subsequent minor release.

    Native-async WebSocket connection managed by AsyncClient.

    Parameters
    ----------
    url:
        WebSocket endpoint.
    traversal_source:
        Gremlin traversal source name (e.g. "g").
    protocol:
        An AsyncGremlinServerWSProtocol instance.
    transport_factory:
        Zero-argument callable returning an async transport implementing
        AbstractBaseTransport (such as AsyncAiohttpWSTransport).
    pool:
        The asyncio.Queue that owns this connection.
    headers:
        Mutable mapping (e.g. ``dict[str, str]``) of extra HTTP headers sent
        during the WebSocket handshake.  List-of-pair headers are not
        supported.
    enable_user_agent_on_connect:
        Whether to send the driver user-agent header.
    """

    def __init__(self, url, traversal_source, protocol, transport_factory,
                 pool, headers=None, enable_user_agent_on_connect=True):
        self._url = url
        self._headers = headers
        self._traversal_source = traversal_source
        self._protocol = protocol
        self._transport_factory = transport_factory
        self._transport = None
        self._pool = pool
        self._results = {}
        self._inited = False
        if enable_user_agent_on_connect:
            self._add_header(useragent.userAgentHeader, useragent.userAgent)
        self._receive_task = None

    def _add_header(self, key, value):
        if self._headers is None:
            self._headers = dict()
        if key.lower() not in [k.lower() for k in self._headers.keys()]:
            self._headers[key] = value

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self):
        """Experimental API. This API may have breaking changes or be removed in a subsequent minor release.

        Open (or re-open) the WebSocket connection.
        """
        if self._transport is not None:
            await self._transport.close()
        self._transport = self._transport_factory()
        await self._transport.connect(self._url, self._headers)
        self._protocol.connection_made(self._transport)
        self._inited = True

    async def close(self):
        """Experimental API. This API may have breaking changes or be removed in a subsequent minor release.

        Close the connection and stop its background tasks.
        """
        await asyncio.shield(self._close())

    async def _close(self):
        if self._receive_task is not None:
            self._receive_task.cancel()
            await asyncio.gather(self._receive_task, return_exceptions=True)
        try:
            if self._transport is not None:
                await self._transport.close()
        finally:
            self._inited = False
            self._results.clear()

    # ------------------------------------------------------------------
    # I/O
    # ------------------------------------------------------------------

    async def write(self, request_message):
        """Experimental API. This API may have breaking changes or be removed in a subsequent minor release.

        Send *request_message* and return an AsyncResultSet.

        Lazily connects on the first call (or if the transport was closed
        while idle).  Returns as soon as the first response chunk arrives; a
        background asyncio.Task drains any remaining partial (206) chunks.
        """
        try:
            if not self._inited or self._transport is None or self._transport.closed:
                await self.connect()

            if request_message.args.get("requestId"):
                request_id = str(request_message.args["requestId"])
                uuid.UUID(request_id)
            else:
                request_id = str(uuid.uuid4())

            result_set = AsyncResultSet(asyncio.Queue(), request_id)
            self._results[request_id] = result_set

            await self._protocol.write(request_id, request_message)
        except BaseException:
            self._inited = False
            self._results.clear()
            self._pool.put_nowait(self)
            raise

        return await self._receive(result_set)

    async def _receive(self, result_set):
        """Read the first response chunk and set up background consumption."""
        try:
            data = await self._transport.read()
            status_code = await self._protocol.data_received(data, self._results)
        except BaseException:
            self._inited = False
            self._results.clear()
            self._pool.put_nowait(self)
            raise

        if status_code != 206:
            fut = asyncio.get_running_loop().create_future()
            fut.set_result(None)
            result_set.future = fut
            self._pool.put_nowait(self)
        else:
            async def _consume():
                while True:
                    chunk = await self._transport.read()
                    sc = await self._protocol.data_received(chunk, self._results)
                    if sc != 206:
                        break

            def release_connection(future):
                # A done callback also runs if cancellation prevents _consume
                # from ever entering its coroutine body.
                if future.cancelled() or future.exception() is not None:
                    self._inited = False
                    self._results.clear()
                self._receive_task = None
                self._pool.put_nowait(self)

            result_set.future = asyncio.create_task(_consume())
            self._receive_task = result_set.future
            result_set.future.add_done_callback(release_connection)

        return result_set
