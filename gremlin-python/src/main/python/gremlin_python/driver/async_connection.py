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
import logging
import uuid

from gremlin_python.driver.async_resultset import AsyncResultSet
from gremlin_python.driver.connection import Connection

log = logging.getLogger("gremlinpython")

__author__ = 'Apache TinkerPop (dev@tinkerpop.apache.org)'


class AsyncConnection(Connection):
    """Native-async WebSocket connection.

    All I/O methods are coroutines.  No :class:`~concurrent.futures.ThreadPoolExecutor`
    is created or used; pass ``executor=None`` when constructing directly.

    The pool must be an :class:`asyncio.Queue` (provided by
    :class:`~gremlin_python.driver.async_client.AsyncClient`).

    Parameters
    ----------
    url:
        WebSocket endpoint.
    traversal_source:
        Gremlin traversal source name (e.g. ``"g"``).
    protocol:
        An :class:`~gremlin_python.driver.async_protocol.AsyncGremlinServerWSProtocol`
        instance.
    transport_factory:
        Zero-argument callable that returns an
        :class:`~gremlin_python.driver.aiohttp.async_transport.AsyncAiohttpWSTransport`.
    executor:
        Ignored; present for API compatibility with
        :class:`~gremlin_python.driver.connection.Connection`.
    pool:
        The :class:`asyncio.Queue` that owns this connection.
    headers:
        Extra HTTP headers sent during the WebSocket handshake.
    enable_user_agent_on_connect:
        Whether to send the driver user-agent header.
    """

    def __init__(self, url, traversal_source, protocol, transport_factory,
                 executor, pool, headers=None, enable_user_agent_on_connect=True):
        super().__init__(
            url, traversal_source, protocol, transport_factory,
            executor, pool, headers=headers,
            enable_user_agent_on_connect=enable_user_agent_on_connect,
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self):
        """Open the WebSocket connection."""
        if self._transport:
            await self._transport.close()
        self._transport = self._transport_factory()
        await self._transport.connect(self._url, self._headers)
        self._protocol.connection_made(self._transport)
        self._inited = True

    async def close(self):
        """Close the underlying WebSocket transport."""
        if self._inited:
            await self._transport.close()

    # ------------------------------------------------------------------
    # I/O
    # ------------------------------------------------------------------

    async def write(self, request_message):
        """Send *request_message* and return an :class:`~gremlin_python.driver.async_resultset.AsyncResultSet`.

        Lazily connects on the first call.  Returns as soon as the first
        response chunk arrives; a background :class:`asyncio.Task` consumes
        any remaining chunks for partial (206) responses.
        """
        if not self._inited:
            await self.connect()

        if request_message.args.get("requestId"):
            request_id = str(request_message.args["requestId"])
            uuid.UUID(request_id)  # validate UUID format
        else:
            request_id = str(uuid.uuid4())

        result_set = AsyncResultSet(asyncio.Queue(), request_id)
        self._results[request_id] = result_set

        await self._protocol.write(request_id, request_message)
        return await self._receive(result_set)

    async def _receive(self, result_set):
        """Read the first response chunk and set up background consumption.

        For non-partial responses (status != 206) the connection is returned
        to the pool immediately.  For partial responses a background task
        consumes the remaining chunks and returns the connection when done.
        """
        try:
            data = await self._transport.read()
            status_code = await self._protocol.data_received(data, self._results)
        except Exception:
            self._inited = False
            self._pool.put_nowait(self)
            raise

        if status_code != 206:
            # All data received; mark the result set as complete and recycle.
            fut = asyncio.get_running_loop().create_future()
            fut.set_result(None)
            result_set.future = fut
            self._pool.put_nowait(self)
        else:
            # Partial response: consume remaining chunks in the background.
            async def _consume():
                try:
                    while True:
                        chunk = await self._transport.read()
                        sc = await self._protocol.data_received(chunk, self._results)
                        if sc != 206:
                            break
                finally:
                    self._pool.put_nowait(self)

            result_set.future = asyncio.create_task(_consume())

        return result_set
