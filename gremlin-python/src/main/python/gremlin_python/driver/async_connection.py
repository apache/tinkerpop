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


class InvalidatedConnectionError(Exception):
    """Raised when a connection was reset during a request.

    :class:`~gremlin_python.driver.async_client.AsyncClient` catches this and
    retries the request on a fresh connection.
    """


class AsyncConnection(Connection):
    """Native-async WebSocket connection with keepalive and auto-reconnect.

    Features beyond the sync :class:`~gremlin_python.driver.connection.Connection`:

    * **Ping keepalive** — a background :class:`asyncio.Task` sends a WebSocket
      ping every *ping_interval* seconds to prevent the server or intermediate
      proxies from closing idle connections.
    * **Auto-reconnect** — a ``ConnectionResetError`` during *write* triggers a
      single reconnect + retry before giving up.
    * **Invalidation signal** — a ``ConnectionResetError`` during *read* raises
      :class:`InvalidatedConnectionError` so the client can transparently retry
      the request on another connection.

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
        Zero-argument callable returning an
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
    ping_interval:
        Seconds between WebSocket ping frames (default 60, 0 to disable).
    """

    def __init__(self, url, traversal_source, protocol, transport_factory,
                 executor, pool, headers=None, enable_user_agent_on_connect=True,
                 ping_interval=60):
        super().__init__(
            url, traversal_source, protocol, transport_factory,
            executor, pool, headers=headers,
            enable_user_agent_on_connect=enable_user_agent_on_connect,
        )
        self._ping_interval = ping_interval
        self._ping_task = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self):
        """Open (or re-open) the WebSocket connection."""
        if self._ping_task is not None:
            self._ping_task.cancel()
            self._ping_task = None
        if self._transport is not None:
            await self._transport.close()
        self._transport = self._transport_factory()
        await self._transport.connect(self._url, self._headers)
        self._protocol.connection_made(self._transport)
        self._inited = True
        if self._ping_interval > 0:
            self._ping_task = asyncio.create_task(self._ping_forever())

    async def close(self):
        """Close the connection, cancelling the ping task first."""
        if self._ping_task is not None:
            self._ping_task.cancel()
            self._ping_task = None
        if self._inited:
            # asyncio.shield protects the close coroutine from cancellation so
            # the underlying transport is always cleanly shut down.
            await asyncio.shield(self._transport.close())

    # ------------------------------------------------------------------
    # Keepalive
    # ------------------------------------------------------------------

    async def _ping_forever(self):
        """Send periodic WebSocket ping frames until cancelled or the transport dies."""
        while True:
            try:
                await asyncio.sleep(self._ping_interval)
                await self._transport.ping()
            except asyncio.CancelledError:
                break
            except Exception:
                # Transport is dead; mark the connection as needing reconnect.
                log.debug("Ping failed on %s — marking connection as uninitialised", self._url)
                self._inited = False
                break

    # ------------------------------------------------------------------
    # I/O
    # ------------------------------------------------------------------

    async def write(self, request_message):
        """Send *request_message* and return an :class:`~gremlin_python.driver.async_resultset.AsyncResultSet`.

        Lazily connects on the first call.  On a ``ConnectionResetError``
        during write, reconnects and retries once before propagating.
        Returns as soon as the first response chunk arrives; a background
        :class:`asyncio.Task` drains any remaining partial (206) chunks.
        """
        if not self._inited:
            await self.connect()

        if request_message.args.get("requestId"):
            request_id = str(request_message.args["requestId"])
            uuid.UUID(request_id)
        else:
            request_id = str(uuid.uuid4())

        result_set = AsyncResultSet(asyncio.Queue(), request_id)
        self._results[request_id] = result_set

        try:
            await self._protocol.write(request_id, request_message)
        except ConnectionResetError:
            log.debug("Write failed on %s — reconnecting and retrying", self._url)
            await self.connect()
            await self._protocol.write(request_id, request_message)

        return await self._receive(result_set)

    async def _receive(self, result_set):
        """Read the first response chunk and set up background consumption."""
        try:
            data = await self._transport.read()
            status_code = await self._protocol.data_received(data, self._results)
        except ConnectionResetError as exc:
            self._inited = False
            self._pool.put_nowait(self)
            raise InvalidatedConnectionError(str(exc)) from exc
        except Exception:
            self._inited = False
            self._pool.put_nowait(self)
            raise

        if status_code != 206:
            fut = asyncio.get_running_loop().create_future()
            fut.set_result(None)
            result_set.future = fut
            self._pool.put_nowait(self)
        else:
            async def _consume():
                try:
                    while True:
                        try:
                            chunk = await self._transport.read()
                            sc = await self._protocol.data_received(chunk, self._results)
                        except ConnectionResetError as exc:
                            raise InvalidatedConnectionError(str(exc)) from exc
                        if sc != 206:
                            break
                finally:
                    self._pool.put_nowait(self)

            result_set.future = asyncio.create_task(_consume())

        return result_set
