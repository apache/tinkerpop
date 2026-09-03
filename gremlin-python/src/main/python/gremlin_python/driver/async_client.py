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

from gremlin_python.driver import protocol, request, serializer
from gremlin_python.driver.async_connection import AsyncConnection, InvalidatedConnectionError
from gremlin_python.driver.async_protocol import AsyncGremlinServerWSProtocol
from gremlin_python.driver.aiohttp.async_transport import AsyncAiohttpWSTransport
from gremlin_python.driver.client import Client
from gremlin_python.process import traversal

log = logging.getLogger("gremlinpython")

__author__ = 'Apache TinkerPop (dev@tinkerpop.apache.org)'


class AsyncClient(Client):
    """Native-async Gremlin client using WebSocket transport.

    Unlike :class:`~gremlin_python.driver.client.Client`, no
    :class:`~concurrent.futures.ThreadPoolExecutor` is created.  The
    connection pool is an :class:`asyncio.Queue` and all submit/close
    methods are coroutines that must be awaited.

    Robustness features:

    * Each connection runs a background WebSocket ping to prevent idle
      disconnects (configurable via ``ping_interval``).
    * On :class:`~gremlin_python.driver.async_connection.InvalidatedConnectionError`
      the request is automatically retried on a fresh connection up to
      *pool_size* times.
    * *default_request_options* are merged into every request so options
      like ``evaluationTimeout`` can be set once per client.

    Usage::

        async with AsyncClient("ws://localhost:8182/gremlin", "g") as client:
            result_set = await client.submit("g.V().count()")
            print(await result_set.all())

    Parameters
    ----------
    url:
        WebSocket endpoint, e.g. ``ws://localhost:8182/gremlin``.
    traversal_source:
        Gremlin traversal source name (default ``"g"``).
    protocol_factory:
        Zero-argument callable returning an
        :class:`~gremlin_python.driver.async_protocol.AsyncGremlinServerWSProtocol`.
    transport_factory:
        Zero-argument callable returning an
        :class:`~gremlin_python.driver.aiohttp.async_transport.AsyncAiohttpWSTransport`.
    pool_size:
        Number of concurrent WebSocket connections (default 8; forced to 1
        in session mode).
    message_serializer:
        Serializer to use (default
        :class:`~gremlin_python.driver.serializer.GraphBinarySerializersV1`).
    username, password:
        Credentials for SASL/basic authentication.
    kerberized_service:
        Kerberos service principal for Kerberos authentication.
    headers:
        Extra HTTP headers included in every WebSocket handshake.
    session:
        Session UUID string to enable server-side sessions.
    enable_user_agent_on_connect:
        Send the driver user-agent header (default ``True``).
    ping_interval:
        Seconds between WebSocket keepalive pings (default 60, 0 to disable).
    default_request_options:
        Dict of request options merged into every request
        (e.g. ``{"evaluationTimeout": 30000}``).  Per-request options take
        precedence over defaults.
    **transport_kwargs:
        Extra keyword arguments forwarded to the default
        :class:`~gremlin_python.driver.aiohttp.async_transport.AsyncAiohttpWSTransport`.
    """

    def __init__(
        self,
        url,
        traversal_source,
        protocol_factory=None,
        transport_factory=None,
        pool_size=None,
        max_workers=None,  # accepted but unused — no thread pool
        message_serializer=None,
        username="",
        password="",
        kerberized_service="",
        headers=None,
        session=None,
        enable_user_agent_on_connect=True,
        ping_interval=60,
        default_request_options=None,
        **transport_kwargs,
    ):
        log.info("Creating AsyncClient with url '%s'", url)

        # Do NOT call super().__init__() — Client.__init__ creates a
        # ThreadPoolExecutor and a threading queue, neither of which we want.
        self._closed = False
        self._url = url
        self._headers = headers
        self._enable_user_agent_on_connect = enable_user_agent_on_connect
        self._traversal_source = traversal_source
        self._executor = None  # no thread pool; attribute expected by Client
        self._ping_interval = ping_interval
        self._default_request_options = default_request_options or {}

        if "max_content_length" not in transport_kwargs:
            transport_kwargs["max_content_length"] = 10 * 1024 * 1024

        if message_serializer is None:
            message_serializer = serializer.GraphBinarySerializersV1()

        self._message_serializer = message_serializer
        self._username = username
        self._password = password
        self._session = session
        self._session_enabled = session is not None and session != ""

        if transport_factory is None:
            def transport_factory():
                return AsyncAiohttpWSTransport(**transport_kwargs)

        self._transport_factory = transport_factory

        if protocol_factory is None:
            def protocol_factory():
                return AsyncGremlinServerWSProtocol(
                    self._message_serializer,
                    username=self._username,
                    password=self._password,
                    kerberized_service=kerberized_service,
                    max_content_length=transport_kwargs["max_content_length"],
                )

        self._protocol_factory = protocol_factory

        if self._session_enabled:
            if pool_size is None:
                pool_size = 1
            elif pool_size != 1:
                raise Exception("PoolSize must be 1 on session mode!")
        if pool_size is None:
            pool_size = 8
        self._pool_size = pool_size

        self._pool = asyncio.Queue()
        self._fill_pool()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_connection(self):
        proto = self._protocol_factory()
        return AsyncConnection(
            self._url,
            self._traversal_source,
            proto,
            self._transport_factory,
            None,
            self._pool,
            headers=self._headers,
            enable_user_agent_on_connect=self._enable_user_agent_on_connect,
            ping_interval=self._ping_interval,
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def close(self):
        """Close all pooled connections and mark the client as closed."""
        if self._closed:
            return
        if self._session_enabled:
            await self._close_session()
        log.info("Closing AsyncClient with url '%s'", self._url)
        conns = []
        while not self._pool.empty():
            conns.append(self._pool.get_nowait())
        await asyncio.gather(*(conn.close() for conn in conns), return_exceptions=True)
        self._closed = True

    async def _close_session(self):
        message = request.RequestMessage(
            processor="session",
            op="close",
            args={"session": str(self._session)},
        )
        conn = await self._pool.get()
        try:
            result_set = await conn.write(message)
            await result_set.all()
        except protocol.GremlinServerError:
            pass

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        await self.close()

    # ------------------------------------------------------------------
    # Submit (with retry)
    # ------------------------------------------------------------------

    async def submit(self, message, bindings=None, request_options=None):
        """Submit *message* and return an :class:`~gremlin_python.driver.async_resultset.AsyncResultSet`.

        Retries automatically up to *pool_size* times when a connection is
        invalidated mid-request due to a network reset.
        """
        last_error = None
        for _ in range(self._pool_size):
            try:
                return await self.submit_async(
                    message, bindings=bindings, request_options=request_options
                )
            except InvalidatedConnectionError as exc:
                log.warning("Connection invalidated, retrying: %s", exc.__cause__)
                last_error = exc.__cause__
        raise last_error or InvalidatedConnectionError("All retry attempts exhausted")

    async def submit_async(self, message, bindings=None, request_options=None):
        """Submit *message* without retry and return an :class:`~gremlin_python.driver.async_resultset.AsyncResultSet`.

        Returns before all results have arrived; the result set can be
        iterated asynchronously (``async for``) or consumed via
        :meth:`~gremlin_python.driver.async_resultset.AsyncResultSet.all`.
        """
        if self.is_closed():
            raise Exception("Client is closed")

        args = {"gremlin": message, "aliases": {"g": self._traversal_source}}
        processor = ""
        op = "eval"
        if isinstance(message, traversal.Bytecode):
            op = "bytecode"
            processor = "traversal"

        if isinstance(message, str) and bindings:
            args["bindings"] = bindings

        if self._session_enabled:
            args["session"] = str(self._session)
            processor = "session"

        if isinstance(message, (traversal.Bytecode, str)):
            message = request.RequestMessage(
                processor=processor, op=op, args=args
            )

        conn = await self._pool.get()

        # Merge default options first; per-request options win.
        if self._default_request_options:
            merged = {**self._default_request_options}
            if request_options:
                merged.update(request_options)
            message.args.update(merged)
        elif request_options:
            message.args.update(request_options)

        return await conn.write(message)
