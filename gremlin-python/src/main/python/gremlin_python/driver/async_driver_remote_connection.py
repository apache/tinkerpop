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

from gremlin_python.driver import serializer
from gremlin_python.driver.async_client import AsyncClient
from gremlin_python.driver.remote_connection import RemoteConnection
from gremlin_python.process.strategies import OptionsStrategy
from gremlin_python.process.traversal import Bytecode

log = logging.getLogger("gremlinpython")


class AsyncDriverRemoteConnection(RemoteConnection):
    """Async remote connection backed by AsyncClient.

    Primary entry point for the async Gremlin driver.  Works with
    async_traversal() to provide an async traversal DSL::

        async with AsyncDriverRemoteConnection("ws://localhost:8182/gremlin", "g") as rc:
            g = async_traversal().with_remote(rc)
            names = await g.V().has_label("person").values("name").to_list()

    Features:

    * Per-connection WebSocket keepalive pings (see *ping_interval*).
    * *query_timeout* sets a default evaluationTimeout on every request.

    Parameters
    ----------
    url:
        WebSocket endpoint, e.g. ws://localhost:8182/gremlin.
    traversal_source:
        Gremlin traversal source name (default "g").
    protocol_factory:
        Forwarded to AsyncClient.
    transport_factory:
        Forwarded to AsyncClient.
    pool_size:
        Number of concurrent WebSocket connections (default 8).
    max_workers:
        Accepted for configuration parity with DriverRemoteConnection.
    message_serializer:
        Serializer instance (default GraphBinarySerializersV1).
    username, password:
        Credentials for SASL/basic authentication.
    kerberized_service:
        Kerberos service principal.
    headers:
        Extra HTTP headers for every WebSocket handshake.
    session:
        Session UUID string to enable server-side sessions.
    enable_user_agent_on_connect:
        Send the driver user-agent header (default True).
    enable_compression:
        Enable WebSocket per-message deflate compression (default False).
    graphson_reader, graphson_writer:
        Optional custom GraphSON reader and writer used when
        *message_serializer* is not explicitly provided.
    ping_interval:
        Seconds between keepalive pings (default 60, 0 to disable).
    query_timeout:
        Default query timeout in seconds applied to every request as
        evaluationTimeout.  Per-request options override this.
    **transport_kwargs:
        Extra keyword arguments forwarded to the default transport.
    """

    def __init__(
        self,
        url,
        traversal_source="g",
        protocol_factory=None,
        transport_factory=None,
        pool_size=None,
        max_workers=None,
        message_serializer=None,
        username="",
        password="",
        kerberized_service="",
        headers=None,
        session=None,
        enable_user_agent_on_connect=True,
        enable_compression=False,
        graphson_reader=None,
        graphson_writer=None,
        ping_interval=60,
        query_timeout=None,
        **transport_kwargs,
    ):
        super().__init__(url, traversal_source)
        self._protocol_factory = protocol_factory
        self._transport_factory = transport_factory
        self._pool_size = pool_size
        self._max_workers = max_workers
        self._username = username
        self._password = password
        self._kerberized_service = kerberized_service
        self._headers = headers
        self.__session = session
        self._enable_user_agent_on_connect = enable_user_agent_on_connect
        self._enable_compression = enable_compression
        self._graphson_reader = graphson_reader
        self._graphson_writer = graphson_writer
        self._ping_interval = ping_interval
        self._query_timeout = query_timeout
        self._transport_kwargs = dict(transport_kwargs)
        self.__spawned_sessions = []

        if (
            message_serializer is None
            and graphson_reader is not None
            and graphson_writer is not None
        ):
            message_serializer = serializer.GraphSONMessageSerializer(
                reader=graphson_reader, writer=graphson_writer
            )
        self._message_serializer = message_serializer

        default_request_options = {}
        if query_timeout is not None:
            default_request_options["evaluationTimeout"] = int(query_timeout * 1000)

        self._async_client = AsyncClient(
            url,
            traversal_source,
            protocol_factory=protocol_factory,
            transport_factory=transport_factory,
            pool_size=pool_size,
            max_workers=max_workers,
            message_serializer=message_serializer,
            username=username,
            password=password,
            kerberized_service=kerberized_service,
            headers=headers,
            session=session,
            enable_user_agent_on_connect=enable_user_agent_on_connect,
            enable_compression=enable_compression,
            ping_interval=ping_interval,
            default_request_options=default_request_options or None,
            **transport_kwargs,
        )

    # ------------------------------------------------------------------
    # RemoteConnection interface
    # ------------------------------------------------------------------

    def submit(self, bytecode):
        """Not supported synchronously.

        submit() is overridden as an async coroutine below.  Python allows an
        async def to satisfy an abstract def requirement, so this docstring
        exists only to explain why the sync method is absent.
        """
        raise RuntimeError(
            "AsyncDriverRemoteConnection.submit() must be awaited. "
            "Call it as 'await connection.submit(bytecode)' or use "
            "'async_traversal().with_remote(rc)' for the DSL."
        )

    # ------------------------------------------------------------------
    # Async submit (called by AsyncRemoteStrategy)
    # ------------------------------------------------------------------

    async def submit(self, bytecode):  # noqa: F811 — intentional async override
        """Execute *bytecode* and return an AsyncRemoteTraversal.

        Called automatically by AsyncRemoteStrategy when a terminal step
        (to_list(), next(), ...) is awaited on a traversal.  Application code
        rarely needs to call this directly.
        """
        from gremlin_python.driver.async_graph_traversal import AsyncRemoteTraversal
        options = self._extract_request_options(bytecode)
        result_set = await self._async_client.submit(bytecode, request_options=options)
        return AsyncRemoteTraversal(result_set)

    # ------------------------------------------------------------------
    # Convenience methods (application-level)
    # ------------------------------------------------------------------

    async def submit_async(self, traversal_or_bytecode, request_options=None):
        """Execute a traversal or bytecode and return all results as a list.

        Results are automatically unwrapped from their Traverser wrappers.
        Prefer the DSL (g.V().to_list()) for most use-cases; use this
        method when you need direct bytecode submission.

        Parameters
        ----------
        traversal_or_bytecode:
            A GraphTraversal (or AsyncGraphTraversal) built with the Gremlin
            DSL without calling a terminal step, or a raw Bytecode object.
        request_options:
            Optional dict of request options.  When *None*, options are
            extracted from any OptionsStrategy attached to the traversal.
        """
        remote_traversal = await self.submit(
            getattr(traversal_or_bytecode, "bytecode", traversal_or_bytecode)
        )
        return await remote_traversal.to_list()

    async def submit_stream(self, traversal_or_bytecode, request_options=None):
        """Execute a traversal and return an AsyncResultSet.

        Use when you need to stream results one at a time (``async for``)
        rather than collecting them into memory.
        """
        bytecode = getattr(traversal_or_bytecode, "bytecode", traversal_or_bytecode)
        options = request_options if request_options is not None else \
            self._extract_request_options(bytecode)
        return await self._async_client.submit(bytecode, request_options=options)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def close(self):
        """Close all spawned session connections and the underlying client."""
        await asyncio.shield(self._close())

    async def _close(self):
        errors = []
        for session in tuple(self.__spawned_sessions):
            try:
                await session.close()
            except BaseException as exc:
                errors.append(exc)
            finally:
                if session in self.__spawned_sessions:
                    self.__spawned_sessions.remove(session)
        try:
            await self._async_client.close()
        except BaseException as exc:
            errors.append(exc)
        if errors:
            raise errors[0]

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        await self.close()

    # ------------------------------------------------------------------
    # State
    # ------------------------------------------------------------------

    def is_closed(self):
        return self._async_client.is_closed()

    def is_session_bound(self):
        return self.__session is not None

    # ------------------------------------------------------------------
    # Session management
    # ------------------------------------------------------------------

    def create_session(self):
        """Return a new session-bound AsyncDriverRemoteConnection."""
        if self.is_session_bound():
            raise Exception(
                "Connection is already bound to a session — nested sessions are not allowed"
            )
        conn = AsyncDriverRemoteConnection(
            self._url,
            traversal_source=self._traversal_source,
            protocol_factory=self._protocol_factory,
            transport_factory=self._transport_factory,
            pool_size=1,
            max_workers=self._max_workers,
            message_serializer=self._message_serializer,
            username=self._username,
            password=self._password,
            kerberized_service=self._kerberized_service,
            headers=self._headers,
            session=uuid.uuid4(),
            enable_user_agent_on_connect=self._enable_user_agent_on_connect,
            enable_compression=self._enable_compression,
            graphson_reader=self._graphson_reader,
            graphson_writer=self._graphson_writer,
            ping_interval=self._ping_interval,
            query_timeout=self._query_timeout,
            **self._transport_kwargs,
        )
        self.__spawned_sessions.append(conn)
        return conn

    async def remove_session(self, session_based_connection):
        """Close and untrack a previously spawned session connection."""
        try:
            await session_based_connection.close()
        finally:
            if session_based_connection in self.__spawned_sessions:
                self.__spawned_sessions.remove(session_based_connection)

    # ------------------------------------------------------------------
    # Transactions
    # ------------------------------------------------------------------

    async def commit(self):
        """Commit the current transaction."""
        result_set = await self._async_client.submit(Bytecode.GraphOp.commit())
        await result_set.all()

    async def rollback(self):
        """Roll back the current transaction."""
        result_set = await self._async_client.submit(Bytecode.GraphOp.rollback())
        await result_set.all()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_request_options(bytecode):
        options_strategy = next(
            (x for x in bytecode.source_instructions
             if x[0] == "withStrategies" and type(x[1]) is OptionsStrategy),
            None,
        )
        if not options_strategy:
            return None
        allowed_keys = [
            "evaluationTimeout", "scriptEvaluationTimeout", "batchSize",
            "requestId", "userAgent", "materializeProperties",
        ]
        return {
            k: options_strategy[1].configuration[k]
            for k in allowed_keys
            if k in options_strategy[1].configuration
        }
