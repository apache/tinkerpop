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
"""Async traversal layer for gremlin-python.

Drop-in async replacement for the standard ``traversal()`` factory::

    async with AsyncDriverRemoteConnection("ws://localhost:8182/gremlin", "g") as rc:
        g = async_traversal().with_remote(rc)

        names  = await g.V().has_label("person").values("name").to_list()
        count  = await g.V().count().next()
        await  g.add_v("person").property("name", "saturn").iterate()

        async with g.tx() as gtx:
            await gtx.add_v("person").property("name", "pluto").iterate()
            # commits on clean exit, rolls back on exception
"""
import asyncio
import logging

from gremlin_python.process.graph_traversal import GraphTraversal, GraphTraversalSource
from gremlin_python.process.traversal import (
    Bytecode, Traversal, TraversalStrategies, TraversalStrategy,
)
from gremlin_python.structure.graph import Graph

log = logging.getLogger("gremlinpython")

__author__ = 'Apache TinkerPop (dev@tinkerpop.apache.org)'


# ---------------------------------------------------------------------------
# Public factory
# ---------------------------------------------------------------------------

def async_traversal():
    """Return an :class:`_AsyncAnonymousTraversalSource`.

    Use in place of :func:`~gremlin_python.process.anonymous_traversal.traversal`
    when working with :class:`~gremlin_python.driver.async_driver_remote_connection.AsyncDriverRemoteConnection`.
    """
    return _AsyncAnonymousTraversalSource()


class _AsyncAnonymousTraversalSource:
    def with_remote(self, remote_connection):
        """Bind *remote_connection* and return an :class:`AsyncGraphTraversalSource`."""
        return AsyncGraphTraversalSource(
            Graph(),
            AsyncTraversalStrategies(),
            remote_connection=remote_connection,
        )


# ---------------------------------------------------------------------------
# Strategy pipeline (async)
# ---------------------------------------------------------------------------

class AsyncTraversalStrategies(TraversalStrategies):
    """Async-aware strategy pipeline.

    Calls ``await strategy.apply(traversal)`` for coroutine strategies and
    ``strategy.apply(traversal)`` for plain sync strategies so both can coexist.
    """

    async def apply_strategies(self, traversal):  # noqa: D102
        for strategy in self.traversal_strategies:
            if asyncio.iscoroutinefunction(strategy.apply):
                await strategy.apply(traversal)
            else:
                strategy.apply(traversal)


class AsyncRemoteStrategy(TraversalStrategy):
    """Client-side strategy that submits bytecode to the remote server asynchronously.

    Registered with the ``py:AsyncRemoteStrategy`` fqcn so it is never sent
    to the server as part of the serialized traversal.
    """

    def __init__(self, remote_connection):
        super().__init__(fqcn="py:AsyncRemoteStrategy")
        self.remote_connection = remote_connection

    async def apply(self, traversal):
        if traversal.traversers is None:
            remote_traversal = await self.remote_connection.submit(traversal.bytecode)
            traversal.remote_results = remote_traversal
            traversal.traversers = remote_traversal.traversers


# ---------------------------------------------------------------------------
# Traversal base (async terminal steps)
# ---------------------------------------------------------------------------

class AsyncTraversal(Traversal):
    """Base traversal class that replaces blocking terminal steps with coroutines.

    Concrete subclasses must be used with :class:`AsyncTraversalStrategies` so
    that ``apply_strategies`` can be awaited inside ``__anext__``.
    """

    # Prevent accidental sync iteration.
    def __next__(self):
        raise NotImplementedError(
            "AsyncTraversal does not support synchronous iteration. "
            "Use 'async for', 'await to_list()', or 'await next()'."
        )

    # ------------------------------------------------------------------
    # Async iteration protocol
    # ------------------------------------------------------------------

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.traversers is None:
            await self.traversal_strategies.apply_strategies(self)
        if self.last_traverser is None:
            # StopAsyncIteration propagates naturally when the result set is exhausted.
            self.last_traverser = await self.traversers.__anext__()
        obj = self.last_traverser.object
        self.last_traverser.bulk -= 1
        if self.last_traverser.bulk <= 0:
            self.last_traverser = None
        return obj

    # ------------------------------------------------------------------
    # Async terminal steps
    # ------------------------------------------------------------------

    async def to_list(self):
        """Execute the traversal and return all results as a :class:`list`."""
        results = []
        async for item in self:
            results.append(item)
        return results

    async def to_set(self):
        """Execute the traversal and return all results as a :class:`set`."""
        return set(await self.to_list())

    async def next(self, amount=None):
        """Execute and return the first result, or the first *amount* results.

        Raises :class:`StopAsyncIteration` when *amount* is ``None`` and the
        result set is empty.
        """
        if amount is None:
            return await self.__anext__()
        results = []
        for _ in range(amount):
            try:
                results.append(await self.__anext__())
            except StopAsyncIteration:
                break
        return results

    async def iterate(self):
        """Execute the traversal, discarding all results.

        Use for write operations (``add_v``, ``add_e``, ``property``, …)
        where no return value is needed.
        """
        self.bytecode.add_step("discard")
        async for _ in self:
            pass
        return self

    async def has_next(self):
        """Return ``True`` if the traversal produces at least one result."""
        try:
            await self.__anext__()
            return True
        except StopAsyncIteration:
            return False

    async def next_traverser(self):
        """Return the next raw :class:`~gremlin_python.process.traversal.Traverser`."""
        if self.traversers is None:
            await self.traversal_strategies.apply_strategies(self)
        return await self.traversers.__anext__()


# ---------------------------------------------------------------------------
# Concrete traversal classes
# ---------------------------------------------------------------------------

class AsyncGraphTraversal(GraphTraversal, AsyncTraversal):
    """Async graph traversal.

    Inherits all Gremlin DSL steps from :class:`~gremlin_python.process.graph_traversal.GraphTraversal`
    and async terminal steps from :class:`AsyncTraversal` via Python's MRO.

    MRO: AsyncGraphTraversal → GraphTraversal → AsyncTraversal → Traversal → object

    Because ``GraphTraversal`` does not define terminal steps directly (it
    inherits them from ``Traversal``), Python finds ``AsyncTraversal``\'s
    async versions first in the MRO before reaching ``Traversal``\'s sync
    versions.
    """


class AsyncRemoteTraversal(AsyncTraversal):
    """Holds the :class:`~gremlin_python.driver.async_resultset.AsyncResultSet`
    returned by :meth:`~gremlin_python.driver.async_driver_remote_connection.AsyncDriverRemoteConnection.submit`.

    ``traversers`` is set directly to the result set; strategies are never
    applied (there is nothing to apply on an already-executed traversal).
    """

    def __init__(self, traversers):
        super().__init__(None, None, None)
        self.traversers = traversers


# ---------------------------------------------------------------------------
# Traversal source
# ---------------------------------------------------------------------------

class AsyncGraphTraversalSource(GraphTraversalSource):
    """Async traversal source.

    All spawned traversals are :class:`AsyncGraphTraversal` instances.
    Configuration steps (``with_bulk()``, ``with_path()``, …) return a new
    :class:`AsyncGraphTraversalSource`.

    Use :func:`async_traversal` to create one::

        g = async_traversal().with_remote(rc)
    """

    def __init__(self, graph, traversal_strategies, bytecode=None, remote_connection=None):
        # Call super WITHOUT remote_connection to avoid RemoteStrategy being added.
        super().__init__(graph, traversal_strategies, bytecode)
        self.graph_traversal = AsyncGraphTraversal
        self.remote_connection = remote_connection
        if remote_connection is not None:
            self.traversal_strategies.add_strategies([AsyncRemoteStrategy(remote_connection)])

    def get_graph_traversal_source(self):
        """Return a copy of this source (used by configuration steps)."""
        # Strategies are copied — do NOT pass remote_connection to avoid
        # registering AsyncRemoteStrategy a second time.
        return AsyncGraphTraversalSource(
            self.graph,
            AsyncTraversalStrategies(self.traversal_strategies),
            Bytecode(self.bytecode),
        )

    def get_graph_traversal(self):
        """Return a fresh :class:`AsyncGraphTraversal` for this source."""
        return AsyncGraphTraversal(
            self.graph,
            self.traversal_strategies,
            Bytecode(self.bytecode),
        )

    async def tx(self):
        """Return an :class:`AsyncTransaction` for this connection.

        Raises if the connection is already session-bound.
        """
        strategy = next(
            (s for s in self.traversal_strategies.traversal_strategies
             if isinstance(s, AsyncRemoteStrategy)),
            None,
        )
        if strategy is None:
            raise Exception("No remote connection configured on this traversal source")
        if strategy.remote_connection.is_session_bound():
            raise Exception(
                "Cannot open a transaction on an already session-bound connection"
            )
        return AsyncTransaction(self, strategy.remote_connection)


# ---------------------------------------------------------------------------
# Transaction
# ---------------------------------------------------------------------------

class AsyncTransaction:
    """Async transaction over a session-bound connection.

    Supports the async context-manager protocol::

        async with await g.tx() as gtx:
            await gtx.add_v("person").property("name", "pluto").iterate()
            # commits on clean exit; rolls back on exception

    Or manage it explicitly::

        tx = await g.tx()
        gtx = await tx.begin()
        try:
            await gtx.add_v("person").property("name", "pluto").iterate()
            await tx.commit()
        except Exception:
            await tx.rollback()
    """

    def __init__(self, g, remote_connection):
        self._g = g
        self._remote_connection = remote_connection
        self._session_connection = None
        self._gtx = None
        self._is_open = False
        self._mutex = asyncio.Lock()

    @property
    def is_open(self):
        return self._is_open

    @property
    def gtx(self):
        if self._gtx is None:
            raise Exception("Transaction not started — call begin() first")
        return self._gtx

    async def begin(self):
        """Open a session and return a session-bound :class:`AsyncGraphTraversalSource`."""
        async with self._mutex:
            if self._is_open:
                raise Exception("Transaction is already open")
            self._session_connection = self._remote_connection.create_session()
            strategies = AsyncTraversalStrategies()
            strategies.add_strategies([AsyncRemoteStrategy(self._session_connection)])
            self._gtx = AsyncGraphTraversalSource(
                self._g.graph,
                strategies,
            )
            self._is_open = True
            return self._gtx

    async def commit(self):
        """Commit the transaction and close the session."""
        async with self._mutex:
            if not self._is_open:
                raise Exception("Transaction is not open")
            await self._session_connection.commit()
            await self._close_session()

    async def rollback(self):
        """Roll back the transaction and close the session."""
        async with self._mutex:
            if not self._is_open:
                raise Exception("Transaction is not open")
            await self._session_connection.rollback()
            await self._close_session()

    async def close(self):
        """Close the session without committing or rolling back."""
        async with self._mutex:
            if self._is_open:
                await self._close_session()

    async def _close_session(self):
        self._is_open = False
        await self._remote_connection.remove_session(self._session_connection)
        self._session_connection = None
        self._gtx = None

    # Context-manager support: `async with await g.tx() as gtx`
    async def __aenter__(self):
        return await self.begin()

    async def __aexit__(self, exc_type, *_):
        if not self._is_open:
            return
        if exc_type is not None:
            await self.rollback()
        else:
            await self.commit()
