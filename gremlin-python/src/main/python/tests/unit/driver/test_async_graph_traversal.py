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
"""Unit tests for async_graph_traversal.

Architecture under test
-----------------------
* async_traversal() → _AsyncAnonymousTraversalSource
* .with_remote(rc)  → AsyncGraphTraversalSource (wraps GraphTraversalSource)
* g.V()             → AsyncGraphTraversal (GraphTraversal + AsyncTraversal via MRO)
* .to_list()        → AsyncTraversal.to_list() (async, iterates via __anext__)
* __anext__         → applies AsyncTraversalStrategies →
                       AsyncRemoteStrategy.apply() →
                       await rc.submit(bytecode) →
                       AsyncRemoteTraversal(AsyncResultSet) →
                       traversal.traversers = AsyncResultSet
                       → AsyncResultSet.__anext__() → Traverser objects
                       → traverser.object (unwrapped)

All I/O is mocked; no Gremlin Server is required.
"""
import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock

from gremlin_python.driver.async_graph_traversal import (
    AsyncGraphTraversal,
    AsyncGraphTraversalSource,
    AsyncRemoteTraversal,
    AsyncRemoteStrategy,
    AsyncTraversal,
    AsyncTraversalStrategies,
    AsyncTransaction,
    async_traversal,
)
from gremlin_python.driver.async_driver_remote_connection import AsyncDriverRemoteConnection
from gremlin_python.driver.async_resultset import AsyncResultSet
from gremlin_python.process.traversal import Bytecode, Traverser


URL = "ws://localhost:8182/gremlin"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_async_result_set(values):
    """AsyncResultSet pre-loaded with Traverser-wrapped *values*.

    Uses a MagicMock future (already done) so this helper works in both sync
    and async test contexts without requiring a running event loop.
    """
    q = asyncio.Queue()
    if values:
        q.put_nowait([Traverser(v) for v in values])
    rs = AsyncResultSet(q, "test-id")
    fut = MagicMock()
    fut.done.return_value = True
    fut.result.return_value = None
    rs.future = fut
    return rs


def _make_rc(values=None):
    """AsyncDriverRemoteConnection whose submit() returns an AsyncRemoteTraversal."""
    rc = AsyncDriverRemoteConnection(URL, "g")
    rs = _make_async_result_set(values or [])
    rc.submit = AsyncMock(return_value=AsyncRemoteTraversal(rs))
    return rc


def _make_g(rc):
    return async_traversal().with_remote(rc)


# ---------------------------------------------------------------------------
# async_traversal() factory
# ---------------------------------------------------------------------------

class TestAsyncTraversalFactory(unittest.TestCase):
    def test_with_remote_returns_async_source(self):
        rc = _make_rc()
        g = async_traversal().with_remote(rc)
        self.assertIsInstance(g, AsyncGraphTraversalSource)

    def test_source_v_returns_async_graph_traversal(self):
        g = _make_g(_make_rc())
        self.assertIsInstance(g.V(), AsyncGraphTraversal)

    def test_source_e_returns_async_graph_traversal(self):
        g = _make_g(_make_rc())
        self.assertIsInstance(g.E(), AsyncGraphTraversal)

    def test_source_add_v_returns_async_graph_traversal(self):
        g = _make_g(_make_rc())
        self.assertIsInstance(g.add_v("person"), AsyncGraphTraversal)

    def test_chained_steps_preserve_type(self):
        g = _make_g(_make_rc())
        t = g.V().has_label("person").values("name")
        self.assertIsInstance(t, AsyncGraphTraversal)

    def test_chained_steps_return_same_instance(self):
        g = _make_g(_make_rc())
        t = g.V()
        t2 = t.has_label("person")
        t3 = t2.values("name")
        self.assertIs(t, t2)
        self.assertIs(t, t3)


# ---------------------------------------------------------------------------
# Bytecode construction
# ---------------------------------------------------------------------------

class TestBytecodeConstruction(unittest.TestCase):
    def test_v_step_in_bytecode(self):
        g = _make_g(_make_rc())
        t = g.V()
        step_names = [s[0] for s in t.bytecode.step_instructions]
        self.assertIn("V", step_names)

    def test_chained_steps_accumulate(self):
        g = _make_g(_make_rc())
        t = g.V().has_label("person").values("name")
        step_names = [s[0] for s in t.bytecode.step_instructions]
        self.assertIn("V", step_names)
        self.assertIn("hasLabel", step_names)
        self.assertIn("values", step_names)

    def test_independent_traversals_separate_bytecodes(self):
        g = _make_g(_make_rc())
        t1 = g.V().has_label("person")
        t2 = g.V().has_label("software")
        self.assertIsNot(t1.bytecode, t2.bytecode)


# ---------------------------------------------------------------------------
# MRO — async terminal steps shadow sync ones
# ---------------------------------------------------------------------------

class TestMRO(unittest.TestCase):
    def test_to_list_is_coroutine_function(self):
        g = _make_g(_make_rc())
        import inspect
        self.assertTrue(inspect.iscoroutinefunction(g.V().to_list))

    def test_next_is_coroutine_function(self):
        import inspect
        g = _make_g(_make_rc())
        self.assertTrue(inspect.iscoroutinefunction(g.V().next))

    def test_iterate_is_coroutine_function(self):
        import inspect
        g = _make_g(_make_rc())
        self.assertTrue(inspect.iscoroutinefunction(g.V().iterate))

    def test_sync_next_raises(self):
        g = _make_g(_make_rc())
        with self.assertRaises(NotImplementedError):
            next(g.V())


# ---------------------------------------------------------------------------
# AsyncTraversalStrategies
# ---------------------------------------------------------------------------

class TestAsyncTraversalStrategies(unittest.IsolatedAsyncioTestCase):
    async def test_applies_async_strategy(self):
        called = []

        class FakeStrategy:
            async def apply(self, traversal):
                called.append("async")

        strategies = AsyncTraversalStrategies()
        strategies.add_strategies([FakeStrategy()])
        t = AsyncGraphTraversal(None, strategies, Bytecode())
        await strategies.apply_strategies(t)
        self.assertEqual(called, ["async"])

    async def test_applies_sync_strategy(self):
        called = []

        class FakeStrategy:
            def apply(self, traversal):
                called.append("sync")

        strategies = AsyncTraversalStrategies()
        strategies.add_strategies([FakeStrategy()])
        t = AsyncGraphTraversal(None, strategies, Bytecode())
        await strategies.apply_strategies(t)
        self.assertEqual(called, ["sync"])


# ---------------------------------------------------------------------------
# AsyncRemoteStrategy
# ---------------------------------------------------------------------------

class TestAsyncRemoteStrategy(unittest.IsolatedAsyncioTestCase):
    async def test_apply_sets_traversers(self):
        rc = _make_rc(values=["marko"])
        strategy = AsyncRemoteStrategy(rc)

        strategies = AsyncTraversalStrategies()
        strategies.add_strategies([strategy])
        t = AsyncGraphTraversal(None, strategies, Bytecode())

        self.assertIsNone(t.traversers)
        await strategy.apply(t)
        self.assertIsNotNone(t.traversers)

    async def test_apply_noop_if_traversers_already_set(self):
        rc = _make_rc(values=["marko"])
        strategy = AsyncRemoteStrategy(rc)

        strategies = AsyncTraversalStrategies()
        strategies.add_strategies([strategy])
        t = AsyncGraphTraversal(None, strategies, Bytecode())

        fake_traversers = object()
        t.traversers = fake_traversers
        await strategy.apply(t)
        # Should not overwrite existing traversers
        self.assertIs(t.traversers, fake_traversers)
        rc.submit.assert_not_called()


# ---------------------------------------------------------------------------
# Terminal steps — to_list
# ---------------------------------------------------------------------------

class TestToList(unittest.IsolatedAsyncioTestCase):
    async def test_to_list_returns_unwrapped_values(self):
        rc = _make_rc(values=["marko", "vadas"])
        g = _make_g(rc)
        result = await g.V().has_label("person").values("name").to_list()
        self.assertEqual(result, ["marko", "vadas"])

    async def test_to_list_empty(self):
        rc = _make_rc(values=[])
        g = _make_g(rc)
        result = await g.V().has_label("missing").to_list()
        self.assertEqual(result, [])

    async def test_to_list_submits_bytecode(self):
        rc = _make_rc(values=[6])
        g = _make_g(rc)
        await g.V().count().to_list()
        rc.submit.assert_awaited_once()
        submitted_bytecode = rc.submit.call_args[0][0]
        self.assertIsInstance(submitted_bytecode, Bytecode)

    async def test_to_list_single_value(self):
        rc = _make_rc(values=[42])
        g = _make_g(rc)
        result = await g.V().count().to_list()
        self.assertEqual(result, [42])


# ---------------------------------------------------------------------------
# Terminal steps — to_set
# ---------------------------------------------------------------------------

class TestToSet(unittest.IsolatedAsyncioTestCase):
    async def test_to_set_deduplicates(self):
        rc = _make_rc(values=["person", "person", "software"])
        g = _make_g(rc)
        result = await g.V().label().to_set()
        self.assertEqual(result, {"person", "software"})

    async def test_to_set_returns_set_type(self):
        rc = _make_rc(values=["a"])
        g = _make_g(rc)
        result = await g.V().label().to_set()
        self.assertIsInstance(result, set)


# ---------------------------------------------------------------------------
# Terminal steps — next
# ---------------------------------------------------------------------------

class TestNext(unittest.IsolatedAsyncioTestCase):
    async def test_next_returns_first(self):
        rc = _make_rc(values=[42])
        g = _make_g(rc)
        result = await g.V().count().next()
        self.assertEqual(result, 42)

    async def test_next_with_amount(self):
        rc = _make_rc(values=["marko", "vadas", "josh"])
        g = _make_g(rc)
        result = await g.V().values("name").next(2)
        self.assertEqual(result, ["marko", "vadas"])

    async def test_next_empty_raises_stop_async_iteration(self):
        rc = _make_rc(values=[])
        g = _make_g(rc)
        with self.assertRaises(StopAsyncIteration):
            await g.V().has("name", "nobody").next()

    async def test_next_zero_returns_empty_list(self):
        rc = _make_rc(values=["x"])
        g = _make_g(rc)
        result = await g.V().values("name").next(0)
        self.assertEqual(result, [])


# ---------------------------------------------------------------------------
# Terminal steps — iterate
# ---------------------------------------------------------------------------

class TestIterate(unittest.IsolatedAsyncioTestCase):
    async def test_iterate_returns_traversal(self):
        rc = _make_rc(values=[])
        g = _make_g(rc)
        result = await g.add_v("person").property("name", "test").iterate()
        self.assertIsInstance(result, AsyncGraphTraversal)

    async def test_iterate_adds_discard_step(self):
        rc = _make_rc(values=[])
        g = _make_g(rc)
        t = g.add_v("person")
        await t.iterate()
        step_names = [s[0] for s in t.bytecode.step_instructions]
        self.assertIn("discard", step_names)

    async def test_iterate_calls_submit(self):
        rc = _make_rc(values=[])
        g = _make_g(rc)
        await g.add_v("planet").property("name", "saturn").iterate()
        rc.submit.assert_awaited_once()


# ---------------------------------------------------------------------------
# Terminal steps — has_next
# ---------------------------------------------------------------------------

class TestHasNext(unittest.IsolatedAsyncioTestCase):
    async def test_has_next_true(self):
        rc = _make_rc(values=["marko"])
        g = _make_g(rc)
        result = await g.V().has("name", "marko").has_next()
        self.assertTrue(result)

    async def test_has_next_false(self):
        rc = _make_rc(values=[])
        g = _make_g(rc)
        result = await g.V().has("name", "nobody").has_next()
        self.assertFalse(result)


# ---------------------------------------------------------------------------
# Concurrent usage
# ---------------------------------------------------------------------------

class TestConcurrent(unittest.IsolatedAsyncioTestCase):
    async def test_gather_multiple_traversals(self):
        results_queue = [["marko", "vadas"], [6], ["knows", "created"]]
        call_index = 0

        async def mock_submit(bc):
            nonlocal call_index
            rs = _make_async_result_set(results_queue[call_index])
            call_index += 1
            return AsyncRemoteTraversal(rs)

        rc = AsyncDriverRemoteConnection(URL, "g")
        rc.submit = mock_submit
        g = _make_g(rc)

        names, count, edges = await asyncio.gather(
            g.V().has_label("person").values("name").to_list(),
            g.V().count().next(),
            g.E().label().to_list(),
        )

        self.assertEqual(names, ["marko", "vadas"])
        self.assertEqual(count, 6)
        self.assertEqual(edges, ["knows", "created"])


# ---------------------------------------------------------------------------
# AsyncGraphTraversalSource — get_graph_traversal_source
# ---------------------------------------------------------------------------

class TestGetGraphTraversalSource(unittest.TestCase):
    def test_returns_async_traversal_source(self):
        g = _make_g(_make_rc())
        g2 = g.get_graph_traversal_source()
        self.assertIsInstance(g2, AsyncGraphTraversalSource)

    def test_strategies_are_async(self):
        g = _make_g(_make_rc())
        g2 = g.get_graph_traversal_source()
        self.assertIsInstance(g2.traversal_strategies, AsyncTraversalStrategies)

    def test_async_remote_strategy_not_duplicated(self):
        rc = _make_rc()
        g = _make_g(rc)
        g2 = g.get_graph_traversal_source()
        remote_strategies = [
            s for s in g2.traversal_strategies.traversal_strategies
            if isinstance(s, AsyncRemoteStrategy)
        ]
        self.assertEqual(len(remote_strategies), 1)


# ---------------------------------------------------------------------------
# AsyncTransaction
# ---------------------------------------------------------------------------

class TestAsyncTransaction(unittest.IsolatedAsyncioTestCase):
    def _make_session_rc(self):
        """A parent RC that returns a session child on create_session()."""
        parent_rc = AsyncDriverRemoteConnection(URL, "g")
        parent_rc.submit = AsyncMock(return_value=AsyncRemoteTraversal(
            _make_async_result_set([])))

        session_rc = AsyncDriverRemoteConnection(URL, "g", session="fake-session")
        session_rc.submit = AsyncMock(return_value=AsyncRemoteTraversal(
            _make_async_result_set([])))
        session_rc.commit = AsyncMock()
        session_rc.rollback = AsyncMock()

        parent_rc.create_session = lambda: session_rc
        parent_rc.remove_session = AsyncMock()
        return parent_rc, session_rc

    async def test_begin_returns_async_graph_traversal_source(self):
        parent_rc, _ = self._make_session_rc()
        g = _make_g(parent_rc)
        tx = await g.tx()
        gtx = await tx.begin()
        self.assertIsInstance(gtx, AsyncGraphTraversalSource)

    async def test_begin_sets_is_open(self):
        parent_rc, _ = self._make_session_rc()
        g = _make_g(parent_rc)
        tx = await g.tx()
        self.assertFalse(tx.is_open)
        await tx.begin()
        self.assertTrue(tx.is_open)

    async def test_commit_closes_session(self):
        parent_rc, session_rc = self._make_session_rc()
        g = _make_g(parent_rc)
        tx = await g.tx()
        await tx.begin()
        await tx.commit()
        session_rc.commit.assert_awaited_once()
        self.assertFalse(tx.is_open)

    async def test_rollback_closes_session(self):
        parent_rc, session_rc = self._make_session_rc()
        g = _make_g(parent_rc)
        tx = await g.tx()
        await tx.begin()
        await tx.rollback()
        session_rc.rollback.assert_awaited_once()
        self.assertFalse(tx.is_open)

    async def test_context_manager_commits_on_success(self):
        parent_rc, session_rc = self._make_session_rc()
        g = _make_g(parent_rc)
        async with await g.tx():
            pass
        session_rc.commit.assert_awaited_once()

    async def test_context_manager_rolls_back_on_exception(self):
        parent_rc, session_rc = self._make_session_rc()
        g = _make_g(parent_rc)
        with self.assertRaises(ValueError):
            async with await g.tx():
                raise ValueError("boom")
        session_rc.rollback.assert_awaited_once()

    async def test_double_begin_raises(self):
        parent_rc, _ = self._make_session_rc()
        g = _make_g(parent_rc)
        tx = await g.tx()
        await tx.begin()
        with self.assertRaises(Exception, msg="already open"):
            await tx.begin()


if __name__ == "__main__":
    unittest.main()
