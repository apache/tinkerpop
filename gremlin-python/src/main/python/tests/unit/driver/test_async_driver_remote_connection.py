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
"""Unit tests for AsyncDriverRemoteConnection.

Architecture under test
-----------------------
* submit(bytecode) — async coroutine, called by AsyncRemoteStrategy,
  returns AsyncRemoteTraversal
* submit_async(traversal) — convenience: calls submit_stream() then .to_list()
* submit_stream(traversal) — returns raw AsyncResultSet for streaming
* Lifecycle: close(), context manager, is_closed(), sessions, transactions
* _extract_request_options — static helper

All I/O is mocked; no Gremlin Server is required.
"""
import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock

from gremlin_python.driver import serializer
from gremlin_python.driver.async_driver_remote_connection import AsyncDriverRemoteConnection
from gremlin_python.driver.async_graph_traversal import (
    AsyncRemoteTraversal,
    async_traversal,
)
from gremlin_python.driver.async_resultset import AsyncResultSet
from gremlin_python.process.traversal import Bytecode, Traverser
from gremlin_python.process.strategies import OptionsStrategy
from gremlin_python.structure.io import graphsonV3d0


URL = "ws://localhost:8182/gremlin"
TRAVERSAL_SOURCE = "g"


class _AsyncioTestCase(unittest.IsolatedAsyncioTestCase):
    def run(self, result=None):
        try:
            return super().run(result)
        finally:
            asyncio.set_event_loop_policy(None)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_result_set(values):
    q = asyncio.Queue()
    if values:
        q.put_nowait([Traverser(v) for v in values])
    rs = AsyncResultSet(q, "test-id")
    fut = MagicMock()
    fut.done.return_value = True
    fut.result.return_value = None
    rs.future = fut
    return rs


def _patch_submit(rc, values=None):
    """Replace rc.submit with an AsyncMock returning AsyncRemoteTraversal."""
    rs = _make_result_set(values or [])
    rc.submit = AsyncMock(return_value=AsyncRemoteTraversal(rs))
    return rs


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------

class TestInit(unittest.TestCase):
    def test_url_and_traversal_source_stored(self):
        from gremlin_python.driver.remote_connection import RemoteConnection
        rc = AsyncDriverRemoteConnection(URL, TRAVERSAL_SOURCE)
        self.assertFalse(issubclass(AsyncDriverRemoteConnection, RemoteConnection))
        self.assertEqual(rc.url, URL)
        self.assertEqual(rc.traversal_source, TRAVERSAL_SOURCE)
        self.assertEqual(repr(rc), f"remoteconnection[{URL},{TRAVERSAL_SOURCE}]")

    def test_default_traversal_source(self):
        rc = AsyncDriverRemoteConnection(URL)
        self.assertEqual(rc.traversal_source, "g")

    def test_not_closed_on_init(self):
        rc = AsyncDriverRemoteConnection(URL)
        self.assertFalse(rc.is_closed())

    def test_not_session_bound_by_default(self):
        rc = AsyncDriverRemoteConnection(URL)
        self.assertFalse(rc.is_session_bound())

    def test_session_bound_when_session_provided(self):
        import uuid
        rc = AsyncDriverRemoteConnection(URL, session=str(uuid.uuid4()))
        self.assertTrue(rc.is_session_bound())

    def test_query_timeout_sets_default_request_options(self):
        rc = AsyncDriverRemoteConnection(URL, query_timeout=30)
        self.assertEqual(
            rc._async_client._default_request_options.get("evaluationTimeout"),
            30000,
        )

    def test_query_timeout_fractional_seconds(self):
        rc = AsyncDriverRemoteConnection(URL, query_timeout=1.5)
        self.assertEqual(
            rc._async_client._default_request_options.get("evaluationTimeout"),
            1500,
        )

    def test_enable_compression_stored_and_forwarded(self):
        rc = AsyncDriverRemoteConnection(URL, enable_compression=True)
        self.assertTrue(rc._enable_compression)
        transport = rc._async_client._transport_factory()
        self.assertEqual(transport._aiohttp_kwargs.get("compress"), 15)

    def test_graphson_reader_and_writer_creates_graphson_serializer(self):
        reader = graphsonV3d0.GraphSONReader()
        writer = graphsonV3d0.GraphSONWriter()
        rc = AsyncDriverRemoteConnection(
            URL,
            graphson_reader=reader,
            graphson_writer=writer,
        )
        self.assertIsInstance(
            rc._message_serializer,
            serializer.GraphSONMessageSerializer,
        )

    def test_explicit_message_serializer_overrides_graphson_reader_writer(self):
        reader = graphsonV3d0.GraphSONReader()
        writer = graphsonV3d0.GraphSONWriter()
        custom_serializer = serializer.GraphBinarySerializersV1()
        rc = AsyncDriverRemoteConnection(
            URL,
            message_serializer=custom_serializer,
            graphson_reader=reader,
            graphson_writer=writer,
        )
        self.assertIs(rc._message_serializer, custom_serializer)


# ---------------------------------------------------------------------------
# submit() — called by AsyncRemoteStrategy
# ---------------------------------------------------------------------------

class TestSubmit(_AsyncioTestCase):
    async def test_submit_bytecode_returns_async_remote_traversal(self):
        rc = AsyncDriverRemoteConnection(URL, TRAVERSAL_SOURCE)
        rc._async_client.submit = AsyncMock(return_value=_make_result_set([6]))
        bc = Bytecode()
        bc.add_step("V")
        bc.add_step("count")
        result = await rc.submit(bc)
        self.assertIsInstance(result, AsyncRemoteTraversal)

    async def test_submit_traversers_iterable(self):
        rc = AsyncDriverRemoteConnection(URL, TRAVERSAL_SOURCE)
        rc._async_client.submit = AsyncMock(return_value=_make_result_set(["marko", "vadas"]))
        bc = Bytecode()
        remote_traversal = await rc.submit(bc)
        results = await remote_traversal.to_list()
        self.assertEqual(results, ["marko", "vadas"])

    async def test_submit_delegates_to_submit_stream(self):
        rc = AsyncDriverRemoteConnection(URL, TRAVERSAL_SOURCE)
        rc._async_client.submit = AsyncMock(side_effect=AssertionError("submit_stream was bypassed"))
        rc.submit_stream = AsyncMock(return_value=_make_result_set([6]))
        bc = Bytecode()
        bc.add_step("V")

        result = await rc.submit(bc)

        self.assertEqual(await result.to_list(), [6])
        rc.submit_stream.assert_awaited_once_with(bc)


# ---------------------------------------------------------------------------
# submit_async() — convenience wrapper
# ---------------------------------------------------------------------------

class TestSubmitAsync(_AsyncioTestCase):
    async def test_returns_list(self):
        rc = self._make_rc(values=[10])
        bc = Bytecode()
        results = await rc.submit_async(bc)
        self.assertEqual(results, [10])

    async def test_accepts_traversal_object(self):
        rc = self._make_rc(values=["marko"])
        g = async_traversal().with_remote(rc)
        t = g.V().has_label("person")
        results = await rc.submit_async(t)
        self.assertEqual(results, ["marko"])
        # Verify bytecode was extracted and passed
        submitted_bc = rc._async_client.submit.call_args[0][0]
        self.assertIsInstance(submitted_bc, Bytecode)

    async def test_empty_result(self):
        rc = self._make_rc()
        results = await rc.submit_async(Bytecode())
        self.assertEqual(results, [])

    async def test_propagates_exception(self):
        rc = self._make_rc()
        rc._async_client.submit.side_effect = RuntimeError("server error")
        with self.assertRaises(RuntimeError):
            await rc.submit_async(Bytecode())

    def _make_rc(self, values=None):
        rc = AsyncDriverRemoteConnection(URL, TRAVERSAL_SOURCE)
        rc._async_client.submit = AsyncMock(return_value=_make_result_set(values or []))
        return rc

    async def test_submit_async_delegates_to_submit_stream_without_options(self):
        rc = AsyncDriverRemoteConnection(URL, TRAVERSAL_SOURCE)
        rc.submit = AsyncMock()
        rc.submit_stream = AsyncMock(return_value=_make_result_set([10]))
        bc = Bytecode()

        self.assertEqual(await rc.submit_async(bc), [10])

        rc.submit_stream.assert_awaited_once_with(bc, request_options=None)
        rc.submit.assert_not_awaited()

    async def test_forwards_explicit_options_for_bytecode(self):
        rc = self._make_rc(values=[10])
        bc = Bytecode()
        bc.add_step("V")
        options = {"evaluationTimeout": 5000, "batchSize": 8}

        self.assertEqual(await rc.submit_async(bc, request_options=options), [10])

        rc._async_client.submit.assert_awaited_once_with(bc, request_options=options)

    async def test_explicit_options_override_traversal_options(self):
        rc = self._make_rc(values=["marko"])
        traversal = async_traversal().with_remote(rc).with_("evaluationTimeout", 1000).V()
        options = {"evaluationTimeout": 5000}

        self.assertEqual(await rc.submit_async(traversal, request_options=options), ["marko"])

        rc._async_client.submit.assert_awaited_once_with(
            traversal.bytecode, request_options=options
        )

    async def test_extracts_traversal_options_when_options_are_none(self):
        rc = self._make_rc(values=["marko"])
        g = async_traversal().with_remote(rc).with_("evaluationTimeout", 1234)
        traversal = g.with_("batchSize", 8).V()

        self.assertEqual(await rc.submit_async(traversal), ["marko"])

        rc._async_client.submit.assert_awaited_once_with(
            traversal.bytecode, request_options={"evaluationTimeout": 1234, "batchSize": 8}
        )

    async def test_explicit_empty_options_do_not_extract_traversal_options(self):
        rc = self._make_rc()
        traversal = async_traversal().with_remote(rc).with_("evaluationTimeout", 1234).V()

        await rc.submit_async(traversal, request_options={})

        rc._async_client.submit.assert_awaited_once_with(traversal.bytecode, request_options={})


# ---------------------------------------------------------------------------
# submit_stream() — returns AsyncResultSet
# ---------------------------------------------------------------------------

class TestSubmitStream(_AsyncioTestCase):
    async def test_returns_async_result_set(self):
        rc = AsyncDriverRemoteConnection(URL, TRAVERSAL_SOURCE)
        q = asyncio.Queue()
        q.put_nowait([Traverser("x"), Traverser("y")])
        rs = AsyncResultSet(q, "req")
        fut = asyncio.get_event_loop().create_future()
        fut.set_result(None)
        rs.future = fut
        rc._async_client.submit = AsyncMock(return_value=rs)

        result = await rc.submit_stream(Bytecode())
        self.assertIsInstance(result, AsyncResultSet)

    async def test_stream_iterable(self):
        rc = AsyncDriverRemoteConnection(URL, TRAVERSAL_SOURCE)
        q = asyncio.Queue()
        q.put_nowait([Traverser("a"), Traverser("b")])
        rs = AsyncResultSet(q, "req")
        fut = asyncio.get_event_loop().create_future()
        fut.set_result(None)
        rs.future = fut
        rc._async_client.submit = AsyncMock(return_value=rs)

        collected = []
        async for item in await rc.submit_stream(Bytecode()):
            collected.append(item)
        # AsyncResultSet yields Traverser objects (raw, not unwrapped)
        self.assertEqual(len(collected), 2)


# ---------------------------------------------------------------------------
# DSL integration via async_traversal()
# ---------------------------------------------------------------------------

class TestDSLIntegration(_AsyncioTestCase):
    async def test_g_v_to_list(self):
        rc = AsyncDriverRemoteConnection(URL, TRAVERSAL_SOURCE)
        _patch_submit(rc, values=[6])
        g = async_traversal().with_remote(rc)
        results = await g.V().count().to_list()
        self.assertEqual(results, [6])

    async def test_g_v_next(self):
        rc = AsyncDriverRemoteConnection(URL, TRAVERSAL_SOURCE)
        _patch_submit(rc, values=[42])
        g = async_traversal().with_remote(rc)
        result = await g.V().count().next()
        self.assertEqual(result, 42)

    async def test_g_add_v_iterate(self):
        rc = AsyncDriverRemoteConnection(URL, TRAVERSAL_SOURCE)
        _patch_submit(rc, values=[])
        g = async_traversal().with_remote(rc)
        await g.add_v("planet").property("name", "saturn").iterate()
        rc.submit.assert_awaited_once()

    async def test_g_has_next_true(self):
        rc = AsyncDriverRemoteConnection(URL, TRAVERSAL_SOURCE)
        _patch_submit(rc, values=["marko"])
        g = async_traversal().with_remote(rc)
        self.assertTrue(await g.V().has("name", "marko").has_next())

    async def test_g_has_next_false(self):
        rc = AsyncDriverRemoteConnection(URL, TRAVERSAL_SOURCE)
        _patch_submit(rc, values=[])
        g = async_traversal().with_remote(rc)
        self.assertFalse(await g.V().has("name", "nobody").has_next())

    async def test_concurrent_gather(self):
        call_results = [["marko", "vadas"], [6]]
        call_index = 0

        async def mock_submit(bc):
            nonlocal call_index
            rs = _make_result_set(call_results[call_index])
            call_index += 1
            return AsyncRemoteTraversal(rs)

        rc = AsyncDriverRemoteConnection(URL, TRAVERSAL_SOURCE)
        rc.submit = mock_submit
        g = async_traversal().with_remote(rc)

        names, count = await asyncio.gather(
            g.V().has_label("person").values("name").to_list(),
            g.V().count().next(),
        )
        self.assertEqual(names, ["marko", "vadas"])
        self.assertEqual(count, 6)


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

class TestLifecycle(_AsyncioTestCase):
    async def test_close_delegates_to_client(self):
        rc = AsyncDriverRemoteConnection(URL, TRAVERSAL_SOURCE)
        rc._async_client.close = AsyncMock()
        await rc.close()
        rc._async_client.close.assert_awaited_once()

    async def test_context_manager_calls_close(self):
        rc = AsyncDriverRemoteConnection(URL, TRAVERSAL_SOURCE)
        rc._async_client.close = AsyncMock()
        async with rc:
            pass
        rc._async_client.close.assert_awaited_once()

    async def test_context_manager_calls_close_on_exception(self):
        rc = AsyncDriverRemoteConnection(URL, TRAVERSAL_SOURCE)
        rc._async_client.close = AsyncMock()
        with self.assertRaises(ValueError):
            async with rc:
                raise ValueError("boom")
        rc._async_client.close.assert_awaited_once()

    async def test_session_connection_does_not_rollback_on_close(self):
        rc = AsyncDriverRemoteConnection(URL, TRAVERSAL_SOURCE, session="sid")
        rc._async_client.close = AsyncMock()
        rc.rollback = AsyncMock()
        await rc.close()
        rc.rollback.assert_not_awaited()
        rc._async_client.close.assert_awaited_once()

    async def test_is_closed_reflects_client_state(self):
        rc = AsyncDriverRemoteConnection(URL, TRAVERSAL_SOURCE)
        rc._async_client._closed = True
        self.assertTrue(rc.is_closed())
        rc._async_client._closed = False
        self.assertFalse(rc.is_closed())

    async def test_child_close_failure_still_closes_other_children_and_client(self):
        rc = AsyncDriverRemoteConnection(URL, TRAVERSAL_SOURCE)
        first = rc.create_session()
        second = rc.create_session()
        first.close = AsyncMock(side_effect=OSError("child close failed"))
        second.close = AsyncMock()
        rc._async_client.close = AsyncMock()

        with self.assertRaisesRegex(OSError, "child close failed"):
            await rc.close()

        second.close.assert_awaited_once()
        rc._async_client.close.assert_awaited_once()
        self.assertEqual(rc._AsyncDriverRemoteConnection__spawned_sessions, [])

    async def test_child_cancellation_still_closes_other_children_and_client(self):
        rc = AsyncDriverRemoteConnection(URL, TRAVERSAL_SOURCE)
        first = rc.create_session()
        second = rc.create_session()
        first.close = AsyncMock(side_effect=asyncio.CancelledError())
        second.close = AsyncMock()
        rc._async_client.close = AsyncMock()

        with self.assertRaises(asyncio.CancelledError):
            await rc.close()

        second.close.assert_awaited_once()
        rc._async_client.close.assert_awaited_once()
        self.assertEqual(rc._AsyncDriverRemoteConnection__spawned_sessions, [])


# ---------------------------------------------------------------------------
# Transactions
# ---------------------------------------------------------------------------

class TestTransactions(_AsyncioTestCase):
    async def test_commit_calls_client_submit(self):
        rc = AsyncDriverRemoteConnection(URL, TRAVERSAL_SOURCE)
        rs = _make_result_set([])
        rc._async_client.submit = AsyncMock(return_value=rs)
        await rc.commit()
        submitted_bc = rc._async_client.submit.call_args[0][0]
        self.assertIsInstance(submitted_bc, Bytecode)

    async def test_rollback_calls_client_submit(self):
        rc = AsyncDriverRemoteConnection(URL, TRAVERSAL_SOURCE)
        rs = _make_result_set([])
        rc._async_client.submit = AsyncMock(return_value=rs)
        await rc.rollback()
        submitted_bc = rc._async_client.submit.call_args[0][0]
        self.assertIsInstance(submitted_bc, Bytecode)


# ---------------------------------------------------------------------------
# Session management
# ---------------------------------------------------------------------------

class TestSessionManagement(_AsyncioTestCase):
    def test_create_session_returns_session_bound_connection(self):
        rc = AsyncDriverRemoteConnection(URL, TRAVERSAL_SOURCE)
        session_rc = rc.create_session()
        self.assertIsInstance(session_rc, AsyncDriverRemoteConnection)
        self.assertTrue(session_rc.is_session_bound())

    def test_create_session_raises_if_already_session_bound(self):
        rc = AsyncDriverRemoteConnection(URL, TRAVERSAL_SOURCE, session="sid")
        with self.assertRaises(Exception):
            rc.create_session()

    def test_create_session_child_is_different_instance(self):
        rc = AsyncDriverRemoteConnection(URL, TRAVERSAL_SOURCE)
        session_rc = rc.create_session()
        self.assertIsNot(rc, session_rc)

    def test_create_session_forwards_connection_options(self):
        rc = AsyncDriverRemoteConnection(
            URL,
            TRAVERSAL_SOURCE,
            username="stephen",
            password="password",
            headers={"X-Custom": "val"},
            enable_user_agent_on_connect=False,
            enable_compression=True,
            ping_interval=15,
            query_timeout=10,
        )
        session_rc = rc.create_session()
        self.assertEqual(session_rc._username, "stephen")
        self.assertEqual(session_rc._password, "password")
        self.assertEqual(session_rc._headers, {"X-Custom": "val"})
        self.assertFalse(session_rc._enable_user_agent_on_connect)
        self.assertTrue(session_rc._enable_compression)
        self.assertEqual(session_rc._ping_interval, 15)
        self.assertEqual(session_rc._query_timeout, 10)
        self.assertEqual(session_rc._async_client._pool_size, 1)

    async def test_remove_session_untracks_connection(self):
        rc = AsyncDriverRemoteConnection(URL, TRAVERSAL_SOURCE)
        session_rc = rc.create_session()
        session_rc._async_client.close = AsyncMock()
        self.assertIn(session_rc, rc._AsyncDriverRemoteConnection__spawned_sessions)
        await rc.remove_session(session_rc)
        self.assertNotIn(session_rc, rc._AsyncDriverRemoteConnection__spawned_sessions)
        session_rc._async_client.close.assert_awaited_once()

    async def test_remove_session_untracks_connection_when_close_fails(self):
        rc = AsyncDriverRemoteConnection(URL, TRAVERSAL_SOURCE)
        session_rc = rc.create_session()
        session_rc.close = AsyncMock(side_effect=OSError("child close failed"))

        with self.assertRaisesRegex(OSError, "child close failed"):
            await rc.remove_session(session_rc)

        self.assertNotIn(session_rc, rc._AsyncDriverRemoteConnection__spawned_sessions)


# ---------------------------------------------------------------------------
# _extract_request_options
# ---------------------------------------------------------------------------

class TestExtractRequestOptions(unittest.TestCase):
    def test_no_options_strategy_returns_none(self):
        bc = Bytecode()
        self.assertIsNone(AsyncDriverRemoteConnection._extract_request_options(bc))

    def test_options_strategy_extracted(self):
        bc = Bytecode()
        strategy = OptionsStrategy(evaluationTimeout=1000, batchSize=50)
        bc.add_source("withStrategies", strategy)
        opts = AsyncDriverRemoteConnection._extract_request_options(bc)
        self.assertEqual(opts["evaluationTimeout"], 1000)
        self.assertEqual(opts["batchSize"], 50)

    def test_unknown_keys_excluded(self):
        bc = Bytecode()
        strategy = OptionsStrategy(evaluationTimeout=500)
        strategy.configuration["unknownKey"] = "value"
        bc.add_source("withStrategies", strategy)
        opts = AsyncDriverRemoteConnection._extract_request_options(bc)
        self.assertNotIn("unknownKey", opts)

    def test_partial_keys(self):
        bc = Bytecode()
        strategy = OptionsStrategy(requestId="abc-123")
        bc.add_source("withStrategies", strategy)
        opts = AsyncDriverRemoteConnection._extract_request_options(bc)
        self.assertEqual(opts["requestId"], "abc-123")
        self.assertNotIn("evaluationTimeout", opts)


if __name__ == "__main__":
    unittest.main()
