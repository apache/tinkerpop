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
"""Unit tests for the native-async WebSocket driver components.

All tests use :class:`unittest.IsolatedAsyncioTestCase` so no extra
test dependencies are required.  Network I/O is replaced with
:mod:`unittest.mock` mocks throughout.
"""
import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp

from gremlin_python.driver.aiohttp.async_transport import AsyncAiohttpWSTransport
from gremlin_python.driver.async_protocol import AsyncGremlinServerWSProtocol
from gremlin_python.driver.async_resultset import AsyncResultSet, _EXHAUSTED
from gremlin_python.driver.async_connection import AsyncConnection
from gremlin_python.driver.async_client import AsyncClient
from gremlin_python.driver.protocol import GremlinServerError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_ws_message(msg_type, data=b"payload"):
    msg = MagicMock()
    msg.type = msg_type
    msg.data = data
    return msg


def _make_done_future(result=None, *, loop=None):
    """Return an already-resolved asyncio.Future."""
    fut = asyncio.get_event_loop().create_future() if loop is None else loop.create_future()
    fut.set_result(result)
    return fut


# ---------------------------------------------------------------------------
# AsyncAiohttpWSTransport
# ---------------------------------------------------------------------------

class TestAsyncAiohttpWSTransport(unittest.IsolatedAsyncioTestCase):

    async def _make_transport(self, **kwargs):
        t = AsyncAiohttpWSTransport(**kwargs)
        return t

    # ---- connect -----------------------------------------------------------

    async def test_connect_opens_session_and_websocket(self):
        transport = await self._make_transport()
        mock_ws = AsyncMock()
        mock_session = MagicMock()
        mock_session.ws_connect = AsyncMock(return_value=mock_ws)
        mock_session.closed = False

        with patch("aiohttp.ClientSession", return_value=mock_session):
            await transport.connect("ws://localhost:8182/gremlin")

        mock_session.ws_connect.assert_awaited_once_with(
            "ws://localhost:8182/gremlin", headers=None
        )
        self.assertIs(transport._websocket, mock_ws)

    async def test_connect_403_raises_friendly_message(self):
        transport = await self._make_transport()
        err = aiohttp.ClientResponseError(request_info=MagicMock(), history=())
        err.status = 403

        mock_session = MagicMock()
        mock_session.ws_connect = AsyncMock(side_effect=err)
        mock_session.closed = False

        with patch("aiohttp.ClientSession", return_value=mock_session):
            with self.assertRaises(Exception) as ctx:
                await transport.connect("ws://localhost:8182/gremlin")
        self.assertIn("403", str(ctx.exception))

    async def test_connect_non_403_reraises(self):
        transport = await self._make_transport()
        err = aiohttp.ClientResponseError(request_info=MagicMock(), history=())
        err.status = 500

        mock_session = MagicMock()
        mock_session.ws_connect = AsyncMock(side_effect=err)
        mock_session.closed = False

        with patch("aiohttp.ClientSession", return_value=mock_session):
            with self.assertRaises(aiohttp.ClientResponseError):
                await transport.connect("ws://localhost:8182/gremlin")

    async def test_connect_forwards_extra_kwargs(self):
        transport = await self._make_transport(max_msg_size=1024)
        mock_ws = AsyncMock()
        mock_session = MagicMock()
        mock_session.ws_connect = AsyncMock(return_value=mock_ws)
        mock_session.closed = False

        with patch("aiohttp.ClientSession", return_value=mock_session):
            await transport.connect("ws://localhost:8182/gremlin", headers={"X-Foo": "bar"})

        mock_session.ws_connect.assert_awaited_once_with(
            "ws://localhost:8182/gremlin",
            headers={"X-Foo": "bar"},
            max_msg_size=1024,
        )

    async def test_connect_remaps_max_content_length(self):
        transport = AsyncAiohttpWSTransport(max_content_length=512)
        self.assertIn("max_msg_size", transport._aiohttp_kwargs)
        self.assertNotIn("max_content_length", transport._aiohttp_kwargs)
        self.assertEqual(transport._aiohttp_kwargs["max_msg_size"], 512)

    async def test_connect_remaps_ssl_options(self):
        ssl_ctx = MagicMock()
        transport = AsyncAiohttpWSTransport(ssl_options=ssl_ctx)
        self.assertIn("ssl", transport._aiohttp_kwargs)
        self.assertNotIn("ssl_options", transport._aiohttp_kwargs)
        self.assertIs(transport._aiohttp_kwargs["ssl"], ssl_ctx)

    # ---- write -------------------------------------------------------------

    async def test_write_sends_bytes(self):
        transport = await self._make_transport()
        transport._websocket = AsyncMock()
        await transport.write(b"hello")
        transport._websocket.send_bytes.assert_awaited_once_with(b"hello")

    # ---- read --------------------------------------------------------------

    async def test_read_binary_returns_bytes(self):
        transport = await self._make_transport()
        transport._websocket = AsyncMock()
        transport._websocket.receive = AsyncMock(
            return_value=_make_ws_message(aiohttp.WSMsgType.binary, b"\x01\x02")
        )
        result = await transport.read()
        self.assertEqual(result, b"\x01\x02")

    async def test_read_text_encodes_to_utf8(self):
        transport = await self._make_transport()
        transport._websocket = AsyncMock()
        msg = _make_ws_message(aiohttp.WSMsgType.text, "hello world")
        transport._websocket.receive = AsyncMock(return_value=msg)
        result = await transport.read()
        self.assertEqual(result, b"hello world")

    async def test_read_close_raises_runtime_error(self):
        transport = await self._make_transport()
        transport._websocket = AsyncMock()
        transport._websocket.closed = False
        transport._client_session = AsyncMock()
        transport._client_session.closed = False
        transport._websocket.receive = AsyncMock(
            return_value=_make_ws_message(aiohttp.WSMsgType.close)
        )
        with self.assertRaises(RuntimeError) as ctx:
            await transport.read()
        self.assertIn("closed by server", str(ctx.exception))

    async def test_read_error_raises_runtime_error(self):
        transport = await self._make_transport()
        transport._websocket = AsyncMock()
        transport._websocket.receive = AsyncMock(
            return_value=_make_ws_message(aiohttp.WSMsgType.error, "boom")
        )
        with self.assertRaises(RuntimeError) as ctx:
            await transport.read()
        self.assertIn("boom", str(ctx.exception))

    # ---- ping --------------------------------------------------------------

    async def test_ping_calls_websocket_ping(self):
        transport = await self._make_transport()
        transport._websocket = AsyncMock()
        await transport.ping()
        transport._websocket.ping.assert_awaited_once()

    # ---- close -------------------------------------------------------------

    async def test_close_closes_websocket_and_session(self):
        transport = await self._make_transport()
        transport._websocket = AsyncMock()
        transport._websocket.closed = False
        transport._client_session = AsyncMock()
        transport._client_session.closed = False
        await transport.close()
        transport._websocket.close.assert_awaited_once()
        transport._client_session.close.assert_awaited_once()

    async def test_close_skips_already_closed(self):
        transport = await self._make_transport()
        transport._websocket = MagicMock()
        transport._websocket.closed = True
        transport._client_session = MagicMock()
        transport._client_session.closed = True
        # Should not raise
        await transport.close()

    # ---- closed property ---------------------------------------------------

    async def test_closed_true_when_websocket_none(self):
        transport = await self._make_transport()
        self.assertTrue(transport.closed)

    async def test_closed_false_when_connected(self):
        transport = await self._make_transport()
        transport._websocket = MagicMock(closed=False)
        transport._client_session = MagicMock(closed=False)
        self.assertFalse(transport.closed)


# ---------------------------------------------------------------------------
# AsyncGremlinServerWSProtocol
# ---------------------------------------------------------------------------

class TestAsyncGremlinServerWSProtocol(unittest.IsolatedAsyncioTestCase):

    def _make_protocol(self):
        ser = MagicMock()
        ser.serialize_message = MagicMock(return_value=b"serialized")
        proto = AsyncGremlinServerWSProtocol(ser)
        proto._transport = AsyncMock()
        return proto, ser

    def _make_result_set(self):
        rs = AsyncResultSet(asyncio.Queue(), "req-1")
        rs.future = _make_done_future()
        return rs

    async def test_write_serializes_and_sends(self):
        proto, ser = self._make_protocol()
        msg = MagicMock()
        await proto.write("req-1", msg)
        ser.serialize_message.assert_called_once_with("req-1", msg)
        proto._transport.write.assert_awaited_once_with(b"serialized")

    async def test_data_received_200_puts_data_in_stream(self):
        proto, ser = self._make_protocol()
        rs = self._make_result_set()
        results_dict = {"req-1": rs}
        server_msg = {
            "requestId": "req-1",
            "status": {"code": 200, "attributes": {"x": 1}},
            "result": {"data": [1, 2, 3], "meta": {}},
        }
        ser.deserialize_message = MagicMock(return_value=server_msg)

        status = await proto.data_received(b"raw", results_dict)

        self.assertEqual(status, 200)
        self.assertEqual(rs.stream.get_nowait(), [1, 2, 3])
        self.assertNotIn("req-1", results_dict)
        self.assertEqual(rs.status_attributes, {"x": 1})

    async def test_data_received_204_puts_empty_list(self):
        proto, ser = self._make_protocol()
        rs = self._make_result_set()
        results_dict = {"req-1": rs}
        server_msg = {
            "requestId": "req-1",
            "status": {"code": 204, "attributes": {}},
            "result": {"data": None, "meta": {}},
        }
        ser.deserialize_message = MagicMock(return_value=server_msg)

        status = await proto.data_received(b"raw", results_dict)

        self.assertEqual(status, 204)
        self.assertEqual(rs.stream.get_nowait(), [])
        self.assertNotIn("req-1", results_dict)

    async def test_data_received_206_keeps_entry_in_results_dict(self):
        proto, ser = self._make_protocol()
        rs = self._make_result_set()
        results_dict = {"req-1": rs}
        server_msg = {
            "requestId": "req-1",
            "status": {"code": 206, "attributes": {}},
            "result": {"data": [4, 5], "meta": {}},
        }
        ser.deserialize_message = MagicMock(return_value=server_msg)

        status = await proto.data_received(b"raw", results_dict)

        self.assertEqual(status, 206)
        self.assertIn("req-1", results_dict)
        self.assertEqual(rs.stream.get_nowait(), [4, 5])

    async def test_data_received_error_raises(self):
        proto, ser = self._make_protocol()
        rs = self._make_result_set()
        results_dict = {"req-1": rs}
        server_msg = {
            "requestId": "req-1",
            "status": {"code": 500, "message": "Internal error", "attributes": {}},
            "result": {"data": None, "meta": {}},
        }
        ser.deserialize_message = MagicMock(return_value=server_msg)

        with self.assertRaises(GremlinServerError):
            await proto.data_received(b"raw", results_dict)

    async def test_data_received_none_raises(self):
        proto, _ = self._make_protocol()
        with self.assertRaises(GremlinServerError):
            await proto.data_received(None, {})


# ---------------------------------------------------------------------------
# AsyncResultSet
# ---------------------------------------------------------------------------

class TestAsyncResultSet(unittest.IsolatedAsyncioTestCase):

    def _make_rs(self, items=None, *, done=True):
        q = asyncio.Queue()
        if items:
            q.put_nowait(items)
        rs = AsyncResultSet(q, "req-1")
        fut = asyncio.get_event_loop().create_future()
        if done:
            fut.set_result(None)
        rs.future = fut
        return rs

    async def test_one_returns_individual_items(self):
        rs = self._make_rs([10, 20, 30])
        self.assertEqual(await rs.one(), 10)
        self.assertEqual(await rs.one(), 20)
        self.assertEqual(await rs.one(), 30)
        result = await rs.one()
        self.assertIs(result, _EXHAUSTED)

    async def test_one_empty_chunk_returns_exhausted(self):
        rs = self._make_rs([])
        result = await rs.one()
        self.assertIs(result, _EXHAUSTED)

    async def test_all_returns_flat_list(self):
        q = asyncio.Queue()
        q.put_nowait([1, 2])
        q.put_nowait([3, 4])
        rs = AsyncResultSet(q, "req-1")
        fut = asyncio.get_event_loop().create_future()
        fut.set_result(None)
        rs.future = fut
        self.assertEqual(await rs.all(), [1, 2, 3, 4])

    async def test_all_empty_stream(self):
        rs = self._make_rs(None)
        self.assertEqual(await rs.all(), [])

    async def test_async_iteration(self):
        rs = self._make_rs([100, 200])
        collected = []
        async for item in rs:
            collected.append(item)
        self.assertEqual(collected, [100, 200])

    async def test_sync_iter_raises(self):
        rs = self._make_rs([1])
        with self.assertRaises(NotImplementedError):
            iter(rs)

    async def test_sync_next_raises(self):
        rs = self._make_rs([1])
        with self.assertRaises(NotImplementedError):
            next(rs)

    async def test_one_with_running_task(self):
        """one() should wait for the background task to complete."""
        q = asyncio.Queue()
        rs = AsyncResultSet(q, "req-1")

        async def _producer():
            await asyncio.sleep(0.05)
            q.put_nowait([42])

        task = asyncio.create_task(_producer())
        rs.future = task

        result = await rs.one()
        self.assertEqual(result, 42)


# ---------------------------------------------------------------------------
# AsyncConnection
# ---------------------------------------------------------------------------

class TestAsyncConnection(unittest.IsolatedAsyncioTestCase):

    def _make_conn(self):
        pool = asyncio.Queue()
        mock_transport = AsyncMock()
        mock_transport.closed = False
        mock_protocol = AsyncMock()
        mock_protocol.connection_made = MagicMock()

        conn = AsyncConnection(
            url="ws://localhost:8182/gremlin",
            traversal_source="g",
            protocol=mock_protocol,
            transport_factory=lambda: mock_transport,
            executor=None,
            pool=pool,
        )
        conn._transport = mock_transport
        return conn, mock_transport, mock_protocol, pool

    async def test_connect_calls_transport_and_protocol(self):
        conn, transport, protocol, _ = self._make_conn()
        conn._transport = None
        conn._inited = False

        await conn.connect()

        transport.connect.assert_awaited_once_with(
            "ws://localhost:8182/gremlin", conn._headers
        )
        protocol.connection_made.assert_called_once_with(transport)
        self.assertTrue(conn._inited)

    async def test_close_calls_transport(self):
        conn, transport, _, _ = self._make_conn()
        conn._inited = True
        await conn.close()
        transport.close.assert_awaited_once()

    async def test_write_lazy_connects(self):
        conn, transport, protocol, _ = self._make_conn()
        conn._inited = False

        msg = MagicMock()
        msg.args = {}
        protocol.data_received = AsyncMock(return_value=200)
        transport.read = AsyncMock(return_value=b"data")

        # Put an already-resolved future-bearing result set via the protocol mock
        async def fake_data_received(_data, results):
            req_id = list(results.keys())[0]
            rs = results[req_id]
            fut = asyncio.get_running_loop().create_future()
            fut.set_result(None)
            rs.future = fut
            return 200

        protocol.data_received = fake_data_received

        result_set = await conn.write(msg)

        self.assertTrue(conn._inited)
        self.assertIsInstance(result_set, AsyncResultSet)

    async def test_receive_non_206_returns_conn_to_pool(self):
        conn, transport, protocol, pool = self._make_conn()
        conn._inited = True

        rs = AsyncResultSet(asyncio.Queue(), "req-1")
        conn._results["req-1"] = rs

        async def fake_data_received(_data, results):
            rs = results.get("req-1")
            if rs:
                fut = asyncio.get_running_loop().create_future()
                fut.set_result(None)
                rs.future = fut
            return 200

        protocol.data_received = fake_data_received
        transport.read = AsyncMock(return_value=b"data")

        returned = await conn._receive(rs)

        self.assertIs(returned, rs)
        self.assertEqual(pool.qsize(), 1)

    async def test_receive_exception_returns_conn_to_pool(self):
        conn, transport, _, pool = self._make_conn()
        conn._inited = True

        rs = AsyncResultSet(asyncio.Queue(), "req-1")
        transport.read = AsyncMock(side_effect=RuntimeError("boom"))

        with self.assertRaises(RuntimeError):
            await conn._receive(rs)

        self.assertEqual(pool.qsize(), 1)
        self.assertFalse(conn._inited)


# ---------------------------------------------------------------------------
# AsyncClient
# ---------------------------------------------------------------------------

class TestAsyncClient(unittest.IsolatedAsyncioTestCase):

    def _make_client(self, **kwargs):
        # Patch the transport so no real network calls happen
        mock_transport = AsyncMock()
        mock_transport.closed = False

        client = AsyncClient(
            "ws://localhost:8182/gremlin",
            "g",
            transport_factory=lambda: mock_transport,
            **kwargs,
        )
        return client, mock_transport

    async def test_pool_is_asyncio_queue(self):
        client, _ = self._make_client()
        self.assertIsInstance(client._pool, asyncio.Queue)

    async def test_pool_size_default(self):
        client, _ = self._make_client()
        self.assertEqual(client._pool_size, 8)
        self.assertEqual(client._pool.qsize(), 8)

    async def test_pool_size_custom(self):
        client, _ = self._make_client(pool_size=3)
        self.assertEqual(client._pool.qsize(), 3)

    async def test_get_connection_returns_async_connection(self):
        client, _ = self._make_client()
        conn = client._get_connection()
        self.assertIsInstance(conn, AsyncConnection)

    async def test_is_closed_initially_false(self):
        client, _ = self._make_client()
        self.assertFalse(client.is_closed())

    async def test_context_manager_closes(self):
        client, _ = self._make_client()
        async with client:
            pass
        self.assertTrue(client.is_closed())

    async def test_submit_async_raises_when_closed(self):
        client, _ = self._make_client()
        await client.close()
        with self.assertRaises(Exception, msg="Client is closed"):
            await client.submit_async("g.V()")

    async def test_submit_async_string_builds_eval_message(self):
        client, _ = self._make_client()
        mock_conn = AsyncMock()
        mock_rs = MagicMock()
        mock_conn.write = AsyncMock(return_value=mock_rs)
        # Drain pool and put our mock connection
        while not client._pool.empty():
            client._pool.get_nowait()
        client._pool.put_nowait(mock_conn)

        result = await client.submit_async("g.V()")

        written_msg = mock_conn.write.call_args[0][0]
        self.assertEqual(written_msg.op, "eval")
        self.assertEqual(written_msg.processor, "")
        self.assertEqual(written_msg.args["gremlin"], "g.V()")
        self.assertIs(result, mock_rs)

    async def test_submit_async_bytecode_builds_bytecode_message(self):
        from gremlin_python.process.traversal import Bytecode
        client, _ = self._make_client()
        mock_conn = AsyncMock()
        mock_rs = MagicMock()
        mock_conn.write = AsyncMock(return_value=mock_rs)
        while not client._pool.empty():
            client._pool.get_nowait()
        client._pool.put_nowait(mock_conn)

        bc = Bytecode()
        bc.add_step("V")
        await client.submit_async(bc)

        written_msg = mock_conn.write.call_args[0][0]
        self.assertEqual(written_msg.op, "bytecode")
        self.assertEqual(written_msg.processor, "traversal")

    async def test_close_drains_and_closes_connections(self):
        client, _ = self._make_client(pool_size=2)
        mock_conns = []
        while not client._pool.empty():
            mc = AsyncMock()
            mock_conns.append(mc)
            client._pool.get_nowait()
        for mc in mock_conns:
            client._pool.put_nowait(mc)

        await client.close()

        self.assertTrue(client.is_closed())
        for mc in mock_conns:
            mc.close.assert_awaited_once()

    async def test_close_idempotent(self):
        client, _ = self._make_client()
        await client.close()
        await client.close()  # should not raise
        self.assertTrue(client.is_closed())

    async def test_session_forced_pool_size_1(self):
        import uuid
        client, _ = self._make_client(session=uuid.uuid4())
        self.assertEqual(client._pool_size, 1)
        self.assertEqual(client._pool.qsize(), 1)

    async def test_session_pool_size_not_1_raises(self):
        import uuid
        with self.assertRaises(Exception):
            AsyncClient(
                "ws://localhost:8182/gremlin", "g",
                session=uuid.uuid4(), pool_size=2
            )


if __name__ == "__main__":
    unittest.main()
