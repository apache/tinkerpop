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
"""Unit tests for PR 1: AsyncAiohttpHTTPTransport, AsyncBufferedStream,
GraphBinarySerializersV4.deserialize_async_response_stream, and AsyncResultSet.
"""
import asyncio
import ssl
import struct
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp

from gremlin_python.driver.aiohttp.async_transport import (
    AsyncAiohttpHTTPTransport,
    AsyncBufferedStream,
    _NeedMoreData,
)
from gremlin_python.driver.async_resultset import AsyncResultSet, _EXHAUSTED, _STREAM_END
from gremlin_python.driver.connection import GremlinServerError
from gremlin_python.driver.exceptions import ReadTimeoutError
from gremlin_python.driver.serializer import GraphBinarySerializersV4
from gremlin_python.process.traversal import Traverser


# ---------------------------------------------------------------------------
# Helpers for GraphBinary v4 binary stream construction
# ---------------------------------------------------------------------------

def _gb_int32(value):
    # DataType.int (0x01) + nullable false (0x00) + 4-byte signed big-endian
    return bytes([0x01, 0x00]) + struct.pack(">i", value)


def _gb_int64(value):
    # DataType.long (0x02) + nullable false (0x00) + 8-byte signed big-endian
    return bytes([0x02, 0x00]) + struct.pack(">q", value)


def _gb_string(value):
    encoded = value.encode("utf-8")
    return bytes([0x03, 0x00]) + struct.pack(">i", len(encoded)) + encoded


def _gb_end_of_stream():
    # DataType.marker (0xFD) + nullable false (0x00) + value 0x00
    return bytes([0xFD, 0x00, 0x00])


def _gb_trailer(status_code=200, message=None, exception=None):
    buf = bytearray(struct.pack(">i", status_code))
    if message is None:
        buf.append(0x01)
    else:
        encoded = message.encode("utf-8")
        buf.append(0x00)
        buf.extend(struct.pack(">i", len(encoded)))
        buf.extend(encoded)
    if exception is None:
        buf.append(0x01)
    else:
        encoded = exception.encode("utf-8")
        buf.append(0x00)
        buf.extend(struct.pack(">i", len(encoded)))
        buf.extend(encoded)
    return bytes(buf)


def _build_gb_response(items_bytes, *, bulked=False, status_code=200,
                       message=None, exception=None):
    header = bytes([0x84, 0x01 if bulked else 0x00])
    body = b"".join(items_bytes) + _gb_end_of_stream()
    trailer = _gb_trailer(status_code, message, exception)
    return header + body + trailer


def _make_async_stream(chunks, read_timeout=None):
    chunk_iter = iter(chunks)
    mock_response = MagicMock()
    mock_response.content.read = AsyncMock(
        side_effect=lambda _n: next(chunk_iter, b"")
    )
    return AsyncBufferedStream(mock_response, read_timeout), mock_response


class _AsyncioTestCase(unittest.IsolatedAsyncioTestCase):
    """IsolatedAsyncioTestCase that resets the event loop policy after each test
    so subsequent synchronous tests calling ``asyncio.get_event_loop()`` on the
    main thread do not fail with ``RuntimeError: There is no current event loop``."""

    def run(self, result=None):
        try:
            return super().run(result)
        finally:
            asyncio.set_event_loop_policy(None)


# ---------------------------------------------------------------------------
# AsyncResultSet tests
# ---------------------------------------------------------------------------

class TestAsyncResultSet(_AsyncioTestCase):

    def _make_done_rs(self, items, error=None):
        q = asyncio.Queue()
        for item in items:
            q.put_nowait(item)
        q.put_nowait(_STREAM_END)
        rs = AsyncResultSet(q, "req-1")
        fut = asyncio.get_running_loop().create_future()
        if error is not None:
            fut.set_exception(error)
        else:
            fut.set_result(None)
        rs.done = fut
        return rs

    async def test_one_returns_single_items_in_order(self):
        rs = self._make_done_rs([1, 2, 3])
        self.assertEqual(await rs.one(), 1)
        self.assertEqual(await rs.one(), 2)
        self.assertEqual(await rs.one(), 3)
        self.assertIs(await rs.one(), _EXHAUSTED)

    async def test_one_returns_list_valued_item_intact(self):
        """A result item that is itself a list (e.g. from fold()) is returned intact."""
        rs = self._make_done_rs([[10, 20, 30]])
        self.assertEqual(await rs.one(), [10, 20, 30])
        self.assertIs(await rs.one(), _EXHAUSTED)

    async def test_one_preserves_none_value(self):
        rs = self._make_done_rs([None])
        self.assertIsNone(await rs.one())
        self.assertIs(await rs.one(), _EXHAUSTED)

    async def test_all_collects_every_item(self):
        rs = self._make_done_rs(["a", "b", "c"])
        self.assertEqual(await rs.all(), ["a", "b", "c"])

    async def test_all_preserves_none_values(self):
        rs = self._make_done_rs([1, None, 2])
        self.assertEqual(await rs.all(), [1, None, 2])

    async def test_all_empty_stream(self):
        rs = self._make_done_rs([])
        self.assertEqual(await rs.all(), [])

    async def test_async_for_iteration(self):
        rs = self._make_done_rs([10, 20, 30])
        collected = [x async for x in rs]
        self.assertEqual(collected, [10, 20, 30])

    async def test_mid_stream_error_raises_after_buffered_items(self):
        err = GremlinServerError({"code": 500, "message": "boom", "exception": "Err"})
        rs = self._make_done_rs([1, 2], error=err)
        self.assertEqual(await rs.one(), 1)
        self.assertEqual(await rs.one(), 2)
        with self.assertRaises(GremlinServerError):
            await rs.one()

    async def test_one_waits_for_running_producer_task_without_polling(self):
        q = asyncio.Queue()
        rs = AsyncResultSet(q, "req-1")

        async def _producer():
            await asyncio.sleep(0.02)
            q.put_nowait(42)
            q.put_nowait(_STREAM_END)

        rs.done = asyncio.create_task(_producer())
        self.assertEqual(await rs.one(), 42)
        self.assertIs(await rs.one(), _EXHAUSTED)

    async def test_sync_iter_and_next_raise(self):
        rs = self._make_done_rs([1])
        with self.assertRaises(NotImplementedError):
            iter(rs)
        with self.assertRaises(NotImplementedError):
            next(rs)

    async def test_status_attributes_set_once(self):
        rs = self._make_done_rs([])
        rs.status_attributes = {"host": "localhost"}
        self.assertEqual(rs.status_attributes["host"], "localhost")
        with self.assertRaises(RuntimeError):
            rs.status_attributes = {"other": 1}


# ---------------------------------------------------------------------------
# AsyncBufferedStream tests
# ---------------------------------------------------------------------------

class TestAsyncBufferedStream(_AsyncioTestCase):

    async def test_read_exactly_serves_multiple_calls_from_single_chunk(self):
        stream, mock_resp = _make_async_stream([b"abcdefghij"])
        self.assertEqual(await stream.read_exactly(3), b"abc")
        self.assertEqual(await stream.read_exactly(4), b"defg")
        self.assertEqual(await stream.read_exactly(3), b"hij")
        self.assertEqual(mock_resp.content.read.await_count, 1)

    async def test_read_exactly_refills_across_chunks(self):
        stream, mock_resp = _make_async_stream([b"abc", b"defgh", b"ij"])
        self.assertEqual(await stream.read_exactly(10), b"abcdefghij")
        self.assertEqual(mock_resp.content.read.await_count, 3)

    async def test_read_zero_or_negative_returns_empty_bytes(self):
        stream, mock_resp = _make_async_stream([b"abc"])
        self.assertEqual(stream.read(0), b"")
        self.assertEqual(stream.read(-5), b"")
        self.assertEqual(await stream.read_exactly(0), b"")
        self.assertEqual(mock_resp.content.read.await_count, 0)

    async def test_checkpoint_and_restore_on_need_more_data(self):
        stream, _ = _make_async_stream([b"ab", b"cdef"])
        await stream.fill_more(2)
        cp = stream.checkpoint()
        self.assertEqual(stream.read(1), b"a")
        with self.assertRaises(_NeedMoreData) as ctx:
            stream.read(4)
        self.assertEqual(ctx.exception.missing, 3)
        stream.restore(cp)
        await stream.fill_more(ctx.exception.missing)
        self.assertEqual(stream.read(5), b"abcde")

    async def test_incomplete_read_raises_with_partial_and_expected(self):
        stream, _ = _make_async_stream([b"ab"])
        with self.assertRaises(asyncio.IncompleteReadError) as ctx:
            await stream.read_exactly(5)
        self.assertEqual(ctx.exception.partial, b"ab")
        self.assertEqual(ctx.exception.expected, 5)

    async def test_empty_response_raises_incomplete_read(self):
        stream, _ = _make_async_stream([])
        with self.assertRaises(asyncio.IncompleteReadError) as ctx:
            await stream.read_exactly(1)
        self.assertEqual(ctx.exception.partial, b"")
        self.assertEqual(ctx.exception.expected, 1)

    async def test_server_timeout_converted_to_read_timeout_error(self):
        mock_response = MagicMock()
        mock_response.content.read = AsyncMock(
            side_effect=aiohttp.ServerTimeoutError("Timeout on reading data")
        )
        stream = AsyncBufferedStream(mock_response, read_timeout=5.0)
        with self.assertRaises(ReadTimeoutError) as ctx:
            await stream.read_exactly(1)
        self.assertIsInstance(ctx.exception, TimeoutError)
        self.assertIn("5.0s", str(ctx.exception))


# ---------------------------------------------------------------------------
# GraphBinarySerializersV4.deserialize_async_response_stream tests
# ---------------------------------------------------------------------------

class TestDeserializeAsyncResponseStream(_AsyncioTestCase):

    async def test_non_bulked_stream_yields_values(self):
        raw = _build_gb_response([_gb_int32(10), _gb_int32(20), _gb_string("hello")])
        stream, _ = _make_async_stream([raw])
        serializer = GraphBinarySerializersV4()
        items = [
            item async for item in serializer.deserialize_async_response_stream(stream)
        ]
        self.assertEqual(items, [10, 20, "hello"])

    async def test_bulked_stream_yields_traversers(self):
        raw = _build_gb_response(
            [_gb_string("marko"), _gb_int64(3), _gb_string("vadas"), _gb_int64(1)],
            bulked=True,
        )
        stream, _ = _make_async_stream([raw])
        serializer = GraphBinarySerializersV4()
        items = [
            item async for item in serializer.deserialize_async_response_stream(stream)
        ]
        self.assertEqual(len(items), 2)
        self.assertIsInstance(items[0], Traverser)
        self.assertEqual(items[0].object, "marko")
        self.assertEqual(items[0].bulk, 3)
        self.assertEqual(items[1].object, "vadas")
        self.assertEqual(items[1].bulk, 1)

    async def test_fragmented_tiny_chunks_across_object_boundaries(self):
        """Split a GraphBinary stream into 2-byte chunks to verify checkpoint/restore."""
        raw = _build_gb_response(
            [_gb_string("fragmented-string-value"), _gb_int32(999), _gb_string("end")]
        )
        tiny_chunks = [raw[i:i + 2] for i in range(0, len(raw), 2)]
        stream, _ = _make_async_stream(tiny_chunks)
        serializer = GraphBinarySerializersV4()
        items = [
            item async for item in serializer.deserialize_async_response_stream(stream)
        ]
        self.assertEqual(items, ["fragmented-string-value", 999, "end"])

    async def test_non_200_trailer_raises_after_yielding_items(self):
        raw = _build_gb_response(
            [_gb_int32(1), _gb_int32(2)],
            status_code=500,
            message="Server error mid-stream",
            exception="org.apache.tinkerpop.BoomException",
        )
        # Also split the trailer across chunks
        chunks = [raw[:12], raw[12:25], raw[25:]]
        stream, _ = _make_async_stream(chunks)
        serializer = GraphBinarySerializersV4()
        yielded = []
        with self.assertRaises(GremlinServerError) as ctx:
            async for item in serializer.deserialize_async_response_stream(stream):
                yielded.append(item)
        self.assertEqual(yielded, [1, 2])
        self.assertEqual(ctx.exception.status_code, 500)
        self.assertIn("Server error mid-stream", str(ctx.exception))


# ---------------------------------------------------------------------------
# AsyncAiohttpHTTPTransport tests
# ---------------------------------------------------------------------------

class TestAsyncAiohttpHTTPTransport(_AsyncioTestCase):

    async def test_never_uses_nest_asyncio_or_new_event_loop(self):
        with patch("asyncio.new_event_loop") as mock_new_loop:
            transport = AsyncAiohttpHTTPTransport(call_from_event_loop=True)
            mock_new_loop.assert_not_called()
            self.assertFalse(hasattr(transport, "_loop"))

    @patch("gremlin_python.driver.aiohttp.async_transport.aiohttp.TCPConnector")
    @patch("gremlin_python.driver.aiohttp.async_transport.aiohttp.ClientSession")
    async def test_connect_creates_session_on_running_loop(self, mock_session_cls, mock_connector_cls):
        mock_session = AsyncMock()
        mock_session.closed = False
        mock_session_cls.return_value = mock_session

        ssl_ctx = ssl.create_default_context()
        transport = AsyncAiohttpHTTPTransport(
            connect_timeout_millis=3000,
            read_timeout_millis=10000,
            idle_timeout_millis=60000,
            max_connections=32,
            ssl=ssl_ctx,
            trust_env=True,
        )
        await transport.connect("http://localhost:8182/gremlin", headers={"User-Agent": "Test"})

        mock_connector_cls.assert_called_once()
        conn_kwargs = mock_connector_cls.call_args.kwargs
        self.assertIs(conn_kwargs["ssl_context"], ssl_ctx)
        self.assertEqual(conn_kwargs["keepalive_timeout"], 60.0)
        self.assertEqual(conn_kwargs["limit"], 32)

        mock_session_cls.assert_called_once()
        sess_kwargs = mock_session_cls.call_args.kwargs
        self.assertEqual(sess_kwargs["headers"], {"User-Agent": "Test"})
        self.assertTrue(sess_kwargs["trust_env"])
        self.assertEqual(sess_kwargs["timeout"].sock_connect, 3.0)
        self.assertEqual(sess_kwargs["timeout"].sock_read, 10.0)
        self.assertFalse(transport.closed)

        await transport.close()
        mock_session.close.assert_awaited_once()

    async def test_write_posts_payload_with_deflate_and_proxy(self):
        transport = AsyncAiohttpHTTPTransport(
            compression="deflate",
            proxy="http://proxy.local:8080",
        )
        mock_session = AsyncMock()
        mock_session.closed = False
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.headers = {"content-type": "application/vnd.graphbinary-v4.0"}
        mock_session.post = AsyncMock(return_value=mock_resp)
        transport._client_session = mock_session
        transport._url = "http://localhost:8182/gremlin"

        headers = {"content-type": "application/json"}
        await transport.write({"payload": b'{"gremlin":"g.V()"}', "headers": headers})

        self.assertEqual(headers["accept-encoding"], "deflate")
        mock_session.post.assert_awaited_once_with(
            url="http://localhost:8182/gremlin",
            data=b'{"gremlin":"g.V()"}',
            headers=headers,
            proxy="http://proxy.local:8080",
        )
        self.assertEqual(transport.status_code, 200)
        self.assertEqual(transport.content_type, "application/vnd.graphbinary-v4.0")

    async def test_write_with_compression_none_skips_auto_accept_encoding(self):
        transport = AsyncAiohttpHTTPTransport(compression="none")
        mock_session = AsyncMock()
        mock_session.closed = False
        mock_session.post = AsyncMock(return_value=MagicMock(status=200))
        transport._client_session = mock_session
        transport._url = "http://localhost:8182/gremlin"

        headers = {"content-type": "application/json"}
        await transport.write({"payload": b"{}", "headers": headers})

        self.assertNotIn("accept-encoding", headers)
        mock_session.post.assert_awaited_once_with(
            url="http://localhost:8182/gremlin",
            data=b"{}",
            headers=headers,
            skip_auto_headers=["Accept-Encoding"],
        )

    async def test_evict_response_closes_active_response(self):
        transport = AsyncAiohttpHTTPTransport()
        mock_resp = MagicMock()
        transport._http_req_resp = mock_resp
        transport.evict_response()
        mock_resp.close.assert_called_once()
        self.assertIsNone(transport._http_req_resp)


if __name__ == "__main__":
    unittest.main()
