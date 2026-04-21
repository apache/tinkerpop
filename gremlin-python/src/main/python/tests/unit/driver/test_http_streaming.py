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

"""
Tests for the Python Driver HTTP Streaming Support.

These tests define the expected behavior for:
  - Step 1: AiohttpSyncStream adapter (synchronous file-like wrapper over async response)
  - Step 2: Streaming receive loop in Connection (reads GB objects one-at-a-time)
  - Step 3: ResultSet individual item consumption (queue entries are single items, not lists)
  - Edge cases: mid-stream errors, non-GB responses, bulked results, empty responses
"""

import asyncio
import io
import queue
import struct
from concurrent.futures import Future
from unittest.mock import MagicMock, AsyncMock, patch, PropertyMock

import pytest

from gremlin_python.driver.connection import GremlinServerError
from gremlin_python.driver.request import RequestMessage
from gremlin_python.structure.io.graphbinaryV4 import (
    GraphBinaryWriter, GraphBinaryReader, DataType,
    int32_pack, uint8_pack, int8_pack,
)
from gremlin_python.statics import LongType
from gremlin_python.structure.io.util import Marker


# ---------------------------------------------------------------------------
# Helper: build raw GraphBinary V4 response bytes
# ---------------------------------------------------------------------------

def _gb_typed_value(writer, obj):
    """Serialize a single object with its type code (fully-qualified)."""
    ba = bytearray()
    writer.to_dict(obj, ba)
    return bytes(ba)


def _gb_marker_end_of_stream():
    """Serialize the end-of-stream marker (type_code + nullable_flag + value)."""
    ba = bytearray()
    ba.append(DataType.marker.value)  # 0xfd
    ba.extend(int8_pack(0))           # nullable flag = not null
    ba.extend(int8_pack(0))           # marker value 0 = end_of_stream
    return bytes(ba)


def _gb_nullable_string(s):
    """Encode a nullable string value (no type code, just nullable flag + string bytes)."""
    ba = bytearray()
    if s is None:
        ba.extend(int8_pack(1))  # null flag
    else:
        ba.extend(int8_pack(0))  # not null
        encoded = s.encode('utf-8')
        ba.extend(int32_pack(len(encoded)))
        ba.extend(encoded)
    return bytes(ba)


def _gb_status(code, message=None, exception=None):
    """Encode the status trailer: int32 code + nullable string message + nullable string exception."""
    ba = bytearray()
    ba.extend(int32_pack(code))
    ba.extend(_gb_nullable_string(message))
    ba.extend(_gb_nullable_string(exception))
    return bytes(ba)


def build_gb_response(items, status_code=200, status_message=None, status_exception=None):
    """
    Build a complete GraphBinary V4 streaming response.

    Args:
        items: list of Python objects to serialize as the result payload
        status_code: the status code in the trailer
        status_message: optional status message string
        status_exception: optional status exception string
    """
    writer = GraphBinaryWriter()
    ba = bytearray()

    # Header
    ba.append(0x84)                                  # version byte
    ba.append(0x00)                                  # flags byte

    # Payload: serialized items
    for item in items:
        ba.extend(_gb_typed_value(writer, item))

    # End-of-stream marker
    ba.extend(_gb_marker_end_of_stream())

    # Status trailer
    ba.extend(_gb_status(status_code, status_message, status_exception))

    return bytes(ba)


def build_gb_empty_response():
    """Build a response with no items and 204 status."""
    return build_gb_response([], status_code=204)


def build_gb_bulked_response(items_with_counts, status_code=200, status_message=None, status_exception=None):
    """
    Build a bulked GraphBinary V4 streaming response.

    The server interleaves [object, long(bulk_count), ...] as fully-qualified objects
    when bulkResults=True. The flags byte is 0x01 to indicate bulking.

    Args:
        items_with_counts: list of (object, bulk_count) tuples
    """
    writer = GraphBinaryWriter()
    ba = bytearray()
    ba.append(0x84)  # version byte
    ba.append(0x01)  # flags byte (bulked)
    for item, count in items_with_counts:
        ba.extend(_gb_typed_value(writer, item))
        ba.extend(_gb_typed_value(writer, LongType(count)))
    ba.extend(_gb_marker_end_of_stream())
    ba.extend(_gb_status(status_code, status_message, status_exception))
    return bytes(ba)


# ===========================================================================
# Step 1: AiohttpSyncStream adapter tests
# ===========================================================================

class TestAiohttpSyncStream:
    """
    Tests for the AiohttpSyncStream class that wraps aiohttp's async
    StreamReader as a synchronous file-like object.

    The class should:
    - Have a read(n) method that blocks until exactly n bytes are available
    - Bridge async readexactly(n) to sync via loop.run_until_complete()
    - Raise on timeout
    - Raise asyncio.IncompleteReadError on premature disconnect
    """

    def test_read_returns_exact_bytes(self):
        """read(n) should return exactly n bytes from the async stream."""
        from gremlin_python.driver.aiohttp.transport import AiohttpSyncStream

        loop = asyncio.new_event_loop()
        mock_response = MagicMock()
        mock_response.content = MagicMock()
        mock_response.content.readexactly = AsyncMock(return_value=b'\x01\x02\x03\x04')

        stream = AiohttpSyncStream(mock_response, loop, read_timeout=30)
        result = stream.read(4)

        assert result == b'\x01\x02\x03\x04'
        mock_response.content.readexactly.assert_awaited_once_with(4)
        loop.close()

    def test_read_single_byte(self):
        """read(1) should work for single byte reads (common in GB header parsing)."""
        from gremlin_python.driver.aiohttp.transport import AiohttpSyncStream

        loop = asyncio.new_event_loop()
        mock_response = MagicMock()
        mock_response.content = MagicMock()
        mock_response.content.readexactly = AsyncMock(return_value=b'\x84')

        stream = AiohttpSyncStream(mock_response, loop, read_timeout=30)
        result = stream.read(1)

        assert result == b'\x84'
        loop.close()

    def test_read_multiple_sequential_calls(self):
        """Multiple read() calls should each invoke readexactly independently."""
        from gremlin_python.driver.aiohttp.transport import AiohttpSyncStream

        loop = asyncio.new_event_loop()
        mock_response = MagicMock()
        mock_response.content = MagicMock()
        mock_response.content.readexactly = AsyncMock(side_effect=[b'\x84', b'\x00', b'\x01\x02\x03\x04'])

        stream = AiohttpSyncStream(mock_response, loop, read_timeout=30)
        assert stream.read(1) == b'\x84'
        assert stream.read(1) == b'\x00'
        assert stream.read(4) == b'\x01\x02\x03\x04'
        assert mock_response.content.readexactly.await_count == 3
        loop.close()

    def test_read_raises_on_incomplete_read(self):
        """read() should propagate IncompleteReadError when server disconnects mid-stream."""
        from gremlin_python.driver.aiohttp.transport import AiohttpSyncStream

        loop = asyncio.new_event_loop()
        mock_response = MagicMock()
        mock_response.content = MagicMock()
        mock_response.content.readexactly = AsyncMock(
            side_effect=asyncio.IncompleteReadError(partial=b'\x01', expected=4)
        )

        stream = AiohttpSyncStream(mock_response, loop, read_timeout=30)
        with pytest.raises(asyncio.IncompleteReadError):
            stream.read(4)
        loop.close()

    def test_read_raises_on_timeout(self):
        """read() should raise TimeoutError when read_timeout is exceeded."""
        from gremlin_python.driver.aiohttp.transport import AiohttpSyncStream

        loop = asyncio.new_event_loop()
        mock_response = MagicMock()
        mock_response.content = MagicMock()
        mock_response.content.readexactly = AsyncMock(side_effect=asyncio.TimeoutError())

        stream = AiohttpSyncStream(mock_response, loop, read_timeout=1)
        with pytest.raises(asyncio.TimeoutError):
            stream.read(4)
        loop.close()


class TestTransportGetStream:
    """
    Tests for AiohttpHTTPTransport.get_stream() which should return
    an AiohttpSyncStream wrapping the current HTTP response.
    """

    def test_get_stream_returns_sync_stream(self):
        """get_stream() should return an AiohttpSyncStream instance."""
        from gremlin_python.driver.aiohttp.transport import AiohttpHTTPTransport, AiohttpSyncStream

        transport = AiohttpHTTPTransport.__new__(AiohttpHTTPTransport)
        transport._loop = asyncio.new_event_loop()
        transport._read_timeout = 30
        transport._http_req_resp = MagicMock()

        stream = transport.get_stream()
        assert isinstance(stream, AiohttpSyncStream)
        transport._loop.close()

    def test_get_stream_uses_current_response(self):
        """get_stream() should wrap the response from the most recent write()."""
        from gremlin_python.driver.aiohttp.transport import AiohttpHTTPTransport, AiohttpSyncStream

        transport = AiohttpHTTPTransport.__new__(AiohttpHTTPTransport)
        transport._loop = asyncio.new_event_loop()
        transport._read_timeout = 30
        mock_resp = MagicMock()
        mock_resp.content = MagicMock()
        mock_resp.content.readexactly = AsyncMock(return_value=b'\x84')
        transport._http_req_resp = mock_resp

        stream = transport.get_stream()
        result = stream.read(1)
        assert result == b'\x84'
        transport._loop.close()


# ===========================================================================
# Step 2: Streaming receive loop in Connection tests
# ===========================================================================

class TestConnectionStreamingReceive:
    """
    Tests for Connection._receive() streaming implementation.

    The streaming _receive() should:
    - Read the GB response header (version + flags)
    - Loop reading objects via GraphBinaryReader.to_object() until end-of-stream
    - Push each individual item into result_set.stream (not as a list)
    - Read the status trailer after end-of-stream
    - Raise GremlinServerError on non-success status codes
    - Handle bulked results by expanding items according to bulk count
    """

    def _make_connection_with_stream(self, response_bytes):
        """Create a Connection wired to a mock transport that streams from response_bytes."""
        from gremlin_python.driver.connection import Connection

        conn = Connection.__new__(Connection)
        conn._pool = queue.Queue()
        conn._result_set = MagicMock()
        conn._result_set.stream = queue.Queue()

        # Mock transport.get_stream() to return a BytesIO over the response bytes
        conn._transport = MagicMock()
        conn._transport.get_stream.return_value = io.BytesIO(response_bytes)
        conn._transport.status_code = 200
        conn._transport.content_type = 'application/vnd.graphbinary-v4.0'

        return conn

    def test_receive_single_int(self):
        """Streaming a single integer result should put one item in the queue."""
        response = build_gb_response([42])
        conn = self._make_connection_with_stream(response)

        conn._receive()

        items = []
        while not conn._result_set.stream.empty():
            items.append(conn._result_set.stream.get_nowait())
        assert items == [42]

    def test_receive_multiple_strings(self):
        """Streaming multiple string results should put each as a separate queue entry."""
        response = build_gb_response(["hello", "world", "foo"])
        conn = self._make_connection_with_stream(response)

        conn._receive()

        items = []
        while not conn._result_set.stream.empty():
            items.append(conn._result_set.stream.get_nowait())
        assert items == ["hello", "world", "foo"]

    def test_receive_mixed_types(self):
        """Streaming mixed types (int, string, float) should preserve types and order."""
        from gremlin_python.statics import long
        response = build_gb_response([long(1), "two", 3.0])
        conn = self._make_connection_with_stream(response)

        conn._receive()

        items = []
        while not conn._result_set.stream.empty():
            items.append(conn._result_set.stream.get_nowait())
        assert len(items) == 3
        assert items[0] == 1
        assert items[1] == "two"
        assert items[2] == 3.0

    def test_receive_empty_response_204(self):
        """A 204 HTTP response should return early with no items."""
        conn = self._make_connection_with_stream(b'')
        conn._transport.status_code = 204

        conn._receive()

        assert conn._result_set.stream.empty()

    def test_receive_server_error_raises(self):
        """A non-success status code should raise GremlinServerError."""
        response = build_gb_response(
            [],
            status_code=500,
            status_message="Internal error",
            status_exception="java.lang.RuntimeException"
        )
        conn = self._make_connection_with_stream(response)

        with pytest.raises(GremlinServerError) as exc_info:
            conn._receive()
        assert exc_info.value.status_code == 500
        assert "Internal error" in str(exc_info.value)

    def test_receive_returns_connection_to_pool(self):
        """After _receive() completes (success or failure), connection must be returned to pool."""
        response = build_gb_response([1])
        conn = self._make_connection_with_stream(response)

        conn._receive()

        assert not conn._pool.empty()
        assert conn._pool.get_nowait() is conn

    def test_receive_returns_connection_to_pool_on_error(self):
        """Connection must be returned to pool even when _receive() raises."""
        response = build_gb_response([], status_code=500, status_message="fail")
        conn = self._make_connection_with_stream(response)

        with pytest.raises(GremlinServerError):
            conn._receive()

        assert not conn._pool.empty()
        assert conn._pool.get_nowait() is conn

    def test_receive_vertex_objects(self):
        """Streaming Vertex objects should deserialize correctly."""
        from gremlin_python.structure.graph import Vertex
        response = build_gb_response([Vertex(1, "person"), Vertex(2, "software")])
        conn = self._make_connection_with_stream(response)

        conn._receive()

        items = []
        while not conn._result_set.stream.empty():
            items.append(conn._result_set.stream.get_nowait())
        assert len(items) == 2
        assert items[0].id == 1
        assert items[0].label == "person"
        assert items[1].id == 2
        assert items[1].label == "software"

    def test_receive_items_available_during_streaming(self):
        """Items should be available in the queue as they are read, not only after completion."""
        # This test verifies the streaming property: items are pushed one at a time.
        # We verify by checking the queue has items before _receive returns.
        # Since _receive is synchronous in tests, we verify the queue is populated incrementally
        # by checking the final state has individual items (not a single list).
        response = build_gb_response([1, 2, 3])
        conn = self._make_connection_with_stream(response)

        conn._receive()

        # Each item should be a separate queue entry
        items = []
        while not conn._result_set.stream.empty():
            item = conn._result_set.stream.get_nowait()
            assert not isinstance(item, list), "Items should be individual objects, not lists"
            items.append(item)
        assert items == [1, 2, 3]


# ===========================================================================
# Bulking tests
# ===========================================================================

class TestBulkedResponse:
    """
    Tests for bulked response handling. When the server sends a bulked response
    (flags byte 0x01), each result object is followed by a fully-qualified long
    bulk count. The client expands each object by its count into the ResultSet.
    """

    def _make_connection(self, response_bytes):
        from gremlin_python.driver.connection import Connection
        conn = Connection.__new__(Connection)
        conn._pool = queue.Queue()
        conn._result_set = MagicMock()
        conn._result_set.stream = queue.Queue()
        conn._transport = MagicMock()
        conn._transport.get_stream.return_value = io.BytesIO(response_bytes)
        conn._transport.status_code = 200
        conn._transport.content_type = 'application/vnd.graphbinary-v4.0'
        return conn

    def _drain(self, conn):
        items = []
        while not conn._result_set.stream.empty():
            items.append(conn._result_set.stream.get_nowait())
        return items

    def test_bulked_results_expanded(self):
        """Each item should appear bulk_count times."""
        conn = self._make_connection(build_gb_bulked_response([("a", 3), ("b", 2)]))
        conn._receive()
        assert self._drain(conn) == ["a", "a", "a", "b", "b"]

    def test_bulked_single_counts(self):
        """Bulk count of 1 behaves like non-bulked."""
        conn = self._make_connection(build_gb_bulked_response([("x", 1), ("y", 1)]))
        conn._receive()
        assert self._drain(conn) == ["x", "y"]

    def test_bulked_large_count(self):
        """Single item with large bulk count expands correctly."""
        conn = self._make_connection(build_gb_bulked_response([("z", 100)]))
        conn._receive()
        assert self._drain(conn) == ["z"] * 100

    def test_bulked_mixed_types(self):
        """Bulking works with different value types."""
        conn = self._make_connection(build_gb_bulked_response([(42, 2), ("hello", 1)]))
        conn._receive()
        assert self._drain(conn) == [42, 42, "hello"]

    def test_bulked_empty_response(self):
        """Bulked response with no items leaves queue empty."""
        conn = self._make_connection(build_gb_bulked_response([]))
        conn._receive()
        assert conn._result_set.stream.empty()

    def test_non_bulked_ignores_flags(self):
        """When flags byte is 0x00, no bulk count is read."""
        conn = self._make_connection(build_gb_response([1, 2, 3]))
        conn._receive()
        assert self._drain(conn) == [1, 2, 3]


# ===========================================================================
# Step 3: ResultSet individual item consumption tests
# ===========================================================================

class TestResultSetStreaming:
    """
    Tests for ResultSet.one() and all() with individual item queue entries.

    After the streaming change:
    - one() should return a single item (not a list)
    - all() should collect individual items into a flat list
    - Iteration via __next__ should yield individual items
    """

    def _make_result_set(self, items):
        """Create a ResultSet with items pre-loaded and a completed done future."""
        from gremlin_python.driver.resultset import ResultSet

        q = queue.Queue()
        for item in items:
            q.put_nowait(item)

        rs = ResultSet(q)
        done = Future()
        done.set_result(None)
        rs.done = done
        return rs

    def test_one_returns_single_item(self):
        """one() should return a single item, not a list."""
        rs = self._make_result_set([42, 43, 44])
        result = rs.one()
        assert result == 42
        assert not isinstance(result, list)

    def test_one_returns_exhausted_when_empty(self):
        """one() should return _EXHAUSTED sentinel when queue is empty and done."""
        from gremlin_python.driver.resultset import _EXHAUSTED
        rs = self._make_result_set([])
        result = rs.one()
        assert result is _EXHAUSTED

    def test_one_sequential_calls(self):
        """Sequential one() calls should return items in order."""
        from gremlin_python.driver.resultset import _EXHAUSTED
        rs = self._make_result_set(["a", "b", "c"])
        assert rs.one() == "a"
        assert rs.one() == "b"
        assert rs.one() == "c"
        assert rs.one() is _EXHAUSTED

    def test_all_returns_flat_list(self):
        """all() should return a flat list of individual items."""
        rs = self._make_result_set([1, 2, 3])
        result = rs.all().result()
        assert result == [1, 2, 3]

    def test_all_returns_empty_list_when_no_items(self):
        """all() should return an empty list when there are no items."""
        rs = self._make_result_set([])
        result = rs.all().result()
        assert result == []

    def test_iteration_yields_individual_items(self):
        """Iterating over ResultSet should yield individual items."""
        rs = self._make_result_set([10, 20, 30])
        items = list(rs)
        assert items == [10, 20, 30]

    def test_iteration_does_not_stop_on_none(self):
        """None is a valid Gremlin result and should not stop iteration."""
        rs = self._make_result_set([1, None, 3])
        items = list(rs)
        assert items == [1, None, 3]

    def test_all_with_mixed_types(self):
        """all() should preserve types in the collected list."""
        from gremlin_python.structure.graph import Vertex
        v = Vertex(1, "person")
        rs = self._make_result_set([v, "hello", 42])
        result = rs.all().result()
        assert len(result) == 3
        assert isinstance(result[0], Vertex)
        assert result[1] == "hello"
        assert result[2] == 42

    def test_all_propagates_error(self):
        """all() should propagate exceptions from the done future."""
        from gremlin_python.driver.resultset import ResultSet

        q = queue.Queue()
        rs = ResultSet(q)
        done = Future()
        done.set_exception(GremlinServerError({'code': 500, 'message': 'fail', 'exception': ''}))
        rs.done = done

        with pytest.raises(GremlinServerError):
            rs.all().result()

    def test_one_propagates_error_when_empty(self):
        """one() should raise the error from done future when queue is empty and done has error."""
        from gremlin_python.driver.resultset import ResultSet

        q = queue.Queue()
        rs = ResultSet(q)
        done = Future()
        done.set_exception(GremlinServerError({'code': 500, 'message': 'fail', 'exception': ''}))
        rs.done = done

        with pytest.raises(GremlinServerError):
            rs.one()


# ===========================================================================
# Edge case tests
# ===========================================================================

class TestStreamingEdgeCases:
    """
    Tests for edge cases in the streaming implementation:
    - Mid-stream disconnect (IncompleteReadError)
    - Non-GraphBinary error responses (JSON/text from server on 4xx/5xx)
    - Large number of results
    - Null values in the stream
    """

    def _make_connection_with_stream(self, response_bytes):
        from gremlin_python.driver.connection import Connection

        conn = Connection.__new__(Connection)
        conn._pool = queue.Queue()
        conn._result_set = MagicMock()
        conn._result_set.stream = queue.Queue()
        conn._transport = MagicMock()
        conn._transport.get_stream.return_value = io.BytesIO(response_bytes)
        conn._transport.status_code = 200
        conn._transport.content_type = 'application/vnd.graphbinary-v4.0'
        return conn

    def test_mid_stream_disconnect(self):
        """If the stream is truncated mid-object, an error should be raised."""
        # Build a valid response but truncate it in the middle of the second item
        writer = GraphBinaryWriter()
        ba = bytearray()
        ba.append(0x84)  # version
        ba.append(0x00)  # flags (not bulked)
        ba.extend(_gb_typed_value(writer, 42))  # first item (complete)
        # Truncate: write only the type code of a string but not the rest
        ba.append(DataType.string.value)

        conn = self._make_connection_with_stream(bytes(ba))

        with pytest.raises(Exception):
            conn._receive()

        # Connection should still be returned to pool
        assert not conn._pool.empty()

    def test_null_values_in_stream(self):
        """Null values in the result stream should be preserved."""
        response = build_gb_response([None, "hello", None])
        conn = self._make_connection_with_stream(response)

        conn._receive()

        items = []
        while not conn._result_set.stream.empty():
            items.append(conn._result_set.stream.get_nowait())
        assert items == [None, "hello", None]

    def test_large_result_set(self):
        """Streaming should handle a large number of results."""
        count = 10000
        response = build_gb_response(list(range(count)))
        conn = self._make_connection_with_stream(response)

        conn._receive()

        items = []
        while not conn._result_set.stream.empty():
            items.append(conn._result_set.stream.get_nowait())
        assert items == list(range(count))

    def test_status_message_and_exception_are_none(self):
        """Status with null message and exception should not raise for success codes."""
        response = build_gb_response([1], status_code=200)
        conn = self._make_connection_with_stream(response)

        conn._receive()  # should not raise

        items = []
        while not conn._result_set.stream.empty():
            items.append(conn._result_set.stream.get_nowait())
        assert items == [1]

    def test_non_graphbinary_error_response(self):
        """
        When the server returns a non-GraphBinary response (e.g. JSON error on 4xx/5xx),
        the content-type check should raise a meaningful GremlinServerError
        rather than a cryptic deserialization failure.
        """
        from gremlin_python.driver.connection import Connection

        conn = Connection.__new__(Connection)
        conn._pool = queue.Queue()
        conn._result_set = MagicMock()
        conn._result_set.stream = queue.Queue()

        # Mock transport with JSON error body and non-GB content type
        json_error = b'{"message": "Unauthorized"}'
        mock_transport = MagicMock()
        mock_transport.status_code = 401
        mock_transport.content_type = 'application/json'
        mock_transport.read_body.return_value = json_error
        conn._transport = mock_transport

        with pytest.raises(GremlinServerError) as exc_info:
            conn._receive()

        assert exc_info.value.status_code == 401
        assert "Unauthorized" in str(exc_info.value)
        # Connection should still be returned to pool
        assert not conn._pool.empty()

    def _make_error_connection(self, status_code, content_type, body):
        """Create a Connection with a mock transport returning an error response."""
        from gremlin_python.driver.connection import Connection

        conn = Connection.__new__(Connection)
        conn._pool = queue.Queue()
        conn._result_set = MagicMock()
        conn._result_set.stream = queue.Queue()
        mock_transport = MagicMock()
        mock_transport.status_code = status_code
        mock_transport.content_type = content_type
        mock_transport.read_body.return_value = body
        conn._transport = mock_transport
        return conn

    def test_plain_text_error_response(self):
        """Plain text 500 error should be returned as the error message."""
        conn = self._make_error_connection(500, 'text/plain', b'Internal Server Error')

        with pytest.raises(GremlinServerError) as exc_info:
            conn._receive()

        assert exc_info.value.status_code == 500
        assert "Internal Server Error" in str(exc_info.value)

    def test_html_error_response(self):
        """HTML error from a proxy/load balancer should be returned as the error message."""
        conn = self._make_error_connection(502, 'text/html', b'<html>Bad Gateway</html>')

        with pytest.raises(GremlinServerError) as exc_info:
            conn._receive()

        assert exc_info.value.status_code == 502
        assert "<html>Bad Gateway</html>" in str(exc_info.value)

    def test_graphbinary_error_falls_through_to_deserialization(self):
        """When content-type IS graphbinary on a 4xx/5xx, should attempt GB deserialization, not text path."""
        # Build a valid GB error response with status 500
        response = build_gb_response([], status_code=500, status_message="Server error")
        conn = self._make_connection_with_stream(response)
        # Override status_code to 500 to simulate server error with GB content-type
        conn._transport.status_code = 500

        with pytest.raises(GremlinServerError) as exc_info:
            conn._receive()

        # Should get the GB-deserialized error, not a raw body text error
        assert exc_info.value.status_code == 500
        assert "Server error" in str(exc_info.value)


class TestStreamingDelivery:
    """
    Tests that verify results are available incrementally during streaming,
    not only after the full response is received.
    """

    def test_first_result_available_before_stream_complete(self):
        """First result should be in the queue while _receive() is still running."""
        import threading

        response = build_gb_response(list(range(100)))

        from gremlin_python.driver.connection import Connection
        conn = Connection.__new__(Connection)
        conn._pool = queue.Queue()
        conn._result_set = MagicMock()
        conn._result_set.stream = queue.Queue()
        conn._transport = MagicMock()
        conn._transport.status_code = 200
        conn._transport.content_type = 'application/vnd.graphbinary-v4.0'
        conn._transport.get_stream.return_value = io.BytesIO(response)

        first_item = []

        def consume():
            item = conn._result_set.stream.get(timeout=5)
            first_item.append(item)

        consumer = threading.Thread(target=consume)
        consumer.start()

        conn._receive()
        consumer.join(timeout=5)

        assert first_item[0] == 0, "First item should be 0"

    def test_results_arrive_incrementally(self):
        """Items should be pushed to the queue one at a time, not all at once."""
        import threading
        import time

        class SlowByteStream:
            """Simulates a network stream that delivers bytes with delays."""
            def __init__(self, data, delay=0.01):
                self._buf = io.BytesIO(data)
                self._delay = delay
                self._reads = 0

            def read(self, n):
                self._reads += 1
                if self._reads > 5:  # delay after header reads
                    time.sleep(self._delay)
                return self._buf.read(n)

        response = build_gb_response(["a", "b", "c", "d", "e"])

        from gremlin_python.driver.connection import Connection
        conn = Connection.__new__(Connection)
        conn._pool = queue.Queue()
        conn._result_set = MagicMock()
        conn._result_set.stream = queue.Queue()
        conn._transport = MagicMock()
        conn._transport.status_code = 200
        conn._transport.content_type = 'application/vnd.graphbinary-v4.0'
        conn._transport.get_stream.return_value = SlowByteStream(response)

        timestamps = []

        def consume():
            while True:
                try:
                    conn._result_set.stream.get(timeout=2)
                    timestamps.append(time.time())
                except queue.Empty:
                    break

        consumer = threading.Thread(target=consume)
        consumer.start()

        conn._receive()
        consumer.join(timeout=5)

        assert len(timestamps) == 5, f"Expected 5 items, got {len(timestamps)}"
        # Verify items didn't all arrive at the same instant
        time_span = timestamps[-1] - timestamps[0]
        assert time_span > 0, "Results should arrive over time, not all at once"


class TestGraphBinaryReaderStreamingCompatibility:
    """
    Verify that GraphBinaryReader.to_object() works correctly when reading
    from a file-like stream (the key insight of the plan: the reader is
    already streaming-capable).
    """

    def test_reader_reads_int_from_stream(self):
        """GraphBinaryReader should read an int from a file-like stream."""
        writer = GraphBinaryWriter()
        reader = GraphBinaryReader()

        ba = bytearray()
        writer.to_dict(42, ba)
        stream = io.BytesIO(bytes(ba))

        result = reader.to_object(stream)
        assert result == 42

    def test_reader_reads_multiple_objects_sequentially(self):
        """GraphBinaryReader should read multiple objects from the same stream."""
        writer = GraphBinaryWriter()
        reader = GraphBinaryReader()

        ba = bytearray()
        writer.to_dict("hello", ba)
        writer.to_dict(99, ba)
        writer.to_dict(Marker.end_of_stream(), ba)
        stream = io.BytesIO(bytes(ba))

        assert reader.to_object(stream) == "hello"
        assert reader.to_object(stream) == 99
        assert reader.to_object(stream) == Marker.end_of_stream()

    def test_reader_reads_end_of_stream_marker(self):
        """GraphBinaryReader should correctly identify the end-of-stream marker."""
        writer = GraphBinaryWriter()
        reader = GraphBinaryReader()

        ba = bytearray()
        writer.to_dict(Marker.end_of_stream(), ba)
        stream = io.BytesIO(bytes(ba))

        result = reader.to_object(stream)
        assert result == Marker.end_of_stream()

    def test_reader_reads_vertex_from_stream(self):
        """GraphBinaryReader should deserialize a Vertex from a stream."""
        from gremlin_python.structure.graph import Vertex

        writer = GraphBinaryWriter()
        reader = GraphBinaryReader()

        ba = bytearray()
        writer.to_dict(Vertex(1, "person"), ba)
        stream = io.BytesIO(bytes(ba))

        result = reader.to_object(stream)
        assert isinstance(result, Vertex)
        assert result.id == 1
        assert result.label == "person"

    def test_reader_reads_null_from_stream(self):
        """GraphBinaryReader should handle null values in the stream."""
        writer = GraphBinaryWriter()
        reader = GraphBinaryReader()

        ba = bytearray()
        writer.to_dict(None, ba)
        stream = io.BytesIO(bytes(ba))

        result = reader.to_object(stream)
        assert result is None


# ===========================================================================
# Early consumption tests
# ===========================================================================

class TestEarlyConsumption:
    """
    Tests that a consumer can call ResultSet.one() and iterate results
    while _receive() is still actively streaming from a slow source.

    This validates the core streaming benefit: the caller does not have to
    wait for the entire response before processing the first result.
    """

    def _make_slow_streaming_connection(self, items, delay_per_read=0.05):
        """
        Wire up a Connection + real ResultSet backed by a SlowByteStream.
        Returns (connection, result_set).
        """
        import time
        from gremlin_python.driver.connection import Connection
        from gremlin_python.driver.resultset import ResultSet

        class SlowByteStream:
            """BytesIO wrapper that injects a delay on every read after the header."""
            def __init__(self, data, delay):
                self._buf = io.BytesIO(data)
                self._delay = delay
                self._reads = 0

            def read(self, n):
                self._reads += 1
                # Let the 2-byte header through fast; slow down payload reads
                if self._reads > 2:
                    time.sleep(self._delay)
                return self._buf.read(n)

        response = build_gb_response(items)

        conn = Connection.__new__(Connection)
        conn._pool = queue.Queue()
        rs = ResultSet(queue.Queue())
        conn._result_set = rs
        conn._transport = MagicMock()
        conn._transport.status_code = 200
        conn._transport.content_type = 'application/vnd.graphbinary-v4.0'
        conn._transport.get_stream.return_value = SlowByteStream(response, delay_per_read)

        return conn, rs

    def test_one_returns_first_item_while_receive_still_running(self):
        """ResultSet.one() should return the first item before _receive() finishes."""
        import threading

        items = list(range(20))
        conn, rs = self._make_slow_streaming_connection(items, delay_per_read=0.02)

        done = Future()
        rs.done = done

        def background_receive():
            try:
                conn._receive()
                done.set_result(None)
            except Exception as e:
                done.set_exception(e)

        t = threading.Thread(target=background_receive)
        t.start()

        # one() should return the first item while _receive is still going
        first = rs.one()
        receive_still_running = t.is_alive()

        t.join(timeout=10)

        assert first == 0
        assert receive_still_running, "one() should have returned before _receive() finished"

    def test_iterate_partial_results_while_streaming(self):
        """Consumer can iterate several results via one() while _receive() is still active."""
        import threading

        items = list(range(50))
        conn, rs = self._make_slow_streaming_connection(items, delay_per_read=0.02)

        done = Future()
        rs.done = done

        def background_receive():
            try:
                conn._receive()
                done.set_result(None)
            except Exception as e:
                done.set_exception(e)

        t = threading.Thread(target=background_receive)
        t.start()

        # Consume a few items early
        early = [rs.one() for _ in range(5)]

        assert t.is_alive(), "Should still be streaming when we consumed only 5 of 50 items"
        assert early == [0, 1, 2, 3, 4]

        # Now drain the rest via iteration
        rest = list(rs)
        t.join(timeout=10)

        assert early + rest == items

    def test_early_consumption_with_bulked_response(self):
        """Early consumption works correctly with bulked results."""
        import threading

        bulked_items = [("x", 10), ("y", 10), ("z", 10)]
        response = build_gb_bulked_response(bulked_items)

        from gremlin_python.driver.connection import Connection
        from gremlin_python.driver.resultset import ResultSet
        import time

        class SlowByteStream:
            def __init__(self, data):
                self._buf = io.BytesIO(data)
                self._reads = 0

            def read(self, n):
                self._reads += 1
                if self._reads > 2:
                    time.sleep(0.02)
                return self._buf.read(n)

        conn = Connection.__new__(Connection)
        conn._pool = queue.Queue()
        rs = ResultSet(queue.Queue())
        conn._result_set = rs
        conn._transport = MagicMock()
        conn._transport.status_code = 200
        conn._transport.content_type = 'application/vnd.graphbinary-v4.0'
        conn._transport.get_stream.return_value = SlowByteStream(response)

        done = Future()
        rs.done = done

        def background_receive():
            try:
                conn._receive()
                done.set_result(None)
            except Exception as e:
                done.set_exception(e)

        t = threading.Thread(target=background_receive)
        t.start()

        first = rs.one()
        assert first == "x"

        rest = list(rs)
        t.join(timeout=10)

        assert [first] + rest == ["x"] * 10 + ["y"] * 10 + ["z"] * 10


# ===========================================================================
# Connection write-path tests (formerly in test_protocol.py)
# ===========================================================================

class TestConnectionWriteRequest:
    """
    Tests for Connection._write_request() which handles serialization,
    header construction, auth, and interceptors before calling transport.write().
    """

    def _make_connection(self, request_serializer=None, response_serializer=None,
                         auth=None, interceptors=None):
        from gremlin_python.driver.connection import Connection
        from gremlin_python.driver.serializer import GraphBinarySerializersV4

        if response_serializer is None:
            response_serializer = GraphBinarySerializersV4()

        conn = Connection.__new__(Connection)
        conn._request_serializer = request_serializer
        conn._response_serializer = response_serializer
        conn._auth = auth
        conn._interceptors = interceptors
        conn._transport = MagicMock()
        return conn

    def test_none_request_serializer_passes_raw_message(self):
        conn = self._make_connection(request_serializer=None)
        msg = RequestMessage(fields={}, gremlin="g.V()")
        conn._write_request(msg)
        written = conn._transport.write.call_args[0][0]
        assert written['payload'] == msg
        assert 'content-type' not in written['headers']

    def test_graphbinary_serializer_serializes_payload(self):
        from gremlin_python.driver.serializer import GraphBinarySerializersV4
        gb = GraphBinarySerializersV4()
        conn = self._make_connection(request_serializer=gb, response_serializer=gb)
        msg = RequestMessage(fields={}, gremlin="g.V()")
        conn._write_request(msg)
        written = conn._transport.write.call_args[0][0]
        assert written['payload'] == gb.serialize_message(msg)
        assert written['headers']['content-type'] == str(gb.version, encoding='utf-8')

    def test_accept_header_set_from_response_serializer(self):
        from gremlin_python.driver.serializer import GraphBinarySerializersV4
        gb = GraphBinarySerializersV4()
        conn = self._make_connection(response_serializer=gb)
        conn._write_request(RequestMessage(fields={}, gremlin="g.V()"))
        written = conn._transport.write.call_args[0][0]
        assert written['headers']['accept'] == str(gb.version, encoding='utf-8')

    def test_auth_passed_in_message(self):
        auth_fn = lambda req: req
        conn = self._make_connection(auth=auth_fn)
        conn._write_request(RequestMessage(fields={}, gremlin="g.V()"))
        written = conn._transport.write.call_args[0][0]
        assert written['auth'] is auth_fn

    def test_single_interceptor_runs(self):
        changed = RequestMessage(fields={}, gremlin="changed")
        def interceptor(request):
            request['payload'] = changed
            return request
        conn = self._make_connection(interceptors=[interceptor])
        conn._write_request(RequestMessage(fields={}, gremlin="g.V()"))
        written = conn._transport.write.call_args[0][0]
        assert written['payload'] == changed

    def test_interceptors_run_sequentially(self):
        def one(req): req['payload'].gremlin.append(1); return req
        def two(req): req['payload'].gremlin.append(2); return req
        def three(req): req['payload'].gremlin.append(3); return req
        conn = self._make_connection(interceptors=[one, two, three])
        conn._write_request(RequestMessage(fields={}, gremlin=[]))
        written = conn._transport.write.call_args[0][0]
        assert written['payload'].gremlin == [1, 2, 3]

    def test_interceptor_works_with_serializer(self):
        from gremlin_python.driver.serializer import GraphBinarySerializersV4
        gb = GraphBinarySerializersV4()
        msg = RequestMessage(fields={}, gremlin="g.E()")
        def assert_interceptor(request):
            assert request['payload'] == gb.serialize_message(msg)
            request['payload'] = "changed"
            return request
        conn = self._make_connection(request_serializer=gb, response_serializer=gb,
                                     interceptors=[assert_interceptor])
        conn._write_request(msg)
        written = conn._transport.write.call_args[0][0]
        assert written['payload'] == "changed"


class TestConnectionInterceptorValidation:
    """Tests for interceptor validation in Connection.__init__."""

    def test_callable_interceptor_wrapped_in_list(self):
        from gremlin_python.driver.connection import Connection
        conn = Connection('url', 'g', None, queue.Queue(),
                          response_serializer=MagicMock(version=b'test'),
                          interceptors=lambda req: req)
        assert isinstance(conn._interceptors, list)
        assert len(conn._interceptors) == 1

    def test_tuple_interceptor_accepted(self):
        from gremlin_python.driver.connection import Connection
        Connection('url', 'g', None, queue.Queue(),
                   response_serializer=MagicMock(version=b'test'),
                   interceptors=(lambda req: req,))

    def test_list_interceptor_accepted(self):
        from gremlin_python.driver.connection import Connection
        Connection('url', 'g', None, queue.Queue(),
                   response_serializer=MagicMock(version=b'test'),
                   interceptors=[lambda req: req])

    def test_invalid_interceptor_raises_type_error(self):
        from gremlin_python.driver.connection import Connection
        with pytest.raises(TypeError):
            Connection('url', 'g', None, queue.Queue(),
                       response_serializer=MagicMock(version=b'test'),
                       interceptors=1)
