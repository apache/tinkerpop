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
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

from gremlin_python.driver.client import Client
from gremlin_python.driver.connection import GremlinConnectionError, GremlinServerError
from gremlin_python.driver.serializer import GraphBinarySerializersV4

from .socket_server_constants import (
    PORT,
    GREMLIN_SINGLE_VERTEX,
    GREMLIN_CLOSE_CONNECTION,
    GREMLIN_VERTEX_THEN_CLOSE,
    GREMLIN_FAIL_AFTER_DELAY,
    GREMLIN_PARTIAL_CONTENT_CLOSE,
    GREMLIN_SLOW_RESPONSE,
    GREMLIN_MALFORMED_RESPONSE,
    GREMLIN_NO_RESPONSE,
    GREMLIN_EMPTY_BODY,
)

url = os.environ.get('GREMLIN_SOCKET_SERVER_URL', 'http://localhost:{}/gremlin'.format(PORT))


@pytest.fixture(scope="module")
def socket_server_client():
    try:
        client = Client(url, 'g')
        # Verify connectivity
        client.submit(GREMLIN_SINGLE_VERTEX).all().result()
    except Exception:
        pytest.skip("Socket server is not available at {}".format(url))
    yield client
    client.close()


@pytest.fixture
def fresh_client():
    client = Client(url, 'g')
    yield client
    client.close()


def test_should_receive_single_vertex(socket_server_client):
    result = socket_server_client.submit(GREMLIN_SINGLE_VERTEX).all().result()
    assert len(result) == 1


def test_should_handle_server_closing_connection_before_response(socket_server_client):
    with pytest.raises(GremlinConnectionError, match="Connection to server closed unexpectedly"):
        socket_server_client.submit(GREMLIN_CLOSE_CONNECTION).all().result()

    # Recovery
    result = socket_server_client.submit(GREMLIN_SINGLE_VERTEX).all().result()
    assert len(result) == 1


def test_should_handle_server_closing_connection_after_response(socket_server_client):
    result = socket_server_client.submit(GREMLIN_VERTEX_THEN_CLOSE).all().result()
    assert len(result) >= 1

    time.sleep(3)

    # Recovery
    result = socket_server_client.submit(GREMLIN_SINGLE_VERTEX).all().result()
    assert len(result) == 1


def test_should_handle_server_error_after_delay(socket_server_client):
    with pytest.raises(GremlinServerError) as exc_info:
        socket_server_client.submit(GREMLIN_FAIL_AFTER_DELAY).all().result()
    assert exc_info.value.status_code == 500
    assert "Server error" in str(exc_info.value)


def test_should_handle_partial_content_close(socket_server_client):
    with pytest.raises(GremlinConnectionError, match="Connection to server closed unexpectedly"):
        socket_server_client.submit(GREMLIN_PARTIAL_CONTENT_CLOSE).all().result()

    # Recovery
    result = socket_server_client.submit(GREMLIN_SINGLE_VERTEX).all().result()
    assert len(result) == 1


def test_should_handle_malformed_response(socket_server_client):
    # The driver surfaces a low-level ValueError from the GraphBinary deserializer
    # rather than a Gremlin-aware exception (flagged in tinkerpop-1fn).
    with pytest.raises(ValueError, match="not a valid DataType"):
        socket_server_client.submit(GREMLIN_MALFORMED_RESPONSE).all().result()

    # Recovery
    result = socket_server_client.submit(GREMLIN_SINGLE_VERTEX).all().result()
    assert len(result) == 1


def test_should_handle_empty_response_body(fresh_client):
    # An empty HTTP response body should surface as a GremlinConnectionError
    # wrapping the underlying IncompleteReadError.
    with pytest.raises(GremlinConnectionError, match="Server returned an empty response body"):
        fresh_client.submit(GREMLIN_EMPTY_BODY).all().result()

    # Recovery on the same client - the dead connection should be evicted from
    # aiohttp's internal pool so subsequent requests get a fresh connection.
    result = fresh_client.submit(GREMLIN_SINGLE_VERTEX).all().result()
    assert len(result) == 1


def test_should_handle_slow_response(socket_server_client):
    result = socket_server_client.submit(GREMLIN_SLOW_RESPONSE).all().result()
    assert len(result) >= 1


def test_should_timeout_when_server_never_responds():
    # Use a short read_timeout so the test fails fast instead of waiting for
    # aiohttp's 5-minute default. aiohttp surfaces this as asyncio.TimeoutError.
    client = Client(url, 'g', read_timeout=2)
    try:
        with pytest.raises(asyncio.TimeoutError):
            client.submit(GREMLIN_NO_RESPONSE).all().result()
    finally:
        client.close()

    # Recovery with a new client
    recovery_client = Client(url, 'g')
    try:
        result = recovery_client.submit(GREMLIN_SINGLE_VERTEX).all().result()
        assert len(result) == 1
    finally:
        recovery_client.close()


def test_should_handle_async_requests_during_connection_close(socket_server_client):
    future1 = socket_server_client.submit_async(GREMLIN_CLOSE_CONNECTION)
    future2 = socket_server_client.submit_async(GREMLIN_CLOSE_CONNECTION)

    for future in [future1, future2]:
        try:
            future.result().all().result()
        except Exception:
            pass

    # Recovery
    result = socket_server_client.submit(GREMLIN_SINGLE_VERTEX).all().result()
    assert len(result) == 1


def test_should_handle_concurrent_mixed_requests(socket_server_client):
    vertex_results = []
    close_errors = []

    def submit_vertex():
        return socket_server_client.submit(GREMLIN_SINGLE_VERTEX).all().result()

    def submit_close():
        return socket_server_client.submit(GREMLIN_CLOSE_CONNECTION).all().result()

    with ThreadPoolExecutor(max_workers=10) as executor:
        vertex_futures = [executor.submit(submit_vertex) for _ in range(5)]
        close_futures = [executor.submit(submit_close) for _ in range(5)]

        for f in as_completed(vertex_futures):
            try:
                vertex_results.append(f.result())
            except Exception:
                pass

        for f in as_completed(close_futures):
            try:
                f.result()
            except Exception:
                close_errors.append(True)

    assert len(vertex_results) == 5
    assert len(close_errors) == 5
