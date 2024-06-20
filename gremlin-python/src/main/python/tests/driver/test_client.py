#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
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
import threading
import uuid

import pytest
from gremlin_python.driver import serializer
from gremlin_python.driver.client import Client
from gremlin_python.driver.protocol import GremlinServerError
from gremlin_python.driver.request import RequestMessageV4
from gremlin_python.process.graph_traversal import __, GraphTraversalSource
from gremlin_python.process.traversal import TraversalStrategies
from gremlin_python.process.strategies import OptionsStrategy
from gremlin_python.structure.graph import Graph, Vertex
from gremlin_python.driver.aiohttp.transport import AiohttpTransport, AiohttpHTTPTransport
from gremlin_python.statics import *
from asyncio import TimeoutError

__author__ = 'David M. Brown (davebshow@gmail.com)'

gremlin_server_url_http = os.environ.get('GREMLIN_SERVER_URL_HTTP', 'http://localhost:{}/')
test_no_auth_http_url = gremlin_server_url_http.format(45940)


def create_basic_request_message(traversal, source='gmodern', type='bytecode'):
    return RequestMessageV4(fields={'g': source, 'gremlinType': type}, gremlin=traversal.bytecode)


@pytest.mark.skip(reason="needs additional updates")
def test_connection(connection_http):
    g = GraphTraversalSource(Graph(), TraversalStrategies())
    t = g.V()
    message = create_basic_request_message(t)
    results_set = connection_http.write(message).result()
    future = results_set.all()
    results = future.result()
    assert len(results) == 6
    assert isinstance(results, list)
    assert results_set.done.done()
    assert 'host' in results_set.status_attributes


@pytest.mark.skip(reason="needs additional updates")
def test_client_message_too_big(client_http):
    try:
        client_http = Client(test_no_auth_http_url, 'g', max_content_length=4096)
        client_http.submit("\" \"*8000").all().result()
        assert False
    except Exception as ex:
        assert ex.args[0].startswith("Received error on read: 'Message size") \
               and ex.args[0].endswith("exceeds limit 4096'")

        # confirm the client instance is still usable and not closed
        assert ["test"] == client_http.submit("'test'").all().result()
    finally:
        client_http.close()


def test_client_script_submission(client_http):
    assert len(client_http.submit("new int[100]").all().result()) == 100


def test_client_script_submission_inject(client_http):
    assert len(client_http.submit("g.inject(new int[100])").all().result()) == 100


def test_client_simple_eval(client_http):
    assert client_http.submit('1 + 1').all().result()[0] == 2


def test_client_simple_eval_bindings(client_http):
    assert client_http.submit('x + x', {'x': 2}).all().result()[0] == 4


def test_client_eval_traversal(client_http):
    assert len(client_http.submit('g.V()').all().result()) == 6


@pytest.mark.skip(reason="needs additional updates")
def test_client_error(client_http):
    try:
        # should fire an exception
        client_http.submit('1/0').all().result()
        assert False
    except GremlinServerError as ex:
        assert 'exceptions' in ex.status_attributes
        assert 'stackTrace' in ex.status_attributes
        assert str(ex) == f"{ex.status_code}: {ex.status_message}"

    # still can submit after failure
    assert client_http.submit('x + x', {'x': 2}).all().result()[0] == 4


@pytest.mark.skip(reason="needs additional updates")
def test_client_connection_pool_after_error(client_http):
    # Overwrite fixture with pool_size=1 client
    client_http = Client(test_no_auth_http_url, 'gmodern', pool_size=1)

    try:
        # should fire an exception
        client_http.submit('1/0').all().result()
        assert False
    except GremlinServerError as gse:
        # expecting the pool size to be 1 again after query returned
        assert gse.status_code == 597
        assert client_http.available_pool_size == 1

    # still can submit after failure
    assert client_http.submit('x + x', {'x': 2}).all().result()[0] == 4


def test_client_no_hang_if_submit_on_closed(client_http):
    assert client_http.submit('1 + 1').all().result()[0] == 2
    client_http.close()
    try:
        # should fail since not hang if closed
        client_http.submit('1 + 1').all().result()
        assert False
    except Exception as ex:
        assert True


def test_client_close_all_connection_in_pool(client_http):
    client_http = Client(test_no_auth_http_url, 'g', pool_size=1)
    assert client_http.available_pool_size == 1
    client_http.submit('2+2').all().result()
    client_http.close()
    assert client_http.available_pool_size == 0


def test_client_side_timeout_set_for_aiohttp(client_http):
    client_http = Client(test_no_auth_http_url, 'gmodern',
                         transport_factory=lambda: AiohttpHTTPTransport(read_timeout=1, write_timeout=1))

    try:
        # should fire an exception
        client_http.submit('Thread.sleep(2000);1').all().result()
        assert False
    except TimeoutError as err:
        # asyncio TimeoutError has no message.
        assert str(err) == ""

    # still can submit after failure
    assert client_http.submit('x + x', {'x': 2}).all().result()[0] == 4


async def async_connect(enable):
    try:
        transport = AiohttpHTTPTransport(call_from_event_loop=enable)
        transport.connect(test_no_auth_http_url)
        transport.close()
        return True
    except RuntimeError:
        return False


def test_from_event_loop():
    assert not asyncio.get_event_loop().run_until_complete(async_connect(False))
    assert asyncio.get_event_loop().run_until_complete(async_connect(True))


def test_client_gremlin(client_http):
    result_set = client_http.submit('g.V(1)')
    result = result_set.all().result()
    assert 1 == len(result)
    vertex = result[0]
    assert type(vertex) is Vertex
    assert 1 == vertex.id
    assert 2 == len(vertex.properties)
    assert 'name' == vertex.properties[0].key
    assert 'marko' == vertex.properties[0].value
    ##
    result_set = client_http.submit('g.with("materializeProperties", "tokens").V(1)')
    result = result_set.all().result()
    assert 1 == len(result)
    vertex = result[0]
    assert 1 == vertex.id
    assert 0 == len(vertex.properties)


def test_client_bytecode(client_http):
    g = GraphTraversalSource(Graph(), TraversalStrategies())
    t = g.V()
    message = create_basic_request_message(t)
    result_set = client_http.submit(message)
    assert len(result_set.all().result()) == 6


def test_client_bytecode_options(client_http):
    # smoke test to validate serialization of OptionsStrategy. no way to really validate this from an integration
    # test perspective because there's no way to access the internals of the strategy via bytecode
    g = GraphTraversalSource(Graph(), TraversalStrategies())
    t = g.with_strategies(OptionsStrategy(options={"x": "test", "y": True})).V()
    message = create_basic_request_message(t)
    result_set = client_http.submit(message)
    assert len(result_set.all().result()) == 6
    ##
    t = g.with_("x", "test").with_("y", True).V()
    message = create_basic_request_message(t)
    result_set = client_http.submit(message)
    assert len(result_set.all().result()) == 6


def test_iterate_result_set(client_http):
    g = GraphTraversalSource(Graph(), TraversalStrategies())
    t = g.V()
    message = create_basic_request_message(t)
    result_set = client_http.submit(message)
    results = []
    for result in result_set:
        results += result
    assert len(results) == 6


def test_client_async(client_http):
    g = GraphTraversalSource(Graph(), TraversalStrategies())
    t = g.V()
    message = create_basic_request_message(t)
    future = client_http.submit_async(message)
    result_set = future.result()
    assert len(result_set.all().result()) == 6


def test_connection_share(client_http):
    # Overwrite fixture with pool_size=1 client
    client_http = Client(test_no_auth_http_url, 'gmodern', pool_size=1)
    g = GraphTraversalSource(Graph(), TraversalStrategies())
    t = g.V()
    message = create_basic_request_message(t)
    message2 = create_basic_request_message(t)
    future = client_http.submit_async(message)
    future2 = client_http.submit_async(message2)

    result_set2 = future2.result()
    assert len(result_set2.all().result()) == 6

    # This future has to finish for the second to yield result - pool_size=1
    assert future.done()
    result_set = future.result()
    assert len(result_set.all().result()) == 6


def test_multi_conn_pool(client_http):
    g = GraphTraversalSource(Graph(), TraversalStrategies())
    t = g.V()
    message = create_basic_request_message(t)
    message2 = create_basic_request_message(t)
    client_http = Client(test_no_auth_http_url, 'g', pool_size=1)
    future = client_http.submit_async(message)
    future2 = client_http.submit_async(message2)

    result_set2 = future2.result()
    assert len(result_set2.all().result()) == 6

    # with connection pool `future` may or may not be done here
    result_set = future.result()
    assert len(result_set.all().result()) == 6


def test_multi_thread_pool(client_http):
    g = GraphTraversalSource(Graph(), TraversalStrategies())
    traversals = [g.V(),
                  g.V().count(),
                  g.E(),
                  g.E().count()
                  ]
    results = [[] for _ in traversals]

    # Use a condition variable to synchronise a group of threads, which should also inject some
    # non-determinism into the run-time execution order
    condition = threading.Condition()

    def thread_run(tr, result_list):
        message = create_basic_request_message(tr)
        with condition:
            condition.wait(5)
        result_set = client_http.submit(message)
        for result in result_set:
            result_list.append(result)

    threads = []
    for i in range(len(results)):
        thread = threading.Thread(target=thread_run,
                                  args=(traversals[i], results[i]),
                                  name="test_multi_thread_pool_%d" % i)
        thread.daemon = True
        threads.append(thread)
        thread.start()
    with condition:
        condition.notify_all()

    for t in threads:
        t.join(5)

    assert len(results[0][0]) == 6
    assert results[1][0][0].object == 6
    assert len(results[2][0]) == 6
    assert results[3][0][0].object == 6


def test_client_bytecode_with_short(client_http):
    g = GraphTraversalSource(Graph(), TraversalStrategies())
    t = g.V().has('age', short(16)).count()
    message = create_basic_request_message(t)
    result_set = client_http.submit(message)
    results = []
    for result in result_set:
        results += result
    assert len(results) == 1


def test_client_bytecode_with_long(client_http):
    g = GraphTraversalSource(Graph(), TraversalStrategies())
    t = g.V().has('age', long(851401972585122)).count()
    message = create_basic_request_message(t)
    result_set = client_http.submit(message)
    results = []
    for result in result_set:
        results += result
    assert len(results) == 1


def test_client_bytecode_with_bigint(client_http):
    g = GraphTraversalSource(Graph(), TraversalStrategies())
    t = g.V().has('age', bigint(0x1000_0000_0000_0000_0000)).count()
    message = create_basic_request_message(t)
    result_set = client_http.submit(message)
    results = []
    for result in result_set:
        results += result
    assert len(results) == 1


def test_big_result_set(client_http):
    g = GraphTraversalSource(Graph(), TraversalStrategies())
    t = g.inject(1).repeat(__.add_V('person').property('name', __.loops())).times(20000).count()
    message = create_basic_request_message(t, source='g')
    result_set = client_http.submit(message)
    results = []
    for result in result_set:
        results += result
    assert len(results) == 1

    t = g.V().limit(10)
    message = create_basic_request_message(t, source='g')
    result_set = client_http.submit(message)
    results = []
    for result in result_set:
        results += result
    assert len(results) == 10

    t = g.V().limit(100)
    message = create_basic_request_message(t, source='g')
    result_set = client_http.submit(message)
    results = []
    for result in result_set:
        results += result
    assert len(results) == 100

    t = g.V().limit(1000)
    message = create_basic_request_message(t, source='g')
    result_set = client_http.submit(message)
    results = []
    for result in result_set:
        results += result
    assert len(results) == 1000

    # TODO: enable after setting up docker for GitHub or cleaning up server-side locally
    # t = g.V().limit(10000)
    # message = create_basic_request_message(t, source='g')
    # result_set = client_http.submit(message)
    # results = []
    # for result in result_set:
    #     results += result
    # assert len(results) == 10000


@pytest.mark.skip(reason="enable after making sure authenticated testing server is set up in docker")
def test_big_result_set_secure(authenticated_client_http):
    g = GraphTraversalSource(Graph(), TraversalStrategies())
    t = g.inject(1).repeat(__.add_v('person').property('name', __.loops())).times(20000).count()
    message = create_basic_request_message(t, source='g')
    result_set = authenticated_client_http.submit(message)
    results = []
    for result in result_set:
        results += result
    assert len(results) == 1

    t = g.V().limit(10)
    message = create_basic_request_message(t, source='g')
    result_set = authenticated_client_http.submit(message)
    results = []
    for result in result_set:
        results += result
    assert len(results) == 10

    t = g.V().limit(100)
    message = create_basic_request_message(t, source='g')
    result_set = authenticated_client_http.submit(message)
    results = []
    for result in result_set:
        results += result
    assert len(results) == 100

    t = g.V().limit(1000)
    message = create_basic_request_message(t, source='g')
    result_set = authenticated_client_http.submit(message)
    results = []
    for result in result_set:
        results += result
    assert len(results) == 1000

    # TODO: enable after setting up docker for GitHub or cleaning up server-side locally
    # t = g.V().limit(10000)
    # message = create_basic_request_message(t, source='g')
    # result_set = authenticated_client_http.submit(message)
    # results = []
    # for result in result_set:
    #     results += result
    # assert len(results) == 10000


async def asyncio_func():
    return 1


def test_asyncio(client_http):
    try:
        asyncio.get_event_loop().run_until_complete(asyncio_func())
    except RuntimeError:
        assert False


# TODO: tests pass because requestID is now generated on HTTP server and this option gets ignored, tests to be removed
#  or updated depending on if we still want to use requestID or not
def test_client_custom_invalid_request_id_graphson_script(client_http):
    client = Client(test_no_auth_http_url, 'gmodern')
    try:
        client.submit('g.V()', request_options={"requestId": "malformed"}).all().result()
    except Exception as ex:
        assert "badly formed hexadecimal UUID string" in str(ex)


def test_client_custom_invalid_request_id_graphbinary_script(client_http):
    client_http = Client(test_no_auth_http_url, 'gmodern')
    try:
        client_http.submit('g.V()', request_options={"requestId": "malformed"}).all().result()
    except Exception as ex:
        assert "badly formed hexadecimal UUID string" in str(ex)


def test_client_custom_valid_request_id_script_uuid(client_http):
    assert len(client_http.submit('g.V()', request_options={"requestId": uuid.uuid4()}).all().result()) == 6


def test_client_custom_valid_request_id_script_string(client_http):
    assert len(client_http.submit('g.V()', request_options={"requestId": str(uuid.uuid4())}).all().result()) == 6


def test_client_custom_invalid_request_id_graphson_bytecode(client_http):
    client_http = Client(test_no_auth_http_url, 'gmodern')
    query = GraphTraversalSource(Graph(), TraversalStrategies()).V().bytecode
    try:
        client_http.submit(query, request_options={"requestId": "malformed"}).all().result()
    except Exception as ex:
        assert "badly formed hexadecimal UUID string" in str(ex)


def test_client_custom_invalid_request_id_graphbinary_bytecode(client_http):
    client_http = Client(test_no_auth_http_url, 'gmodern')
    query = GraphTraversalSource(Graph(), TraversalStrategies()).V().bytecode
    try:
        client_http.submit(query, request_options={"requestId": "malformed"}).all().result()
    except Exception as ex:
        assert "badly formed hexadecimal UUID string" in str(ex)


def test_client_custom_valid_request_id_bytecode(client_http):
    query = GraphTraversalSource(Graph(), TraversalStrategies()).V().bytecode
    assert len(client_http.submit(query).all().result()) == 6
