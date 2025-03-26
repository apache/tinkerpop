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
from gremlin_python.driver.client import Client
from gremlin_python.driver.protocol import GremlinServerError
from gremlin_python.driver.request import RequestMessage
from gremlin_python.driver.serializer import GraphBinarySerializersV4
from gremlin_python.process.graph_traversal import __, GraphTraversalSource
from gremlin_python.process.traversal import TraversalStrategies, GValue
from gremlin_python.process.strategies import OptionsStrategy
from gremlin_python.structure.graph import Graph, Vertex
from gremlin_python.driver.aiohttp.transport import AiohttpHTTPTransport
from gremlin_python.statics import *
from asyncio import TimeoutError

__author__ = 'David M. Brown (davebshow@gmail.com)'

gremlin_server_url = os.environ.get('GREMLIN_SERVER_URL', 'http://localhost:{}/gremlin')
test_no_auth_url = gremlin_server_url.format(45940)


def create_basic_request_message(traversal, source='gmodern'):
    return RequestMessage(fields={'g': source}, gremlin=traversal.gremlin_lang.get_gremlin())


def test_connection(connection):
    g = GraphTraversalSource(Graph(), TraversalStrategies())
    t = g.V()
    message = create_basic_request_message(t)
    results_set = connection.write(message).result()
    future = results_set.all()
    results = future.result()
    assert len(results) == 6
    assert isinstance(results, list)
    assert results_set.done.done()

# TODO: revisit after max_content_length definition/implementation is updated
def test_client_message_too_big(client):
    try:
        client = Client(test_no_auth_url, 'g', max_content_length=1024)
        client.submit("g.inject(' ').repeat(concat(' ')).times(1234)").all().result()
        assert False
    except Exception as ex:
        assert ex.args[0].startswith('Response size') \
               and ex.args[0].endswith('exceeds limit 1024 bytes')

        # confirm the client instance is still usable and not closed
        assert ["test"] == client.submit("g.inject('test')").all().result()
    finally:
        client.close()


def test_client_large_result(client):
    result_set = client.submit("[\" \".repeat(200000), \" \".repeat(100000)]", request_options={'language': 'gremlin-groovy'}).all().result()
    assert len(result_set[0]) == 200000
    assert len(result_set[1]) == 100000


def test_client_script_submission(client):
    assert len(client.submit("g.inject(0).repeat(inject(0)).times(99)").all().result()) == 100


def test_client_simple_eval(client):
    assert client.submit('g.inject(2)').all().result()[0] == 2


def test_client_simple_eval_bindings(client):
    assert client.submit('g.inject(x).math("_+_")', {'x': 2}).all().result()[0] == 4.0


def test_client_eval_traversal(client):
    assert len(client.submit('g.V()').all().result()) == 6


def test_client_eval_traversal_bindings(client):
    assert client.submit('g.V(x).values("name")', bindings={'x': 1}).all().result()[0] == 'marko'


def test_client_eval_traversal_request_options_bindings(client):
    assert client.submit('g.V(x).values("name")', request_options={'bindings': {'x': 1}}).all().result()[0] == 'marko'


def test_client_eval_traversal_bindings_request_options_bindings(client):
    # Note that parameters from request_options[bindings] is applied later and will replace bindings if key is the same
    assert client.submit('g.V(x).values("name")', bindings={'x': 1},
                         request_options={'bindings': {'x': 2}}).all().result()[0] == 'vadas'


def test_client_error(client):
    try:
        # should fire an exception
        client.submit('g.inject(1).math("_/0")').all().result()
        assert False
    except GremlinServerError as ex:
        assert ex.status_message.endswith('Division by zero!')
        assert ex.status_exception
        assert str(ex) == f"{ex.status_code}: {ex.status_message}"

    # still can submit after failure
    assert client.submit('g.inject(x).math("_+_")', {'x': 2}).all().result()[0] == 4.0


def test_bad_serialization(client):
    try:
        # should timeout
        client.submit('java.awt.Color.RED', request_options={'language': 'gremlin-groovy'}).all().result()
        assert False
    except GremlinServerError as ex:
        assert ex.status_message
        assert ex.status_exception
        assert str(ex) == f"{ex.status_code}: {ex.status_message}"

    # still can submit after failure
    assert client.submit('g.inject(x).math("_+_")', {'x': 2}).all().result()[0] == 4.0


def test_client_connection_pool_after_error(client):
    # Overwrite fixture with pool_size=1 client
    client = Client(test_no_auth_url, 'gmodern', pool_size=1)

    try:
        # should fire an exception
        client.submit('g.inject(1).math("_/0")').all().result()
        assert False
    except GremlinServerError as gse:
        # expecting the pool size to be 1 again after query returned
        assert gse.status_code == 500
        assert client.available_pool_size == 1

    # still can submit after failure
    assert client.submit('g.inject(x).math("_+_")', {'x': 2}).all().result()[0] == 4


def test_client_no_hang_if_submit_on_closed(client):
    assert client.submit('g.inject(2)').all().result()[0] == 2
    client.close()
    try:
        # should fail since not hang if closed
        client.submit('g.inject(2)').all().result()
        assert False
    except Exception as ex:
        assert True


def test_client_close_all_connection_in_pool(client):
    client = Client(test_no_auth_url, 'g', pool_size=1)
    assert client.available_pool_size == 1
    client.submit('g.inject(4)').all().result()
    client.close()
    assert client.available_pool_size == 0


def test_client_side_timeout_set_for_aiohttp(client):
    client = Client(test_no_auth_url, 'gmodern',
                    transport_factory=lambda: AiohttpHTTPTransport(read_timeout=1, write_timeout=1))

    try:
        # should fire an exception
        client.submit('Thread.sleep(2000);1', request_options={'language': 'gremlin-groovy'}).all().result()
        assert False
    except TimeoutError as err:
        # asyncio TimeoutError has no message.
        assert str(err) == ""

    # still can submit after failure
    assert client.submit('g.inject(x).math("_+_")', {'x': 2}).all().result()[0] == 4.0


async def async_connect(enable):
    try:
        transport = AiohttpHTTPTransport(call_from_event_loop=enable)
        transport.connect(test_no_auth_url)
        transport.close()
        return True
    except RuntimeError:
        return False


def test_from_event_loop():
    assert not asyncio.get_event_loop().run_until_complete(async_connect(False))
    assert asyncio.get_event_loop().run_until_complete(async_connect(True))


def test_client_submit(client):
    result_set = client.submit('g.V(1)')
    result = result_set.all().result()
    assert 1 == len(result)
    vertex = result[0]
    assert type(vertex) is Vertex
    assert 1 == vertex.id
    assert 2 == len(vertex.properties)
    assert 'name' == vertex.properties[0].key
    assert 'marko' == vertex.properties[0].value
    ##
    result_set = client.submit('g.with("materializeProperties", "tokens").V(1)')
    result = result_set.all().result()
    assert 1 == len(result)
    vertex = result[0]
    assert 1 == vertex.id
    assert 0 == len(vertex.properties)
    ##
    result_set = client.submit('g.with("materializeProperties", "tokens").E(7)')
    result = result_set.all().result()
    assert 1 == len(result)
    edge = result[0]
    assert 7 == edge.id
    assert 0 == len(edge.properties)
    ##
    result_set = client.submit('g.with("materializeProperties", "tokens").V(1).properties()')
    result = result_set.all().result()
    assert 2 == len(result)
    for vp in result:
        assert 0 == len(vp.properties)


def test_client_gremlin_lang(client):
    g = GraphTraversalSource(Graph(), TraversalStrategies())
    t = g.V()
    message = create_basic_request_message(t)
    result_set = client.submit(message)
    assert len(result_set.all().result()) == 6


def test_client_gremlin_lang_options(client):
    # smoke test to validate serialization of OptionsStrategy. no way to really validate this from an integration
    # test perspective because there's no way to access the internals of the strategy via bytecode
    g = GraphTraversalSource(Graph(), TraversalStrategies())
    t = g.with_strategies(OptionsStrategy(**{"x": "test", "y": True})).V()
    message = create_basic_request_message(t)
    result_set = client.submit(message)
    assert len(result_set.all().result()) == 6
    ##
    t = g.with_("x", "test").with_("y", True).V()
    message = create_basic_request_message(t)
    result_set = client.submit(message)
    assert len(result_set.all().result()) == 6


def test_client_gremlin_lang_request_options_with_binding(client):
    g = GraphTraversalSource(Graph(), TraversalStrategies())
    # Note that bindings for constructed traversals is done via Parameter only
    t = g.with_('language', 'gremlin-lang').V(GValue('x', [1, 2, 3])).count()
    request_opts = {'language': 'gremlin-lang', 'params': {'x': [1, 2, 3]}}
    message = create_basic_request_message(t)
    result_set = client.submit(message, request_options=request_opts)
    assert result_set.all().result()[0] == 3
    # We can re-use the extracted request options in script submission
    result_set = client.submit('g.V(x).values("name")', request_options=request_opts)
    assert result_set.all().result()[0] == 'marko'
    # For script submission only, we can also add bindings to request options and they will be applied
    request_opts['bindings'] = {'y': 4}
    result_set = client.submit('g.V(y).values("name")', request_options=request_opts)
    assert result_set.all().result()[0] == 'josh'
    result_set = client.submit('g.V(z).values("name")', bindings={'z': 5}, request_options=request_opts)
    assert result_set.all().result()[0] == 'ripple'


def test_iterate_result_set(client):
    g = GraphTraversalSource(Graph(), TraversalStrategies())
    t = g.V()
    message = create_basic_request_message(t)
    result_set = client.submit(message)
    results = []
    for result in result_set:
        results += result
    assert len(results) == 6


def test_client_async(client):
    g = GraphTraversalSource(Graph(), TraversalStrategies())
    t = g.V()
    message = create_basic_request_message(t)
    future = client.submit_async(message)
    result_set = future.result()
    assert len(result_set.all().result()) == 6


def test_connection_share(client):
    # Overwrite fixture with pool_size=1 client
    client = Client(test_no_auth_url, 'gmodern', pool_size=1)
    g = GraphTraversalSource(Graph(), TraversalStrategies())
    t = g.V()
    message = create_basic_request_message(t)
    message2 = create_basic_request_message(t)
    future = client.submit_async(message)
    future2 = client.submit_async(message2)

    result_set2 = future2.result()
    assert len(result_set2.all().result()) == 6

    # This future has to finish for the second to yield result - pool_size=1
    assert future.done()
    result_set = future.result()
    assert len(result_set.all().result()) == 6


def test_multi_conn_pool(client):
    g = GraphTraversalSource(Graph(), TraversalStrategies())
    t = g.V()
    message = create_basic_request_message(t)
    message2 = create_basic_request_message(t)
    client = Client(test_no_auth_url, 'g', pool_size=1)
    future = client.submit_async(message)
    future2 = client.submit_async(message2)

    result_set2 = future2.result()
    assert len(result_set2.all().result()) == 6

    # with connection pool `future` may or may not be done here
    result_set = future.result()
    assert len(result_set.all().result()) == 6


def test_multi_thread_pool(client):
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
        result_set = client.submit(message)
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
    assert results[1][0][0] == 6
    assert len(results[2][0]) == 6
    assert results[3][0][0] == 6


def test_client_gremlin_lang_with_short(client):
    g = GraphTraversalSource(Graph(), TraversalStrategies())
    t = g.with_('language', 'gremlin-lang').V().has('age', short(16)).count()
    request_opts = {'language': 'gremlin-lang'}
    message = create_basic_request_message(t)
    result_set = client.submit(message, request_options=request_opts)
    results = []
    for result in result_set:
        results += result
    assert len(results) == 1


def test_client_gremlin_lang_with_long(client):
    g = GraphTraversalSource(Graph(), TraversalStrategies())
    t = g.V().has('age', long(851401972585122)).count()
    request_opts = {}
    message = create_basic_request_message(t)
    result_set = client.submit(message, request_options=request_opts)
    results = []
    for result in result_set:
        results += result
    assert len(results) == 1


def test_client_gremlin_lang_with_bigint(client):
    g = GraphTraversalSource(Graph(), TraversalStrategies())
    t = g.with_('language', 'gremlin-lang').V().has('age', bigint(0x1000_0000_0000_0000_0000)).count()
    request_opts = {'language': 'gremlin-lang'}
    message = create_basic_request_message(t)
    result_set = client.submit(message, request_options=request_opts)
    results = []
    for result in result_set:
        results += result
    assert len(results) == 1


def test_big_result_set(client):
    g = GraphTraversalSource(Graph(), TraversalStrategies())
    t = g.inject(1).repeat(__.add_v('person').property('name', __.loops())).times(20000).count()
    message = create_basic_request_message(t, source='g')
    result_set = client.submit(message)
    results = []
    for result in result_set:
        results += result
    assert len(results) == 1

    t = g.V().limit(10)
    message = create_basic_request_message(t, source='g')
    result_set = client.submit(message)
    results = []
    for result in result_set:
        results += result
    assert len(results) == 10

    t = g.V().limit(100)
    message = create_basic_request_message(t, source='g')
    result_set = client.submit(message)
    results = []
    for result in result_set:
        results += result
    assert len(results) == 100

    t = g.V().limit(1000)
    message = create_basic_request_message(t, source='g')
    result_set = client.submit(message)
    results = []
    for result in result_set:
        results += result
    assert len(results) == 1000

    t = g.V().limit(10000)
    message = create_basic_request_message(t, source='g')
    result_set = client.submit(message)
    results = []
    for result in result_set:
        results += result
    assert len(results) == 10000


def test_big_result_set_secure(authenticated_client):
    g = GraphTraversalSource(Graph(), TraversalStrategies())
    t = g.inject(1).repeat(__.add_v('person').property('name', __.loops())).times(20000).count()
    message = create_basic_request_message(t, source='g')
    result_set = authenticated_client.submit(message)
    results = []
    for result in result_set:
        results += result
    assert len(results) == 1

    t = g.V().limit(10)
    message = create_basic_request_message(t, source='g')
    result_set = authenticated_client.submit(message)
    results = []
    for result in result_set:
        results += result
    assert len(results) == 10

    t = g.V().limit(100)
    message = create_basic_request_message(t, source='g')
    result_set = authenticated_client.submit(message)
    results = []
    for result in result_set:
        results += result
    assert len(results) == 100

    t = g.V().limit(1000)
    message = create_basic_request_message(t, source='g')
    result_set = authenticated_client.submit(message)
    results = []
    for result in result_set:
        results += result
    assert len(results) == 1000

    t = g.V().limit(10000)
    message = create_basic_request_message(t, source='g')
    result_set = authenticated_client.submit(message)
    results = []
    for result in result_set:
        results += result
    assert len(results) == 10000


async def asyncio_func():
    return 1


def test_asyncio(client):
    try:
        asyncio.get_event_loop().run_until_complete(asyncio_func())
    except RuntimeError:
        assert False


# TODO: tests pass because requestID is now generated on HTTP server and this option gets ignored, tests to be removed
#  or updated depending on if we still want to use requestID or not
@pytest.mark.skip(reason="requestID is generated on server side only, disable for now")
def test_client_custom_invalid_request_id_graphson_script(client):
    client = Client(test_no_auth_url, 'gmodern')
    try:
        client.submit('g.V()', request_options={"requestId": "malformed"}).all().result()
    except Exception as ex:
        assert "badly formed hexadecimal UUID string" in str(ex)


@pytest.mark.skip(reason="requestID is generated on server side only, disable for now")
def test_client_custom_invalid_request_id_graphbinary_script(client):
    client = Client(test_no_auth_url, 'gmodern')
    try:
        client.submit('g.V()', request_options={"requestId": "malformed"}).all().result()
    except Exception as ex:
        assert "badly formed hexadecimal UUID string" in str(ex)


@pytest.mark.skip(reason="requestID is generated on server side only, disable for now")
def test_client_custom_valid_request_id_script_uuid(client):
    assert len(client.submit('g.V()', request_options={"requestId": uuid.uuid4()}).all().result()) == 6


@pytest.mark.skip(reason="requestID is generated on server side only, disable for now")
def test_client_custom_valid_request_id_script_string(client):
    assert len(client.submit('g.V()', request_options={"requestId": str(uuid.uuid4())}).all().result()) == 6


@pytest.mark.skip(reason="requestID is generated on server side only, disable for now")
def test_client_custom_invalid_request_id_graphson_bytecode(client):
    client = Client(test_no_auth_url, 'gmodern')
    query = GraphTraversalSource(Graph(), TraversalStrategies()).V().bytecode
    try:
        client.submit(query, request_options={"requestId": "malformed"}).all().result()
    except Exception as ex:
        assert "badly formed hexadecimal UUID string" in str(ex)


@pytest.mark.skip(reason="requestID is generated on server side only, disable for now")
def test_client_custom_invalid_request_id_graphbinary_bytecode(client):
    client = Client(test_no_auth_url, 'gmodern')
    query = GraphTraversalSource(Graph(), TraversalStrategies()).V().bytecode
    try:
        client.submit(query, request_options={"requestId": "malformed"}).all().result()
    except Exception as ex:
        assert "badly formed hexadecimal UUID string" in str(ex)


@pytest.mark.skip(reason="requestID is generated on server side only, disable for now")
def test_client_custom_valid_request_id_bytecode(client):
    query = GraphTraversalSource(Graph(), TraversalStrategies()).V().bytecode
    assert len(client.submit(query).all().result()) == 6

def test_response_serializer_never_None():
    client = Client('url', 'g', response_serializer=None)
    resp_ser = client.response_serializer()
    assert resp_ser is not None


def test_serializer_and_interceptor_forwarded(client_with_interceptor):
    result = client_with_interceptor.submit("g.inject(1)").next()
    assert [2] == result # interceptor changes request to g.inject(2)
