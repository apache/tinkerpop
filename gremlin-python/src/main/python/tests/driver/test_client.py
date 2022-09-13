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

from gremlin_python.driver.client import Client
from gremlin_python.driver.protocol import GremlinServerError
from gremlin_python.driver.request import RequestMessage
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.strategies import OptionsStrategy
from gremlin_python.structure.graph import Graph
from gremlin_python.driver.aiohttp.transport import AiohttpTransport
from gremlin_python.statics import *
from asyncio import TimeoutError

__author__ = 'David M. Brown (davebshow@gmail.com)'

gremlin_server_url = os.environ.get('GREMLIN_SERVER_URL', 'ws://localhost:{}/gremlin')
test_no_auth_url = gremlin_server_url.format(45940)


def test_connection(connection):
    g = Graph().traversal()
    t = g.V()
    message = RequestMessage('traversal', 'bytecode', {'gremlin': t.bytecode, 'aliases': {'g': 'gmodern'}})
    results_set = connection.write(message).result()
    future = results_set.all()
    results = future.result()
    assert len(results) == 6
    assert isinstance(results, list)
    assert results_set.done.done()
    assert 'host' in results_set.status_attributes


def test_client_message_too_big(client):
    try:
        client = Client(test_no_auth_url, 'g', max_content_length=4096)
        client.submit("\" \"*8000").all().result()
        assert False
    except Exception as ex:
        assert ex.args[0].startswith("Received error on read: 'Message size") \
               and ex.args[0].endswith("exceeds limit 4096'")

        # confirm the client instance is still usable and not closed
        assert ["test"] == client.submit("'test'").all().result()
    finally:
        client.close()


def test_client_simple_eval(client):
    assert client.submit('1 + 1').all().result()[0] == 2


def test_client_simple_eval_bindings(client):
    assert client.submit('x + x', {'x': 2}).all().result()[0] == 4


def test_client_eval_traversal(client):
    assert len(client.submit('g.V()').all().result()) == 6


def test_client_error(client):
    try:
        # should fire an exception
        client.submit('1/0').all().result()
        assert False
    except GremlinServerError as ex:
        assert 'exceptions' in ex.status_attributes
        assert 'stackTrace' in ex.status_attributes
        assert str(ex) == f"{ex.status_code}: {ex.status_message}"


def test_client_connection_pool_after_error(client):
    # Overwrite fixture with pool_size=1 client
    client = Client(test_no_auth_url, 'gmodern', pool_size=1)

    try:
        # should fire an exception
        client.submit('1/0').all().result()
        assert False
    except GremlinServerError as gse:
        # expecting the pool size to be 1 again after query returned
        assert gse.status_code == 597
        assert client.available_pool_size == 1


def test_client_side_timeout_set_for_aiohttp(client):
    client = Client(test_no_auth_url, 'gmodern',
                    transport_factory=lambda: AiohttpTransport(read_timeout=1, write_timeout=1))

    try:
        # should fire an exception
        client.submit('Thread.sleep(2000);1').all().result()
        assert False
    except TimeoutError as err:
        # asyncio TimeoutError has no message.
        assert str(err) == ""


async def async_connect(enable):
    try:
        transport = AiohttpTransport(call_from_event_loop=enable)
        transport.connect(test_no_auth_url)
        transport.close()
        return True
    except RuntimeError:
        return False


def test_from_event_loop():
    assert not asyncio.get_event_loop().run_until_complete(async_connect(False))
    assert asyncio.get_event_loop().run_until_complete(async_connect(True))


def test_client_bytecode(client):
    g = Graph().traversal()
    t = g.V()
    message = RequestMessage('traversal', 'bytecode', {'gremlin': t.bytecode, 'aliases': {'g': 'gmodern'}})
    result_set = client.submit(message)
    assert len(result_set.all().result()) == 6


def test_client_bytecode_options(client):
    # smoke test to validate serialization of OptionsStrategy. no way to really validate this from an integration
    # test perspective because there's no way to access the internals of the strategy via bytecode
    g = Graph().traversal()
    t = g.withStrategies(OptionsStrategy(options={"x": "test", "y": True})).V()
    message = RequestMessage('traversal', 'bytecode', {'gremlin': t.bytecode, 'aliases': {'g': 'gmodern'}})
    result_set = client.submit(message)
    assert len(result_set.all().result()) == 6
    ##
    t = g.with_("x", "test").with_("y", True).V()
    message = RequestMessage('traversal', 'bytecode', {'gremlin': t.bytecode, 'aliases': {'g': 'gmodern'}})
    result_set = client.submit(message)
    assert len(result_set.all().result()) == 6


def test_iterate_result_set(client):
    g = Graph().traversal()
    t = g.V()
    message = RequestMessage('traversal', 'bytecode', {'gremlin': t.bytecode, 'aliases': {'g': 'gmodern'}})
    result_set = client.submit(message)
    results = []
    for result in result_set:
        results += result
    assert len(results) == 6


def test_client_async(client):
    g = Graph().traversal()
    t = g.V()
    message = RequestMessage('traversal', 'bytecode', {'gremlin': t.bytecode, 'aliases': {'g': 'gmodern'}})
    future = client.submit_async(message)
    result_set = future.result()
    assert len(result_set.all().result()) == 6


def test_connection_share(client):
    # Overwrite fixture with pool_size=1 client
    client = Client(test_no_auth_url, 'gmodern', pool_size=1)
    g = Graph().traversal()
    t = g.V()
    message = RequestMessage('traversal', 'bytecode', {'gremlin': t.bytecode, 'aliases': {'g': 'gmodern'}})
    message2 = RequestMessage('traversal', 'bytecode', {'gremlin': t.bytecode, 'aliases': {'g': 'gmodern'}})
    future = client.submit_async(message)
    future2 = client.submit_async(message2)

    result_set2 = future2.result()
    assert len(result_set2.all().result()) == 6

    # This future has to finish for the second to yield result - pool_size=1
    assert future.done()
    result_set = future.result()
    assert len(result_set.all().result()) == 6


def test_multi_conn_pool(client):
    g = Graph().traversal()
    t = g.V()
    message = RequestMessage('traversal', 'bytecode', {'gremlin': t.bytecode, 'aliases': {'g': 'gmodern'}})
    message2 = RequestMessage('traversal', 'bytecode', {'gremlin': t.bytecode, 'aliases': {'g': 'gmodern'}})
    client = Client(test_no_auth_url, 'g', pool_size=1)
    future = client.submit_async(message)
    future2 = client.submit_async(message2)

    result_set2 = future2.result()
    assert len(result_set2.all().result()) == 6

    # with connection pool `future` may or may not be done here
    result_set = future.result()
    assert len(result_set.all().result()) == 6


def test_multi_thread_pool(client):
    g = Graph().traversal()
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
        message = RequestMessage('traversal', 'bytecode', {'gremlin': tr.bytecode, 'aliases': {'g': 'gmodern'}})
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
    assert results[1][0][0].object == 6
    assert len(results[2][0]) == 6
    assert results[3][0][0].object == 6


def test_client_bytecode_with_long(client):
    g = Graph().traversal()
    t = g.V().has('age', long(851401972585122)).count()
    message = RequestMessage('traversal', 'bytecode', {'gremlin': t.bytecode, 'aliases': {'g': 'gmodern'}})
    result_set = client.submit(message)
    results = []
    for result in result_set:
        results += result
    assert len(results) == 1


def test_client_bytecode_with_bigint(client):
    g = Graph().traversal()
    t = g.V().has('age', bigint(0x1000_0000_0000_0000_0000)).count()
    message = RequestMessage('traversal', 'bytecode', {'gremlin': t.bytecode, 'aliases': {'g': 'gmodern'}})
    result_set = client.submit(message)
    results = []
    for result in result_set:
        results += result
    assert len(results) == 1


def test_multi_request_in_session(client):
    # Overwrite fixture with session client
    session_id = str(uuid.uuid4())
    client = Client(test_no_auth_url, 'g', session=session_id)

    assert client.submit('x = 1').all().result()[0] == 1
    assert client.submit('x + 2').all().result()[0] == 3

    client.close()

    # attempt reconnect to session and make sure "x" is no longer a thing
    client = Client(test_no_auth_url, 'g', session=session_id)
    try:
        # should fire an exception
        client.submit('x').all().result()
        assert False
    except Exception:
        assert True


def test_client_pool_in_session(client):
    # Overwrite fixture with pool_size=2 client
    try:
        # should fire an exception
        client = Client(test_no_auth_url, 'g', session=str(uuid.uuid4()), pool_size=2)
        assert False
    except Exception:
        assert True


def test_big_result_set(client):
    g = Graph().traversal()
    t = g.inject(1).repeat(__.addV('person').property('name', __.loops())).times(20000).count()
    message = RequestMessage('traversal', 'bytecode', {'gremlin': t.bytecode, 'aliases': {'g': 'g'}})
    result_set = client.submit(message)
    results = []
    for result in result_set:
        results += result
    assert len(results) == 1

    t = g.V().limit(10)
    message = RequestMessage('traversal', 'bytecode', {'gremlin': t.bytecode, 'aliases': {'g': 'g'}})
    result_set = client.submit(message)
    results = []
    for result in result_set:
        results += result
    assert len(results) == 10

    t = g.V().limit(100)
    message = RequestMessage('traversal', 'bytecode', {'gremlin': t.bytecode, 'aliases': {'g': 'g'}})
    result_set = client.submit(message)
    results = []
    for result in result_set:
        results += result
    assert len(results) == 100

    t = g.V().limit(1000)
    message = RequestMessage('traversal', 'bytecode', {'gremlin': t.bytecode, 'aliases': {'g': 'g'}})
    result_set = client.submit(message)
    results = []
    for result in result_set:
        results += result
    assert len(results) == 1000

    t = g.V().limit(10000)
    message = RequestMessage('traversal', 'bytecode', {'gremlin': t.bytecode, 'aliases': {'g': 'g'}})
    result_set = client.submit(message)
    results = []
    for result in result_set:
        results += result
    assert len(results) == 10000


def test_big_result_set_secure(authenticated_client):
    g = Graph().traversal()
    t = g.inject(1).repeat(__.addV('person').property('name', __.loops())).times(20000).count()
    message = RequestMessage('traversal', 'bytecode', {'gremlin': t.bytecode, 'aliases': {'g': 'g'}})
    result_set = authenticated_client.submit(message)
    results = []
    for result in result_set:
        results += result
    assert len(results) == 1

    t = g.V().limit(10)
    message = RequestMessage('traversal', 'bytecode', {'gremlin': t.bytecode, 'aliases': {'g': 'g'}})
    result_set = authenticated_client.submit(message)
    results = []
    for result in result_set:
        results += result
    assert len(results) == 10

    t = g.V().limit(100)
    message = RequestMessage('traversal', 'bytecode', {'gremlin': t.bytecode, 'aliases': {'g': 'g'}})
    result_set = authenticated_client.submit(message)
    results = []
    for result in result_set:
        results += result
    assert len(results) == 100

    t = g.V().limit(1000)
    message = RequestMessage('traversal', 'bytecode', {'gremlin': t.bytecode, 'aliases': {'g': 'g'}})
    result_set = authenticated_client.submit(message)
    results = []
    for result in result_set:
        results += result
    assert len(results) == 1000

    t = g.V().limit(10000)
    message = RequestMessage('traversal', 'bytecode', {'gremlin': t.bytecode, 'aliases': {'g': 'g'}})
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
