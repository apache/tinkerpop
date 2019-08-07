"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""
import concurrent.futures
import pytest

from six.moves import queue

from gremlin_python.driver.client import Client
from gremlin_python.driver.connection import Connection
from gremlin_python.driver import serializer
from gremlin_python.driver.driver_remote_connection import (
    DriverRemoteConnection)
from gremlin_python.driver.protocol import GremlinServerWSProtocol
from gremlin_python.driver.serializer import (
    GraphSONMessageSerializer, GraphSONSerializersV2d0, GraphSONSerializersV3d0,
    GraphBinaryMessageSerializerV1)
from gremlin_python.driver.tornado.transport import TornadoTransport

gremlin_server_host = "localhost"
gremlin_server_url = 'ws://' + gremlin_server_host + ':45940/gremlin'


@pytest.fixture
def connection(request):
    protocol = GremlinServerWSProtocol(
        GraphSONMessageSerializer(),
        username='stephen', password='password')
    executor = concurrent.futures.ThreadPoolExecutor(5)
    pool = queue.Queue()
    try:
        conn = Connection(gremlin_server_url, 'gmodern', protocol,
                          lambda: TornadoTransport(), executor, pool)
    except OSError:
        executor.shutdown()
        pytest.skip('Gremlin Server is not running')
    else:
        def fin():
            executor.shutdown()
            conn.close()
        request.addfinalizer(fin)
        return conn


@pytest.fixture
def client(request):
    try:
        client = Client(gremlin_server_url, 'gmodern')
    except OSError:
        pytest.skip('Gremlin Server is not running')
    else:
        def fin():
            client.close()
        request.addfinalizer(fin)
        return client


@pytest.fixture
def secure_client(request):
    try:
        client = Client('ws://' + gremlin_server_host + ':45941/gremlin', 'gmodern', username='stephen', password='password')
    except OSError:
        pytest.skip('Gremlin Server is not running')
    else:
        def fin():
            client.close()
        request.addfinalizer(fin)
        return client


@pytest.fixture(params=['graphsonv2', 'graphsonv3', 'graphbinaryv1'])
def remote_connection(request):
    try:
        if request.param == 'graphbinaryv1':
            remote_conn = DriverRemoteConnection(gremlin_server_url, 'gmodern',
                                                 message_serializer=serializer.GraphBinaryMessageSerializerV1())
        elif request.param == 'graphsonv2':
            remote_conn = DriverRemoteConnection(gremlin_server_url, 'gmodern',
                                                 message_serializer=serializer.GraphSONSerializersV2d0())
        else:
            remote_conn = DriverRemoteConnection(gremlin_server_url, 'gmodern')
    except OSError:
        pytest.skip('Gremlin Server is not running')
    else:
        def fin():
            remote_conn.close()
        request.addfinalizer(fin)
        return remote_conn


@pytest.fixture
def remote_connection_v2(request):
    try:
        remote_conn = DriverRemoteConnection(gremlin_server_url, 'g',
                                             message_serializer=serializer.GraphSONSerializersV2d0())
    except OSError:
        pytest.skip('Gremlin Server is not running')
    else:
        def fin():
            remote_conn.close()
        request.addfinalizer(fin)
        return remote_conn


@pytest.fixture
def graphson_serializer_v2(request):
    return GraphSONSerializersV2d0()


@pytest.fixture
def graphson_serializer_v3(request):
    return GraphSONSerializersV3d0()


@pytest.fixture
def graphbinary_serializer_v1(request):
    return GraphBinaryMessageSerializerV1()
