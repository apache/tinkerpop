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

import concurrent.futures
from json import dumps
import os
import ssl
import pytest
import logging
import queue

from gremlin_python.driver import serializer
from gremlin_python.driver.client import Client
from gremlin_python.driver.connection import Connection
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.driver.protocol import GremlinServerHTTPProtocol
from gremlin_python.driver.serializer import GraphBinarySerializersV4
from gremlin_python.driver.aiohttp.transport import AiohttpHTTPTransport
from gremlin_python.driver.auth import basic, sigv4

"""HTTP server testing variables"""
gremlin_server_url = os.environ.get('GREMLIN_SERVER_URL', 'http://localhost:{}/gremlin')
gremlin_basic_auth_url = os.environ.get('GREMLIN_SERVER_BASIC_AUTH_URL', 'https://localhost:{}/gremlin')
anonymous_url = gremlin_server_url.format(45940)
basic_url = gremlin_basic_auth_url.format(45941)

verbose_logging = False

logging.basicConfig(format='%(asctime)s [%(levelname)8s] [%(filename)15s:%(lineno)d - %(funcName)10s()] - %(message)s',
                    level=logging.DEBUG if verbose_logging else logging.INFO)

"""
Tests below are for the HTTP server with GraphBinaryV4
"""
@pytest.fixture
def connection(request):
    protocol = GremlinServerHTTPProtocol(
        GraphBinarySerializersV4(), GraphBinarySerializersV4(),
        auth=basic('stephen', 'password'))
    executor = concurrent.futures.ThreadPoolExecutor(5)
    pool = queue.Queue()
    try:
        conn = Connection(anonymous_url, 'gmodern', protocol,
                          lambda: AiohttpHTTPTransport(), executor, pool)
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
        client = Client(anonymous_url, 'gmodern')
    except OSError:
        pytest.skip('Gremlin Server is not running')
    else:
        def fin():
            client.close()

        request.addfinalizer(fin)
        return client


@pytest.fixture(params=['basic'])
def authenticated_client(request):
    try:
        if request.param == 'basic':
            # turn off certificate verification for testing purposes only
            ssl_opts = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
            ssl_opts.verify_mode = ssl.CERT_NONE
            client = Client(basic_url, 'gmodern',
                            auth=basic('stephen', 'password'),
                            transport_factory=lambda: AiohttpHTTPTransport(ssl_options=ssl_opts))
        else:
            raise ValueError("Invalid authentication option - " + request.param)
    except OSError:
        pytest.skip('Gremlin Server is not running')
    else:
        def fin():
            client.close()

        request.addfinalizer(fin)
        return client


@pytest.fixture
def graphbinary_serializer_v4(request):
    return GraphBinarySerializersV4()


@pytest.fixture(params=['graphbinaryv4','graphsonv4'])
def remote_connection(request):
    try:
        if request.param == 'graphbinaryv4':
            remote_conn = DriverRemoteConnection(anonymous_url, 'gmodern',
                                                 request_serializer=serializer.GraphBinarySerializersV4(),
                                                 response_serializer=serializer.GraphBinarySerializersV4())
        elif request.param == 'graphsonv4':
            remote_conn = DriverRemoteConnection(anonymous_url, 'gmodern',
                                                 request_serializer=serializer.GraphSONSerializersV4(),
                                                 response_serializer=serializer.GraphSONSerializersV4())
        else:
            raise ValueError("Invalid serializer option - " + request.param)
    except OSError:
        pytest.skip('Gremlin Server is not running')
    else:
        def fin():
            remote_conn.close()

        request.addfinalizer(fin)
        return remote_conn


@pytest.fixture(params=['graphbinaryv4','graphsonv4'])
def remote_connection_crew(request):
    try:
        if request.param == 'graphbinaryv4':
            remote_conn = DriverRemoteConnection(anonymous_url, 'gcrew')
        elif request.param == 'graphsonv4':
            remote_conn = DriverRemoteConnection(anonymous_url, 'gcrew',
                                                 request_serializer=serializer.GraphSONSerializersV4(),
                                                 response_serializer=serializer.GraphSONSerializersV4())
        else:
            raise ValueError("Invalid serializer option - " + request.param)
    except OSError:
        pytest.skip('Gremlin Server is not running')
    else:
        def fin():
            remote_conn.close()

        request.addfinalizer(fin)
        return remote_conn


# TODO: revisit once testing for multiple types of auth is enabled
@pytest.fixture(params=['basic'])
def remote_connection_authenticated(request):
    try:
        if request.param == 'basic':
            # turn off certificate verification for testing purposes only
            ssl_opts = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
            ssl_opts.verify_mode = ssl.CERT_NONE
            remote_conn = DriverRemoteConnection(basic_url, 'gmodern',
                                                 auth=basic('stephen', 'password'),
                                                 transport_factory=lambda: AiohttpHTTPTransport(ssl_options=ssl_opts))
        else:
            raise ValueError("Invalid authentication option - " + request.param)
    except OSError:
        pytest.skip('Gremlin Server is not running')
    else:
        def fin():
            remote_conn.close()

        request.addfinalizer(fin)
        return remote_conn


@pytest.fixture(params=['graphbinaryv4'])
def invalid_alias_remote_connection(request):
    try:
        if request.param == 'graphbinaryv4':
            remote_conn = DriverRemoteConnection(anonymous_url, 'does_not_exist')
        else:
            raise ValueError("Invalid serializer option - " + request.param)
    except OSError:
        pytest.skip('Gremlin Server is not running')
    else:
        def fin():
            remote_conn.close()

        request.addfinalizer(fin)
        return remote_conn


@pytest.fixture()
def remote_connection_with_interceptor(request):
    try:
        remote_conn = DriverRemoteConnection(anonymous_url, 'gmodern',
                                             request_serializer=None,
                                             interceptors=json_interceptor)
    except OSError:
        pytest.skip('Gremlin Server is not running')
    else:
        def fin():
            remote_conn.close()

        request.addfinalizer(fin)
        return remote_conn


@pytest.fixture()
def client_with_interceptor(request):
    try:
        client = Client(anonymous_url, 'gmodern', request_serializer=None,
                        response_serializer=GraphBinarySerializersV4(),
                        interceptors=json_interceptor)
    except OSError:
        pytest.skip('Gremlin Server is not running')
    else:
        def fin():
            client.close()

        request.addfinalizer(fin)
        return client


def json_interceptor(request):
        request['headers']['content-type'] = "application/json"
        request['payload'] = dumps({"gremlin": "g.inject(2)", "g": "g"})
        return request
