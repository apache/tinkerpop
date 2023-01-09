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
import os
import ssl
import pytest
import socket
import logging
import queue

import yaml

from gremlin_python.driver.client import Client
from gremlin_python.driver.connection import Connection
from gremlin_python.driver import serializer
from gremlin_python.driver.driver_remote_connection import (
    DriverRemoteConnection)
from gremlin_python.driver.protocol import GremlinServerWSProtocol
from gremlin_python.driver.serializer import (
    GraphSONMessageSerializer, GraphSONSerializersV2d0, GraphSONSerializersV3d0,
    GraphBinarySerializersV1)
from gremlin_python.driver.aiohttp.transport import AiohttpTransport

gremlin_server_url = os.environ.get('GREMLIN_SERVER_URL', 'ws://localhost:{}/gremlin')
gremlin_basic_auth_url = os.environ.get('GREMLIN_SERVER_BASIC_AUTH_URL', 'wss://localhost:{}/gremlin')
gremlin_socket_server_url = os.environ.get('GREMLIN_SOCKET_SERVER_URL', 'ws://localhost:{}/gremlin')
gremlin_socket_server_config_path = os.environ.get("GREMLIN_SOCKET_SERVER_CONFIG_PATH",
                                                   "../../../../../../gremlin-tools/gremlin-socket-server/conf/"
                                                   "test-ws-gremlin.yaml")
kerberos_hostname = os.environ.get('KRB_HOSTNAME', socket.gethostname())
anonymous_url = gremlin_server_url.format(45940)
basic_url = gremlin_basic_auth_url.format(45941)
kerberos_url = gremlin_server_url.format(45942)

kerberized_service = 'test-service@{}'.format(kerberos_hostname)
verbose_logging = False

logging.basicConfig(format='%(asctime)s [%(levelname)8s] [%(filename)15s:%(lineno)d - %(funcName)10s()] - %(message)s',
                    level=logging.DEBUG if verbose_logging else logging.INFO)


@pytest.fixture
def connection(request):
    protocol = GremlinServerWSProtocol(
        GraphSONMessageSerializer(),
        username='stephen', password='password')
    executor = concurrent.futures.ThreadPoolExecutor(5)
    pool = queue.Queue()
    try:
        conn = Connection(anonymous_url, 'gmodern', protocol,
                          lambda: AiohttpTransport(), executor, pool)
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


@pytest.fixture
def gremlin_socket_server_serializer(socket_server_settings):
    if socket_server_settings["SERIALIZER"] == "GraphBinaryV1":
        return serializer.GraphBinarySerializersV1()
    elif socket_server_settings["SERIALIZER"] == "GraphSONV2":
        return serializer.GraphSONSerializersV2d0()
    elif socket_server_settings["SERIALIZER"] == "GraphSONV3":
        return serializer.GraphSONSerializersV3d0()
    else:
        return serializer.GraphBinarySerializersV1()


@pytest.fixture
def socket_server_client(request, socket_server_settings, gremlin_socket_server_serializer):
    url = gremlin_socket_server_url.format(socket_server_settings["PORT"])
    try:
        client = Client(url, 'g', pool_size=1, message_serializer=gremlin_socket_server_serializer)
    except OSError:
        pytest.skip('Gremlin Socket Server is not running')
    else:
        def fin():
            client.close()

        request.addfinalizer(fin)
        return client


@pytest.fixture
def socket_server_client_no_user_agent(request, socket_server_settings, gremlin_socket_server_serializer):
    url = gremlin_socket_server_url.format(socket_server_settings["PORT"])
    try:
        client = Client(url, 'g', pool_size=1, message_serializer=gremlin_socket_server_serializer,
                        enable_user_agent_on_connect=False)
    except OSError:
        pytest.skip('Gremlin Socket Server is not running')
    else:
        def fin():
            client.close()

        request.addfinalizer(fin)
        return client


@pytest.fixture
def socket_server_settings():
    with open(gremlin_socket_server_config_path, mode="rb") as file:
        settings = yaml.safe_load(file)
    return settings

@pytest.fixture(params=['basic', 'kerberos'])
def authenticated_client(request):
    try:
        if request.param == 'basic':
            # turn off certificate verification for testing purposes only
            ssl_opts = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
            ssl_opts.verify_mode = ssl.CERT_NONE
            client = Client(basic_url, 'gmodern', username='stephen', password='password',
                            transport_factory=lambda: AiohttpTransport(ssl_options=ssl_opts))
        elif request.param == 'kerberos':
            client = Client(kerberos_url, 'gmodern', kerberized_service=kerberized_service)
        else:
            raise ValueError("Invalid authentication option - " + request.param)
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
            remote_conn = DriverRemoteConnection(anonymous_url, 'gmodern',
                                                 message_serializer=serializer.GraphBinarySerializersV1())
        elif request.param == 'graphsonv2':
            remote_conn = DriverRemoteConnection(anonymous_url, 'gmodern',
                                                 message_serializer=serializer.GraphSONSerializersV2d0())
        elif request.param == 'graphsonv3':
            remote_conn = DriverRemoteConnection(anonymous_url, 'gmodern',
                                                 message_serializer=serializer.GraphSONSerializersV3d0())
        else:
            raise ValueError("Invalid serializer option - " + request.param)
    except OSError:
        pytest.skip('Gremlin Server is not running')
    else:
        def fin():
            remote_conn.close()

        request.addfinalizer(fin)
        return remote_conn


@pytest.fixture(params=['graphsonv2', 'graphsonv3', 'graphbinaryv1'])
def remote_transaction_connection(request):
    try:
        if request.param == 'graphbinaryv1':
            remote_conn = DriverRemoteConnection(anonymous_url, 'gtx',
                                                 message_serializer=serializer.GraphBinarySerializersV1())
        elif request.param == 'graphsonv2':
            remote_conn = DriverRemoteConnection(anonymous_url, 'gtx',
                                                 message_serializer=serializer.GraphSONSerializersV2d0())
        elif request.param == 'graphsonv3':
            remote_conn = DriverRemoteConnection(anonymous_url, 'gtx',
                                                 message_serializer=serializer.GraphSONSerializersV3d0())
        else:
            raise ValueError("Invalid serializer option - " + request.param)
    except OSError:
        pytest.skip('Gremlin Server is not running')
    else:
        def fin():
            remote_conn.close()

        request.addfinalizer(fin)
        return remote_conn


@pytest.fixture(params=['basic', 'kerberos'])
def remote_connection_authenticated(request):
    try:
        if request.param == 'basic':
            # turn off certificate verification for testing purposes only
            ssl_opts = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
            ssl_opts.verify_mode = ssl.CERT_NONE
            remote_conn = DriverRemoteConnection(basic_url, 'gmodern',
                                                 username='stephen', password='password',
                                                 message_serializer=serializer.GraphSONSerializersV2d0(),
                                                 transport_factory=lambda: AiohttpTransport(ssl_options=ssl_opts))
        elif request.param == 'kerberos':
            remote_conn = DriverRemoteConnection(kerberos_url, 'gmodern', kerberized_service=kerberized_service,
                                                 message_serializer=serializer.GraphSONSerializersV2d0())
        else:
            raise ValueError("Invalid authentication option - " + request.param)
    except OSError:
        pytest.skip('Gremlin Server is not running')
    else:
        def fin():
            remote_conn.close()

        request.addfinalizer(fin)
        return remote_conn


@pytest.fixture
def remote_connection_graphsonV2(request):
    try:
        remote_conn = DriverRemoteConnection(anonymous_url,
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
    return GraphBinarySerializersV1()
