# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import sys
import os
import ssl
import socket

sys.path.append("..")

from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.strategies import *
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.driver.serializer import GraphBinarySerializersV1
from gremlin_python.driver.aiohttp.transport import AiohttpTransport

VERTEX_LABEL = os.getenv('VERTEX_LABEL', 'connection')

def main():
    with_remote()
    with_auth()
    with_kerberos()
    with_configs()


def with_remote():
    # connect to a remote server that is compatible with the Gremlin Server protocol. for those who
    # downloaded and are using Gremlin Server directly be sure that it is running locally with:
    #
    # bin/gremlin-server.sh console
    #
    # which starts it in "console" mode with an empty in-memory TinkerGraph ready to go bound to a
    # variable named "g" as referenced in the following line.
    # if there is a port placeholder in the env var then we are running with docker so set appropriate port 
    server_url = os.getenv('GREMLIN_SERVER_URL', 'ws://localhost:8182/gremlin').format(45940)
    rc = DriverRemoteConnection(server_url, 'g')
    g = traversal().with_remote(rc)

    # simple query to verify connection
    v = g.add_v(VERTEX_LABEL).iterate()
    count = g.V().has_label(VERTEX_LABEL).count().next()
    print("Vertex count: " + str(count))

    # cleanup
    rc.close()


# connecting with plain text authentication
def with_auth():
    # if there is a port placeholder in the env var then we are running with docker so set appropriate port 
    server_url = os.getenv('GREMLIN_SERVER_BASIC_AUTH_URL', 'ws://localhost:8182/gremlin').format(45941)
    
    # disable SSL certificate verification for CI environments
    if ':45941' in server_url:
        ssl_opts = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_opts.check_hostname = False
        ssl_opts.verify_mode = ssl.CERT_NONE
        rc = DriverRemoteConnection(server_url, 'g', username='stephen', password='password',
                                    transport_factory=lambda: AiohttpTransport(ssl_options=ssl_opts))
    else:
        rc = DriverRemoteConnection(server_url, 'g', username='stephen', password='password')
    
    g = traversal().with_remote(rc)

    v = g.add_v(VERTEX_LABEL).iterate()
    count = g.V().has_label(VERTEX_LABEL).count().next()
    print("Vertex count: " + str(count))

    rc.close()


# connecting with Kerberos SASL authentication
def with_kerberos():
    # if there is a port placeholder in the env var then we are running with docker so set appropriate port 
    server_url = os.getenv('GREMLIN_SERVER_URL', 'ws://localhost:8182/gremlin').format(45942)
    kerberos_hostname = os.getenv('KRB_HOSTNAME', socket.gethostname())
    kerberized_service = f'test-service@{kerberos_hostname}'
    
    rc = DriverRemoteConnection(server_url, 'g', kerberized_service=kerberized_service)
    g = traversal().with_remote(rc)

    v = g.add_v(VERTEX_LABEL).iterate()
    count = g.V().has_label(VERTEX_LABEL).count().next()
    print("Vertex count: " + str(count))

    rc.close()


# connecting with customized configurations
def with_configs():
    # if there is a port placeholder in the env var then we are running with docker so set appropriate port 
    server_url = os.getenv('GREMLIN_SERVER_URL', 'ws://localhost:8182/gremlin').format(45940)
    rc = DriverRemoteConnection(
        server_url, 'g',
        username="", password="", kerberized_service='',
        message_serializer=GraphBinarySerializersV1(), graphson_reader=None,
        graphson_writer=None, headers=None, session=None,
        enable_user_agent_on_connect=True
    )
    g = traversal().with_remote(rc)

    v = g.add_v(VERTEX_LABEL).iterate()
    count = g.V().has_label(VERTEX_LABEL).count().next()
    print("Vertex count: " + str(count))

    rc.close()


if __name__ == "__main__":
    main()
