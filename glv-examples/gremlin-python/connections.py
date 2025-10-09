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

sys.path.append("..")

from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.strategies import *
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.driver.serializer import GraphBinarySerializersV1


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
    rc = DriverRemoteConnection('ws://localhost:8182/gremlin', 'g')
    g = traversal().with_remote(rc)

    # drop existing vertices
    g.V().drop().iterate()

    # simple query to verify connection
    v = g.add_v("connection").iterate()
    count = g.V().has_label("connection").count().next()
    print("Vertex count: " + str(count))

    # cleanup
    rc.close()


# connecting with plain text authentication
def with_auth():
    rc = DriverRemoteConnection('ws://localhost:8182/gremlin', 'g', username='stephen', password='password')
    g = traversal().with_remote(rc)

    v = g.add_v("connection").iterate()
    count = g.V().has_label("connection").count().next()
    print("Vertex count: " + str(count))

    rc.close()


# connecting with Kerberos SASL authentication
def with_kerberos():
    rc = DriverRemoteConnection('ws://localhost:8182/gremlin', 'g', kerberized_service='gremlin@hostname.your.org')
    g = traversal().with_remote(rc)

    v = g.add_v("connection").iterate()
    count = g.V().has_label("connection").count().next()
    print("Vertex count: " + str(count))

    rc.close()


# connecting with customized configurations
def with_configs():
    rc = DriverRemoteConnection(
        'ws://localhost:8182/gremlin', 'g',
        username="", password="", kerberized_service='',
        message_serializer=GraphBinarySerializersV1(), graphson_reader=None,
        graphson_writer=None, headers=None, session=None,
        enable_user_agent_on_connect=True
    )
    g = traversal().with_remote(rc)

    v = g.add_v("connection").iterate()
    count = g.V().has_label("connection").count().next()
    print("Vertex count: " + str(count))

    rc.close()


if __name__ == "__main__":
    main()
