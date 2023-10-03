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

import ssl
from ssl import Purpose

import sys

sys.path.append("..")

# Common imports
from gremlin_python import statics
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.strategies import *
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.traversal import T
from gremlin_python.process.traversal import Order
from gremlin_python.process.traversal import Cardinality
from gremlin_python.process.traversal import Column
from gremlin_python.process.traversal import Direction
from gremlin_python.process.traversal import Operator
from gremlin_python.process.traversal import P
from gremlin_python.process.traversal import TextP
from gremlin_python.process.traversal import Pop
from gremlin_python.process.traversal import Scope
from gremlin_python.process.traversal import Barrier
from gremlin_python.process.traversal import Bindings
from gremlin_python.process.traversal import WithOptions

from gremlin_python.driver.serializer import GraphBinarySerializersV1
from gremlin_python.driver.protocol import GremlinServerWSProtocol
from gremlin_python.driver.aiohttp.transport import AiohttpTransport

# Special type classes for more specific data types
from gremlin_python.statics import long  # Java long
from gremlin_python.statics import timestamp  # Java timestamp
from gremlin_python.statics import SingleByte  # Java byte type
from gremlin_python.statics import SingleChar  # Java char type
from gremlin_python.statics import GremlinType  # Java Class


def main():
    connection_example()
    basic_gremlin_example()
    modern_traversal_example()


def connection_example():
    # connect to a remote server that is compatible with the Gremlin Server protocol. for those who
    # downloaded and are using Gremlin Server directly be sure that it is running locally with:
    #
    # bin/gremlin-server.sh console
    #
    # which starts it in "console" mode with an empty in-memory TinkerGraph ready to go bound to a
    # variable named "g" as referenced in the following line.
    g = traversal().with_remote(DriverRemoteConnection('ws://localhost:8182/gremlin', 'g'))

    # connecting with plain text and Kerberos SASL authentication
    rc = DriverRemoteConnection('ws://localhost:8182/gremlin', 'g', username='stephen', password='password')
    g = traversal().with_remote(rc)
    rc.close()

    rc = DriverRemoteConnection('ws://localhost:8182/gremlin', 'g', kerberized_service='gremlin@hostname.your.org')
    g = traversal().with_remote(rc)
    rc.close()

    # connecting with customized configurations
    rc = DriverRemoteConnection(
        'ws://localhost:8182/gremlin', 'g',
        username="", password="", kerberized_service='',
        message_serializer=GraphBinarySerializersV1(), graphson_reader=None,
        graphson_writer=None, headers=None, session=None,
        enable_user_agent_on_connect=True
    )
    g = traversal().with_remote(rc)
    rc.close()


def basic_gremlin_example():
    g = traversal().with_remote(DriverRemoteConnection('ws://localhost:8182/gremlin', 'g'))

    # basic Gremlin: adding and retrieving data
    v1 = g.add_v('person').property('name', 'marko').next()
    v2 = g.add_v('person').property('name', 'stephen').next()
    v3 = g.add_v('person').property('name', 'vadas').next()

    # be sure to use a terminating step like next() or iterate() so that the traversal "executes"
    # iterate() does not return any data and is used to just generate side-effects (i.e. write data to the database)
    g.V(v1).add_e('knows').to(v2).property('weight', 0.75).iterate()
    g.V(v1).add_e('knows').to(v3).property('weight', 0.75).iterate()

    # retrieve the data from the "marko" vertex
    marko = g.V().has('person', 'name', 'marko').values('name').next()
    print("name: " + marko)

    # find the "marko" vertex and then traverse to the people he "knows" and return their data
    people_marko_knows = g.V().has('person', 'name', 'marko').out('knows').values('name').toList()
    for person in people_marko_knows:
        print("marko knows " + person)


def modern_traversal_example():
    # This example requires the Modern toy graph to be preloaded upon launching the Gremlin server.
    # For details, see https://tinkerpop.apache.org/docs/current/reference/#gremlin-server-docker-image and use
    # conf/gremlin-server-modern.yaml.
    rc = DriverRemoteConnection('ws://localhost:8182/gremlin', 'g')
    g = traversal().with_remote(rc)

    e1 = g.V(1).both_e().to_list()  # (1)
    e2 = g.V(1).both_e().where(__.other_v().has_id(2)).to_list()  # (2)
    v1 = g.V(1).next()
    v2 = g.V(2).next()
    e3 = g.V(v1).both_e().where(__.other_v().is_(v2)).to_list()  # (3)
    e4 = g.V(v1).out_e().where(__.in_v().is_(v2)).to_list()  # (4)
    e5 = g.V(1).out_e().where(__.in_v().has(T.id, P.within(2, 3))).to_list()  # (5)
    e6 = g.V(1).out().where(__.in_().has_id(6)).to_list()  # (6)

    print("1: " + str(e1))
    print("2: " + str(e2))
    print("3: " + str(e3))
    print("4: " + str(e4))
    print("5: " + str(e5))
    print("6: " + str(e6))

    # 1. There are three edges from the vertex with the identifier of "1".
    # 2. Filter those three edges using the where()-step using the identifier of the vertex returned by otherV() to
    #    ensure it matches on the vertex of concern, which is the one with an identifier of "2".
    # 3. Note that the same traversal will work if there are actual Vertex instances rather than just vertex
    #    identifiers.
    # 4. The vertex with identifier "1" has all outgoing edges, so it would also be acceptable to use the directional
    #    steps of outE() and inV() since the schema allows it.
    # 5. There is also no problem with filtering the terminating side of the traversal on multiple vertices, in this
    #    case, vertices with identifiers "2" and "3".
    # 6. There’s no reason why the same pattern of exclusion used for edges with where() can’t work for a vertex
    #    between two vertices.

    rc.close()


if __name__ == "__main__":
    main()
