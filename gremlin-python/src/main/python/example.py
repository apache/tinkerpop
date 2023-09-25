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

# Common imports
from gremlin_python import statics
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.strategies import *
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.traversal import T
from gremlin_python.process.traversal import Order
from gremlin_python.process.traversal import Cardinality
from gremlin_python.process.traversal import CardinalityValue
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
from gremlin_python.statics import long         # Java long
from gremlin_python.statics import timestamp    # Java timestamp
from gremlin_python.statics import SingleByte   # Java byte type
from gremlin_python.statics import SingleChar   # Java char type
from gremlin_python.statics import GremlinType  # Java Class



import json


to_string = json.dumps


def main():

    # connect to a remote server that is compatible with the Gremlin Server protocol. for those who
    # downloaded and are using Gremlin Server directly be sure that it is running locally with:
    #
    # bin/gremlin-server.sh console
    #
    # which starts it in "console" mode with an empty in-memory TinkerGraph ready to go bound to a
    # variable named "g" as referenced in the following line.
    g = traversal().with_remote(DriverRemoteConnection('ws://localhost:8182/gremlin', 'g'))

    # connecting with plain text and Kerberos SASL authentication
    g = traversal().with_remote(DriverRemoteConnection(
        'ws://localhost:8182/gremlin', 'g', username='stephen', password='password'))

    g = traversal().with_remote(DriverRemoteConnection(
        'ws://localhost:8182/gremlin', 'g', kerberized_service='gremlin@hostname.your.org'))

    # connecting with customized configurations
    g = traversal().with_remote(DriverRemoteConnection(
        'ws://localhost:8182/gremlin', 'g',
        username="", password="", kerberized_service='',
        message_serializer=GraphBinarySerializersV1(), graphson_reader=None,
        graphson_writer=None, headers=None, session=None,
        enable_user_agent_on_connect=True
    ))

    # add some data - be sure to use a terminating step like iterate() so that the traversal
    # "executes". iterate() does not return any data and is used to just generate side-effects
    # (i.e. write data to the database)
    g.add_v('person').property('name', 'marko').as_('m'). \
        add_v('person').property('name', 'vadas').as_('v'). \
        add_e('knows').from_('m').to('v').iterate()

    # retrieve the data from the "marko" vertex
    print("marko: " + to_string(g.V().has('person', 'name', 'marko').value_map().next()))

    # find the "marko" vertex and then traverse to the people he "knows" and return their data
    print("who marko knows: " + to_string(g.V().has('person', 'name', 'marko').out('knows').value_map().next()))


if __name__ == "__main__":
    main()
