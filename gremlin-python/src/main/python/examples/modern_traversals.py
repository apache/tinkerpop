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

sys.path.append("..")

from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.strategies import *
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.traversal import T
from gremlin_python.process.traversal import P


def main():
    # This example requires the Modern toy graph to be preloaded upon launching the Gremlin server.
    # For details, see https://tinkerpop.apache.org/docs/current/reference/#gremlin-server-docker-image and use
    # conf/gremlin-server-modern.yaml.
    # if there is a port placeholder in the env var then we are running with docker so set appropriate port 
    server_url = os.getenv('GREMLIN_SERVER_URL', 'ws://localhost:8182/gremlin').format(45940)
    
    # CI uses port 45940 with gmodern binding, local uses 8182 with g binding
    if ':45940' in server_url:
        graph_binding = 'gmodern'  # CI environment
    else:
        graph_binding = 'g'        # Local environment
    
    rc = DriverRemoteConnection(server_url, graph_binding)
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
    # 2. Filter those three edges using the where()-step using the identifier of the vertex returned by other_v() to
    #    ensure it matches on the vertex of concern, which is the one with an identifier of "2".
    # 3. Note that the same traversal will work if there are actual Vertex instances rather than just vertex
    #    identifiers.
    # 4. The vertex with identifier "1" has all outgoing edges, so it would also be acceptable to use the directional
    #    steps of out_e() and in_v() since the schema allows it.
    # 5. There is also no problem with filtering the terminating side of the traversal on multiple vertices, in this
    #    case, vertices with identifiers "2" and "3".
    # 6. There’s no reason why the same pattern of exclusion used for edges with where() can’t work for a vertex
    #    between two vertices.

    rc.close()


if __name__ == "__main__":
    main()
