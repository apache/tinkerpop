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
from gremlin_python.process.strategies import *
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection

VERTEX_LABEL = os.getenv('VERTEX_LABEL', 'person')

def main():
    # if there is a port placeholder in the env var then we are running with docker so set appropriate port 
    server_url = os.getenv('GREMLIN_SERVER_URL', 'ws://localhost:8182/gremlin').format(45940)
    rc = DriverRemoteConnection(server_url, 'g')
    g = traversal().with_remote(rc)

    # basic Gremlin: adding and retrieving data
    v1 = g.add_v(VERTEX_LABEL).property('name', 'marko').next()
    v2 = g.add_v(VERTEX_LABEL).property('name', 'stephen').next()
    v3 = g.add_v(VERTEX_LABEL).property('name', 'vadas').next()

    # be sure to use a terminating step like next() or iterate() so that the traversal "executes"
    # iterate() does not return any data and is used to just generate side-effects (i.e. write data to the database)
    g.V(v1).add_e('knows').to(v2).property('weight', 0.75).iterate()
    g.V(v1).add_e('knows').to(v3).property('weight', 0.75).iterate()

    # retrieve the data from the "marko" vertex
    marko = g.V().has(VERTEX_LABEL, 'name', 'marko').values('name').next()
    print("name: " + marko)

    # find the "marko" vertex and then traverse to the people he "knows" and return their data
    people_marko_knows = g.V().has(VERTEX_LABEL, 'name', 'marko').out('knows').values('name').to_list()
    for person in people_marko_knows:
        print("marko knows " + person)

    rc.close()


if __name__ == "__main__":
    main()
