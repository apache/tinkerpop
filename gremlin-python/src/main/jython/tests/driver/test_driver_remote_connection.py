'''
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
'''

__author__ = 'Marko A. Rodriguez (http://markorodriguez.com)'

import unittest
from unittest import TestCase

from gremlin_python import statics
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.traversal import Traverser
from gremlin_python.structure.graph import Graph
from gremlin_python.structure.graph import Vertex


class TestDriverRemoteConnection(TestCase):
    def test_traversals(self):
        statics.load_statics(globals())
        connection = DriverRemoteConnection('ws://localhost:8182', 'g')
        assert "remoteconnection[ws://localhost:8182,g]" == str(connection)
        #
        g = Graph().traversal().withRemote(connection)
        #
        assert Vertex(1) == g.V(1).next()
        assert 1 == g.V(1).id().next()
        assert Traverser(Vertex(1)) == g.V(1).nextTraverser()
        assert 1 == len(g.V(1).toList())
        assert isinstance(g.V(1).toList(), list)
        #
        results = g.V().repeat(out()).times(2).name.toList()
        assert 2 == len(results)
        assert "lop" in results
        assert "ripple" in results
        #
        connection.close()


if __name__ == '__main__':
    test = False
    try:
        connection = DriverRemoteConnection('ws://localhost:8182', 'g')
        test = True
        connection.close()
    except:
        print "GremlinServer is not running and this test case will not execute: " + __file__

    if test:
        unittest.main()
