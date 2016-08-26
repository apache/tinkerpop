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
        assert 6L == g.V().count().toList()[0]
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
        assert 10 == g.V().repeat(both()).times(5)[0:10].count().next()
        assert 1 == g.V().repeat(both()).times(5)[0].count().next()
        assert 0 == g.V().repeat(both()).times(5)[0:0].count().next()
        assert 4 == g.V()[2:].count().next()
        assert 2 == g.V()[:2].count().next()
        # todo: need a traversal metrics deserializer
        g.V().out().profile().next()
        connection.close()

    def test_side_effects(self):
        statics.load_statics(globals())
        connection = DriverRemoteConnection('ws://localhost:8182', 'g')
        #
        g = Graph().traversal().withRemote(connection)
        ###
        t = g.V().hasLabel("project").name.iterate()
        assert 0 == len(t.side_effects.keys())
        try:
            m = t.side_effects["m"]
            raise Exception("Accessing a non-existent key should throw an error")
        except KeyError:
            pass
        ###
        t = g.V().out("created").groupCount("m").by("name")
        results = t.toSet()
        assert 2 == len(results)
        assert Vertex(3) in results
        assert Vertex(5) in results
        assert 1 == len(t.side_effects.keys())
        assert "m" in t.side_effects.keys()
        m = t.side_effects["m"]
        assert isinstance(m, dict)
        assert 2 == len(m)
        assert 3 == m["lop"]
        assert 1 == m["ripple"]
        assert isinstance(m["lop"], long)
        assert isinstance(m["ripple"], long)
        ###
        t = g.V().out("created").groupCount("m").by("name").name.aggregate("n")
        results = t.toSet()
        assert 2 == len(results)
        assert "lop" in results
        assert "ripple" in results
        assert 2 == len(t.side_effects.keys())
        assert "m" in t.side_effects.keys()
        assert "n" in t.side_effects.keys()
        n = t.side_effects.get("n")
        assert isinstance(n, dict)
        assert 2 == len(n)
        assert "lop" in n.keys()
        assert "ripple" in n.keys()
        assert 3 == n["lop"]
        assert 1 == n["ripple"]
        try:
            x = t.side_effects["x"]
            raise Exception("Accessing a non-existent key should throw an error")
        except KeyError:
            pass


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
