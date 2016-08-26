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

from gremlin_python.structure.graph import Edge
from gremlin_python.structure.graph import Property
from gremlin_python.structure.graph import Vertex
from gremlin_python.structure.graph import VertexProperty


class TestGraph(TestCase):
    def testGraphObjects(self):
        vertex = Vertex(1)
        assert "v[1]" == str(vertex)
        assert "vertex" == vertex.label
        assert "person" == Vertex(1, "person").label
        assert vertex == Vertex(1)
        #
        edge = Edge(2, Vertex(1), "said", Vertex("hello", "phrase"))
        assert "e[2][1-said->hello]" == str(edge)
        assert Vertex(1) == edge.outV
        assert Vertex("hello") == edge.inV
        assert "said" == edge.label
        assert "phrase" == edge.inV.label
        assert edge.inV != edge.outV
        #
        vertex_property = VertexProperty(24L, "name", "marko")
        assert "vp[name->marko]" == str(vertex_property)
        assert "name" == vertex_property.label
        assert "name" == vertex_property.key
        assert "marko" == vertex_property.value
        assert 24L == vertex_property.id
        assert isinstance(vertex_property.id, long)
        assert vertex_property == VertexProperty(24L, "name", "marko")
        #
        property = Property("age", 29)
        assert "p[age->29]" == str(property)
        assert "age" == property.key
        assert 29 == property.value
        assert isinstance(property.value, int)
        assert property == Property("age", 29)
        assert property != Property("age", 29L)
        #
        for i in [vertex, edge, vertex_property, property]:
            for j in [vertex, edge, vertex_property, property]:
                if type(i) != type(j):
                    assert i != j
                else:
                    assert i == j
                    assert i.__hash__() == hash(i)


if __name__ == '__main__':
    unittest.main()
