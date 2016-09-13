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
from gremlin_python.structure.graph import Path


class TestGraph(TestCase):
    def test_graph_objects(self):
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

    def test_path(self):
        path = Path([set(["a", "b"]), set(["c", "b"]), set([])], [1, Vertex(1), "hello"])
        assert "[1, v[1], 'hello']" == str(path)
        assert 1 == path["a"]
        assert Vertex(1) == path["c"]
        assert [1, Vertex(1)] == path["b"]
        assert path[0] == 1
        assert path[1] == Vertex(1)
        assert path[2] == "hello"
        assert 3 == len(path)
        assert "hello" in path
        assert "goodbye" not in path
        assert Vertex(1) in path
        assert Vertex(123) not in path
        #
        try:
            temp = path[3]
            raise Exception("Accessing beyond the list index should throw an index error")
        except IndexError:
            pass
        #
        try:
            temp = path["zz"]
            raise Exception("Accessing nothing should throw a key error")
        except KeyError:
            pass
        #
        try:
            temp = path[1:2]
            raise Exception("Accessing using slices should throw a type error")
        except TypeError:
            pass
        #
        assert path == path
        assert hash(path) == hash(path)
        path2 = Path([set(["a", "b"]), set(["c", "b"]), set([])], [1, Vertex(1), "hello"])
        assert path == path2
        assert hash(path) == hash(path2)
        assert path != Path([set(["a"]), set(["c", "b"]), set([])], [1, Vertex(1), "hello"])
        assert path != Path([set(["a", "b"]), set(["c", "b"]), set([])], [3, Vertex(1), "hello"])


if __name__ == '__main__':
    unittest.main()
