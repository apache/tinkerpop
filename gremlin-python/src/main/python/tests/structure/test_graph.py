#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

__author__ = 'Marko A. Rodriguez (http://markorodriguez.com)'

from gremlin_python.statics import long
from gremlin_python.structure.graph import Edge
from gremlin_python.structure.graph import Property
from gremlin_python.structure.graph import Vertex
from gremlin_python.structure.graph import VertexProperty
from gremlin_python.structure.graph import Path


class TestGraph(object):
    def test_graph_objects(self):
        vertex = Vertex(1)
        assert "v[1]" == str(vertex)
        assert "vertex" == vertex.label
        assert "person" == Vertex(1, "person").label
        assert vertex == Vertex(1)
        # properties default to empty list when not provided
        assert vertex.properties == []
        #
        edge = Edge(2, Vertex(1), "said", Vertex("hello", "phrase"))
        assert "e[2][1-said->hello]" == str(edge)
        assert Vertex(1) == edge.outV
        assert Vertex("hello") == edge.inV
        assert "said" == edge.label
        assert "phrase" == edge.inV.label
        assert edge.inV != edge.outV
        # properties default to empty list when not provided
        assert edge.properties == []
        #
        vertex_property = VertexProperty(long(24), "name", "marko", Vertex(1))
        assert "vp[name->marko]" == str(vertex_property)
        assert "name" == vertex_property.label
        assert "name" == vertex_property.key
        assert "marko" == vertex_property.value
        assert long(24) == vertex_property.id
        assert Vertex(1) == vertex_property.vertex
        assert isinstance(vertex_property.id, long)
        assert vertex_property == VertexProperty(long(24), "name", "marko", Vertex(1))
        # meta-properties default to empty list when not provided
        assert vertex_property.properties == []
        #
        property = Property("age", 29, Vertex(1))
        assert "p[age->29]" == str(property)
        assert "age" == property.key
        assert 29 == property.value
        assert Vertex(1) == property.element
        assert isinstance(property.value, int)
        assert property == Property("age", 29, Vertex(1))
        #
        # Now create elements with properties explicitly set
        v2 = Vertex(10, "person", [VertexProperty(100, "name", "marko", Vertex(10))])
        assert len(v2.properties) == 1
        assert isinstance(v2.properties[0], VertexProperty)
        assert v2.properties[0].label == "name"
        assert v2.properties[0].value == "marko"
        e2 = Edge(20, Vertex(10), "knows", Vertex(11), [Property("weight", 0.5, None)])
        assert len(e2.properties) == 1
        assert isinstance(e2.properties[0], Property)
        assert e2.properties[0].key == "weight"
        assert e2.properties[0].value == 0.5
        vp2 = VertexProperty(30, "name", "marko", Vertex(10), [Property("since", 2006, None)])
        assert len(vp2.properties) == 1
        assert isinstance(vp2.properties[0], Property)
        assert vp2.properties[0].key == "since"
        assert vp2.properties[0].value == 2006
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
        assert "path[1, v[1], hello]" == str(path)
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
