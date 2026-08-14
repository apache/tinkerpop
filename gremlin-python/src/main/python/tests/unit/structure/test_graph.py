#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

__author__ = 'Marko A. Rodriguez (http://markorodriguez.com)'

import pytest

from gremlin_python.statics import long
from gremlin_python.structure.graph import Edge
from gremlin_python.structure.graph import Graph
from gremlin_python.structure.graph import Property
from gremlin_python.structure.graph import Vertex
from gremlin_python.structure.graph import VertexProperty
from gremlin_python.structure.graph import Path
from gremlin_python.structure.graph import Tree
from gremlin_python.structure.graph import CompositePDT
from gremlin_python.structure.graph import PrimitivePDT
from gremlin_python.structure.graph import PDTRegistry


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

    def test_graph_repr(self):
        # empty graph
        g = Graph()
        assert "graph[vertices:0 edges:0]" == repr(g)

        # graph with two vertices and one edge
        v1 = Vertex(1, "person")
        v2 = Vertex(2, "person")
        g.vertices[1] = v1
        g.vertices[2] = v2
        g.edges[3] = Edge(3, v1, "knows", v2)
        assert "graph[vertices:2 edges:1]" == repr(g)

    def test_vertex_property_map(self):
        v = Vertex(1, "person")
        # multi-properties: two VertexProperty entries with the same key group together
        name1 = VertexProperty(long(1), "name", "marko", v)
        name2 = VertexProperty(long(2), "name", "marko a. rodriguez", v)
        age = VertexProperty(long(3), "age", 29, v)
        v.properties = [name1, name2, age]
        pm = v.property_map()
        assert set(pm.keys()) == {"name", "age"}
        assert pm["name"] == [name1, name2]
        assert len(pm["name"]) == 2
        assert pm["age"] == [age]
        # single-valued keys still map to 1-element lists
        assert len(pm["age"]) == 1
        #
        # a vertex with no properties yields an empty dict
        assert Vertex(2).property_map() == {}

    def test_edge_property_map(self):
        e = Edge(20, Vertex(10), "knows", Vertex(11))
        # Edge holds Property objects, each exposing .key
        weight = Property("weight", 0.5, e)
        since = Property("since", 2006, e)
        e.properties = [weight, since]
        pm = e.property_map()
        assert set(pm.keys()) == {"weight", "since"}
        # single-valued keys map to 1-element lists
        assert pm["weight"] == [weight]
        assert len(pm["weight"]) == 1
        assert pm["since"] == [since]
        assert len(pm["since"]) == 1
        #
        # an edge with no properties yields an empty dict
        assert Edge(21, Vertex(10), "knows", Vertex(11)).property_map() == {}

    def test_vertex_property_property_map(self):
        vp = VertexProperty(long(30), "name", "marko", Vertex(10))
        # VertexProperty holds meta-properties as Property objects exposing .key
        since = Property("since", 2006, vp)
        skill = Property("skill", 4, vp)
        vp.properties = [since, skill]
        pm = vp.property_map()
        assert set(pm.keys()) == {"since", "skill"}
        # single-valued keys map to 1-element lists
        assert pm["since"] == [since]
        assert len(pm["since"]) == 1
        assert pm["skill"] == [skill]
        assert len(pm["skill"]) == 1
        #
        # a vertex property with no meta-properties yields an empty dict
        assert VertexProperty(long(31), "name", "marko", Vertex(10)).property_map() == {}

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

    def test_element_value_values(self):
        v = Vertex(1, "person", [VertexProperty(10, "name", "marko", Vertex(1)),
                                 VertexProperty(11, "age", 29, Vertex(1))])
        assert v["name"] == "marko"
        assert v["age"] == 29
        try:
            x = v["nonexistent"]
            assert False, "Should have thrown KeyError"
        except KeyError:
            pass

        assert v.values("name") == ["marko"]
        assert v.values("age") == [29]
        assert "marko" in v.values()
        assert 29 in v.values()
        assert len(v.values()) == 2
        assert v.values("name", "age") == ["marko", 29]
        assert v.values("nonexistent") == []

        e = Edge(2, Vertex(1), "knows", Vertex(3), [Property("weight", 0.5, None)])
        assert e["weight"] == 0.5
        assert e.values("weight") == [0.5]
        assert e.values() == [0.5]

        vp = VertexProperty(10, "name", "marko", Vertex(1), [Property("acl", "public", None)])
        assert vp["acl"] == "public"
        assert vp.values("acl") == ["public"]
        assert vp.values() == ["public"]

    def test_element_contains_and_keys(self):
        v = Vertex(1, "person", [VertexProperty(10, "name", "marko", Vertex(1)),
                                 VertexProperty(11, "age", 29, Vertex(1))])
        assert "name" in v
        assert "age" in v
        assert "nonexistent" not in v
        assert v.keys() == {"name", "age"}

        e = Edge(2, Vertex(1), "knows", Vertex(3), [Property("weight", 0.5, None)])
        assert "weight" in e
        assert "missing" not in e
        assert e.keys() == {"weight"}

        empty_v = Vertex(99)
        assert "anything" not in empty_v
        assert empty_v.keys() == set()

        # supports the pattern: vertex[key] if key in vertex else None
        assert v["name"] if "name" in v else None == "marko"
        assert v["missing"] if "missing" in v else None is None


class TestEdgeLabels(object):
    def test_edge_labels_property_is_frozenset(self):
        e = Edge(1, Vertex(2), "knows", Vertex(3), labels=["knows", "likes"])
        assert e.labels == frozenset({"knows", "likes"})
        assert isinstance(e.labels, frozenset)
        # label (singular) is derived from the first of the provided labels
        assert e.label in {"knows", "likes"}


class TestCompositePDT(object):
    def test_equal_composites_hash_equal(self):
        a = CompositePDT("point", {"x": 1, "y": 2})
        b = CompositePDT("point", {"x": 1, "y": 2})
        assert a == b
        assert hash(a) == hash(b)

    def test_hash_with_unhashable_field_falls_back_to_name(self):
        # frozenset(items) raises TypeError when a field value is unhashable
        # (e.g. a list); __hash__ then falls back to hash(name) without raising.
        pdt = CompositePDT("thing", {"vals": [1, 2, 3]})
        assert hash(pdt) == hash("thing")

    def test_repr_format(self):
        pdt = CompositePDT("point", {"x": 1})
        assert repr(pdt) == "pdt[point]{'x': 1}"


class TestPDTRegistryRegisterPrimitive(object):
    def test_register_with_target_class_populates_by_class_map(self):
        class MyType(object):
            pass

        reg = PDTRegistry()
        reg.register_primitive("myType", from_value=lambda s: MyType(),
                                to_value=str, target_class=MyType)
        adapter = reg.get_primitive_adapter_by_class(MyType)
        assert adapter is not None
        assert adapter["type_name"] == "myType"
        assert adapter["to_value"] is str

    def test_register_without_target_class_leaves_by_class_map_empty(self):
        class MyType(object):
            pass

        reg = PDTRegistry()
        reg.register_primitive("myType", from_value=lambda s: MyType())
        assert reg.get_primitive_adapter_by_class(MyType) is None


class TestPDTRegistryHydratePrimitive(object):
    def test_non_primitive_input_returned_unchanged(self):
        reg = PDTRegistry()
        composite = CompositePDT("point", {"x": 1})
        assert reg.hydrate_primitive(composite) is composite


class TestTree(object):
    def test_init_rejects_non_tree_child(self):
        with pytest.raises(TypeError):
            Tree([("k", "not-a-tree")])

    def test_eq_against_non_tree(self):
        t = Tree([("a", Tree())])
        # __eq__ returns NotImplemented for non-Tree; Python resolves to False
        assert (t == "not-a-tree") is False
        assert (t != "not-a-tree") is True

    def test_eq_same_size_different_root_key(self):
        t1 = Tree([("a", Tree())])
        t2 = Tree([("b", Tree())])
        # equal number of root entries but differing keys -> not equal
        assert len(t1.root_nodes()) == len(t2.root_nodes())
        assert t1 != t2

    def test_repr_format(self):
        assert repr(Tree()) == "{}"
        assert repr(Tree([("a", Tree())])) == "{a={}}"
        assert repr(Tree([("a", Tree([("b", Tree())]))])) == "{a={b={}}}"
