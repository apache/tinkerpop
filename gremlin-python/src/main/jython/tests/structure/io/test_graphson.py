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

import json
from mock import Mock
import unittest
from unittest import TestCase

import six

from gremlin_python.statics import *
from gremlin_python.structure.graph import Vertex, VertexProperty, Edge, Property
from gremlin_python.structure.graph import Path
from gremlin_python.structure.io.graphson import GraphSONWriter, GraphSONReader, GraphSONUtil
import gremlin_python.structure.io.graphson
from gremlin_python.process.traversal import P
from gremlin_python.process.strategies import SubgraphStrategy
from gremlin_python.process.graph_traversal import __


class TestGraphSONReader(TestCase):
    graphson_reader = GraphSONReader()

    def test_number_input(self):
        x = self.graphson_reader.readObject(json.dumps({
            "@type": "g:Int32",
            "@value": 31
        }))
        assert isinstance(x, int)
        assert 31 == x
        ##
        x = self.graphson_reader.readObject(json.dumps({
            "@type": "g:Int64",
            "@value": 31
        }))
        assert isinstance(x, long)
        assert long(31) == x
        ##
        x = self.graphson_reader.readObject(json.dumps({
            "@type": "g:Float",
            "@value": 31.3
        }))
        assert isinstance(x, float)
        assert 31.3 == x
        ##
        x = self.graphson_reader.readObject(json.dumps({
            "@type": "g:Double",
            "@value": 31.2
        }))
        assert isinstance(x, float)
        assert 31.2 == x

    def test_graph(self):
        vertex = self.graphson_reader.readObject(
                """{"@type":"g:Vertex", "@value":{"id":{"@type":"g:Int32","@value":1},"label":"person","outE":{"created":[{"id":{"@type":"g:Int32","@value":9},"inV":{"@type":"g:Int32","@value":3},"properties":{"weight":{"@type":"g:Double","@value":0.4}}}],"knows":[{"id":{"@type":"g:Int32","@value":7},"inV":{"@type":"g:Int32","@value":2},"properties":{"weight":{"@type":"g:Double","@value":0.5}}},{"id":{"@type":"g:Int32","@value":8},"inV":{"@type":"g:Int32","@value":4},"properties":{"weight":{"@type":"g:Double","@value":1.0}}}]},"properties":{"name":[{"id":{"@type":"g:Int64","@value":0},"value":"marko"}],"age":[{"id":{"@type":"g:Int64","@value":1},"value":{"@type":"g:Int32","@value":29}}]}}}""")
        assert isinstance(vertex, Vertex)
        assert "person" == vertex.label
        assert 1 == vertex.id
        assert isinstance(vertex.id, int)
        assert vertex == Vertex(1)
        assert isinstance(vertex.properties, dict)
        assert "name" in vertex.properties
        assert isinstance(vertex.properties["name"], list)
        assert isinstance(vertex.properties["name"][0], dict)
        assert vertex.properties["name"][0]["value"] == "marko"

    def test_vertex(self):
        vertex = self.graphson_reader.readObject(
                """{"@type": "g:Vertex", "@value": {"properties": {"name": [{"@type": "g:VertexProperty", "@value": {"id": {"@type": "g:Int64", "@value": 0}, "value": "marko", "label": "name"}}], "location": [{"@type": "g:VertexProperty", "@value": {"properties": {"endTime": {"@type": "g:Int32", "@value": 2001}, "startTime": {"@type": "g:Int32", "@value": 1997}}, "id": {"@type": "g:Int64", "@value": 12}, "value": "san diego", "label": "location"}}, {"@type": "g:VertexProperty", "@value": {"properties": {"endTime": {"@type": "g:Int32", "@value": 2004}, "startTime": {"@type": "g:Int32", "@value": 2001}}, "id": {"@type": "g:Int64", "@value": 13}, "value": "santa cruz", "label": "location"}}, {"@type": "g:VertexProperty", "@value": {"properties": {"endTime": {"@type": "g:Int32", "@value": 2005}, "startTime": {"@type": "g:Int32", "@value": 2004}}, "id": {"@type": "g:Int64", "@value": 14}, "value": "brussels", "label": "location"}}, {"@type": "g:VertexProperty", "@value": {"properties": {"startTime": {"@type": "g:Int32", "@value": 2005}}, "id": {"@type": "g:Int64", "@value": 15}, "value": "santa fe", "label": "location"}}]}, "id": {"@type": "g:Int64", "@value": 1}, "label": "person"}}"""
                )
        assert isinstance(vertex, Vertex)
        assert "person" == vertex.label
        assert 1 == vertex.id
        assert isinstance(vertex.id, long)
        assert vertex == Vertex(1)
        assert isinstance(vertex.properties, dict)
        assert isinstance(vertex.properties["location"], list)
        assert 4 == len(vertex.properties["location"])
        assert isinstance(vertex.properties["location"][0], VertexProperty)

    def test_vertexProperty(self):
        vp = self.graphson_reader.readObject(
                """{"@type": "g:VertexProperty", "@value": {"properties": {"endTime": {"@type": "g:Int32", "@value": 2001}, "startTime": {"@type": "g:Int32", "@value": 1997}}, "id": {"@type": "g:Int64", "@value": 12}, "value": "san diego", "label": "location"}}"""
                )
        assert isinstance(vp, VertexProperty)
        assert "location" == vp.key
        assert "location" == vp.label
        assert 12 == vp.id
        assert isinstance(vp.properties, dict)
        assert "endTime" in vp.properties
        assert 2001 == vp.properties["endTime"]

    def test_edgeProperties(self):
        edge = self.graphson_reader.readObject(
                """{"@type": "g:Edge", "@value": {"properties": {"skill": {"@type": "g:Property", "@value": {"value": {"@type": "g:Int32", "@value": 5}, "key": "skill"}}}, "outV": {"@type": "g:Int64", "@value": 1}, "inVLabel": "software", "inV": {"@type": "g:Int64", "@value": 11}, "outVLabel": "person", "label": "uses", "id": {"@type": "g:Int32", "@value": 16}}}"""
                )
        assert isinstance(edge, Edge)
        assert "uses" == edge.label
        assert 1 == edge.outV.id
        assert 11 == edge.inV.id
        assert isinstance(edge.properties, dict)
        assert "skill" in edge.properties
        assert isinstance(edge.properties["skill"], Property)
        assert 5 == edge.properties["skill"].value

    def test_path(self):
        path = self.graphson_reader.readObject(
            """{"@type":"g:Path","@value":{"labels":[["a"],["b","c"],[]],"objects":[{"@type":"g:Vertex","@value":{"id":{"@type":"g:Int32","@value":1},"label":"person","properties":{"name":[{"@type":"g:VertexProperty","@value":{"id":{"@type":"g:Int64","@value":0},"value":"marko","label":"name"}}],"age":[{"@type":"g:VertexProperty","@value":{"id":{"@type":"g:Int64","@value":1},"value":{"@type":"g:Int32","@value":29},"label":"age"}}]}}},{"@type":"g:Vertex","@value":{"id":{"@type":"g:Int32","@value":3},"label":"software","properties":{"name":[{"@type":"g:VertexProperty","@value":{"id":{"@type":"g:Int64","@value":4},"value":"lop","label":"name"}}],"lang":[{"@type":"g:VertexProperty","@value":{"id":{"@type":"g:Int64","@value":5},"value":"java","label":"lang"}}]}}},"lop"]}}"""
        )
        assert isinstance(path, Path)
        if six.PY3:
            assert "[v[1], v[3], 'lop']" == str(path)
        else:
            assert "[v[1], v[3], u'lop']" == str(path)
        assert Vertex(1) == path[0]
        assert Vertex(1) == path["a"]
        assert "lop" == path[2]
        assert 3 == len(path)

    def test_custom_mapping(self):

        # extended mapping
        class X(object):
            pass

        type_string = "test:Xtype"
        override_string = "g:Int64"
        serdes = Mock()

        reader = GraphSONReader(deserializer_map={type_string: serdes})
        assert type_string in reader.deserializers

        # base dicts are not modified
        assert type_string not in gremlin_python.structure.io.graphson._deserializers

        x = X()
        o = reader.toObject({GraphSONUtil.TYPE_KEY: type_string, GraphSONUtil.VALUE_KEY: x})
        serdes.objectify.assert_called_once_with(x, reader)
        assert o is serdes.objectify()

        # overridden mapping
        type_string = "g:Int64"
        serdes = Mock()
        reader = GraphSONReader(deserializer_map={type_string: serdes, override_string: serdes})
        assert gremlin_python.structure.io.graphson._deserializers[type_string] is not reader.deserializers[type_string]

        value = 3
        o = reader.toObject({GraphSONUtil.TYPE_KEY: type_string, GraphSONUtil.VALUE_KEY: value})
        serdes.objectify.assert_called_once_with(value, reader)
        assert o is serdes.objectify()


class TestGraphSONWriter(TestCase):

    graphson_writer = GraphSONWriter()

    def test_number_output(self):
        assert {"@type":"g:Int64","@value":2} == json.loads(self.graphson_writer.writeObject(long(2)))
        assert {"@type":"g:Int32","@value":1} == json.loads(self.graphson_writer.writeObject(1))
        assert {"@type":"g:Double","@value":3.2} == json.loads(self.graphson_writer.writeObject(3.2))
        assert """true""" == self.graphson_writer.writeObject(True)

    def test_numbers(self):
        assert {"@type": "g:Int64", "@value": 2} == json.loads(self.graphson_writer.writeObject(long(2)))
        assert {"@type": "g:Int32", "@value": 1} == json.loads(self.graphson_writer.writeObject(1))
        assert {"@type": "g:Double", "@value": 3.2} == json.loads(self.graphson_writer.writeObject(3.2))
        assert """true""" == self.graphson_writer.writeObject(True)

    def test_P(self):
        assert """{"@type":"g:P","@value":{"predicate":"and","value":[{"@type":"g:P","@value":{"predicate":"or","value":[{"@type":"g:P","@value":{"predicate":"lt","value":"b"}},{"@type":"g:P","@value":{"predicate":"gt","value":"c"}}]}},{"@type":"g:P","@value":{"predicate":"neq","value":"d"}}]}}""" == self.graphson_writer.writeObject(
            P.lt("b").or_(P.gt("c")).and_(P.neq("d")))

    def test_strategies(self):
        # we have a proxy model for now given that we don't want to have to have g:XXX all registered on the Gremlin traversal machine (yet)
        assert {"@type": "g:SubgraphStrategy", "@value": {}} == json.loads(self.graphson_writer.writeObject(SubgraphStrategy))
        assert {"@type": "g:SubgraphStrategy", "@value": {
            "vertices": {"@type": "g:Bytecode", "@value": {"step": [["has", "name", "marko"]]}}}} == json.loads(
            self.graphson_writer.writeObject(SubgraphStrategy(vertices=__.has("name", "marko"))))

    def test_custom_mapping(self):

        # extended mapping
        class X(object):
            pass

        serdes = Mock()
        writer = GraphSONWriter(serializer_map={X: serdes})
        assert X in writer.serializers

        # base dicts are not modified
        assert X not in gremlin_python.structure.io.graphson._serializers

        obj = X()
        d = writer.toDict(obj)
        serdes.dictify.assert_called_once_with(obj, writer)
        assert d is serdes.dictify()

        # overridden mapping
        serdes = Mock()
        writer = GraphSONWriter(serializer_map={int: serdes})
        assert gremlin_python.structure.io.graphson._serializers[int] is not writer.serializers[int]

        value = 3
        d = writer.toDict(value)
        serdes.dictify.assert_called_once_with(value, writer)
        assert d is serdes.dictify()


if __name__ == '__main__':
    unittest.main()
