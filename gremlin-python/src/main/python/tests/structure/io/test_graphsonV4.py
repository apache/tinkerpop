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

import datetime
import calendar
import json
import uuid
import math
from decimal import *

from unittest.mock import Mock

from gremlin_python.statics import *
from gremlin_python.structure.graph import Vertex, Edge, Property, VertexProperty, Path
from gremlin_python.structure.io.graphsonV4 import GraphSONWriter, GraphSONReader, GraphSONUtil
import gremlin_python.structure.io.graphsonV4
from gremlin_python.process.traversal import P, Merge, Barrier, Order, Operator, Direction
from gremlin_python.process.strategies import SubgraphStrategy
from gremlin_python.process.graph_traversal import __


class TestGraphSONReader:
    graphson_reader = GraphSONReader()

    def test_collections(self):
        x = self.graphson_reader.read_object(
            json.dumps({"@type": "g:List", "@value": [{"@type": "g:Int32", "@value": 1},
                                                      {"@type": "g:Int32", "@value": 2},
                                                      "3"]}))
        assert isinstance(x, list)
        assert x[0] == 1
        assert x[1] == 2
        assert x[2] == "3"
        ##

        x = self.graphson_reader.read_object(
            json.dumps({"@type": "g:Set", "@value": [{"@type": "g:Int32", "@value": 1},
                                                     {"@type": "g:Int32", "@value": 2},
                                                     "3"]}))
        # return a set as normal
        assert isinstance(x, set)
        assert x == {1, 2, "3"}

        x = self.graphson_reader.read_object(
            json.dumps({"@type": "g:Set", "@value": [{"@type": "g:Int32", "@value": 1},
                                                    {"@type": "g:Int32", "@value": 2},
                                                    {"@type": "g:Float", "@value": 2.0},
                                                    "3"]}))
        # coerce to list here because Java might return numerics of different types which python won't recognize
        # see comments of TINKERPOP-1844 for more details
        assert isinstance(x, list)
        assert x == list([1, 2, 2.0, "3"])
        ##
        x = self.graphson_reader.read_object(
            json.dumps({"@type": "g:Map",
                        "@value": ['a', {"@type": "g:Int32", "@value": 1}, 'b', "marko"]}))
        assert isinstance(x, dict)
        assert x['a'] == 1
        assert x['b'] == "marko"
        assert len(x) == 2

    def test_number_input(self):
        x = self.graphson_reader.read_object(json.dumps({
            "@type": "g:Byte",
            "@value": 1
        }))
        assert isinstance(x, SingleByte)
        assert 1 == x
        x = self.graphson_reader.read_object(json.dumps({
            "@type": "g:Int16",
            "@value": 16
        }))
        assert isinstance(x, short)
        assert 16 == x
        x = self.graphson_reader.read_object(json.dumps({
            "@type": "g:Int32",
            "@value": 31
        }))
        assert isinstance(x, int)
        assert 31 == x
        ##
        x = self.graphson_reader.read_object(json.dumps({
            "@type": "g:Int64",
            "@value": 31
        }))
        assert isinstance(x, long)
        assert long(31) == x
        ##
        x = self.graphson_reader.read_object(json.dumps({
            "@type": "g:Float",
            "@value": 31.3
        }))
        assert isinstance(x, float)
        assert 31.3 == x
        ##
        x = self.graphson_reader.read_object(json.dumps({
            "@type": "g:Double",
            "@value": 31.2
        }))
        assert isinstance(x, float)
        assert 31.2 == x
        ##
        x = self.graphson_reader.read_object(json.dumps({
            "@type": "g:Double",
            "@value": "NaN"
        }))
        assert isinstance(x, float)
        assert math.isnan(x)
        ##
        x = self.graphson_reader.read_object(json.dumps({
            "@type": "g:Double",
            "@value": "Infinity"
        }))
        assert isinstance(x, float)
        assert math.isinf(x) and x > 0
        ##
        x = self.graphson_reader.read_object(json.dumps({
            "@type": "g:Double",
            "@value": "-Infinity"
        }))
        assert isinstance(x, float)
        assert math.isinf(x) and x < 0
        ##
        x = self.graphson_reader.read_object(json.dumps({
            "@type": "g:BigDecimal",
            "@value": 31.2
        }))
        assert isinstance(x, BigDecimal)
        assert Decimal('31.2') == x.value
        ##
        x = self.graphson_reader.read_object(json.dumps({
            "@type": "g:BigDecimal",
            "@value": 123456789987654321123456789987654321
        }))
        assert isinstance(x, BigDecimal)
        assert to_bigdecimal(123456789987654321123456789987654321).value == x.value
        ##
        x = self.graphson_reader.read_object(json.dumps({
            "@type": "g:BigInteger",
            "@value": 31
        }))
        assert isinstance(x, long)
        assert 31 == x
        ##
        x = self.graphson_reader.read_object(json.dumps({
            "@type": "g:BigInteger",
            "@value": 123456789987654321123456789987654321
        }))
        assert isinstance(x, long)
        assert 123456789987654321123456789987654321 == x

    def test_graph(self):
        vertex = self.graphson_reader.read_object("""
        {"@type":"g:Vertex", "@value":{"id":{"@type":"g:Int32","@value":1},"label": ["person"],"outE":{"created":[{"id":{"@type":"g:Int32","@value":9},"inV":{"@type":"g:Int32","@value":3},"properties":{"weight":[{"@type":"g:Property","@value":{"key":"weight","value":{"@type":"g:Double","@value":0.4}}}]}}],"knows":[{"id":{"@type":"g:Int32","@value":7},"inV":{"@type":"g:Int32","@value":2},"properties":{"weight":[{"@type":"g:Property","@value":{"key":"weight","value":{"@type":"g:Double","@value":0.5}}}]}},{"id":{"@type":"g:Int32","@value":8},"inV":{"@type":"g:Int32","@value":4},"properties":{"weight":[{"@type":"g:Property","@value":{"key":"weight","value":{"@type":"g:Double","@value":1.0}}}]}}]},"properties":{"name":[{"id":{"@type":"g:Int64","@value":0},"value":"marko"}],"age":[{"id":{"@type":"g:Int64","@value":1},"value":{"@type":"g:Int32","@value":29}}]}}}""")
        assert isinstance(vertex, Vertex)
        assert "person" == vertex.label
        assert 1 == vertex.id
        assert isinstance(vertex.id, int)
        assert vertex == Vertex(1)
        ##
        vertex = self.graphson_reader.read_object("""
        {"@type":"g:Vertex", "@value":{"id":{"@type":"g:Float","@value":45.23}}}""")
        assert isinstance(vertex, Vertex)
        assert 45.23 == vertex.id
        assert isinstance(vertex.id, FloatType)
        assert "vertex" == vertex.label
        assert vertex == Vertex(45.23)
        ##
        vertex_property = self.graphson_reader.read_object("""
        {"@type":"g:VertexProperty", "@value":{"id":{"@type":"g:Int32","@value":1},"label":["name"],"value":"marko"}}""")
        assert isinstance(vertex_property, VertexProperty)
        assert 1 == vertex_property.id
        assert "name" == vertex_property.label
        assert "marko" == vertex_property.value
        assert vertex_property.vertex is None
        ##
        edge = self.graphson_reader.read_object("""
        {"@type":"g:Edge", "@value":{"id":{"@type":"g:Int64","@value":17},"label":["knows"],"inV":{"id":"x","label":["xLabel"]},"outV":{"id":"y","label":["vertex"]},"properties":{"aKey":[{"@type":"g:Property","@value":{"key":"aKey","value":"aValue"}}],"bKey":[{"@type":"g:Property","@value":{"key":"bKey","value":true}}]}}}""")
        # print edge
        assert isinstance(edge, Edge)
        assert 17 == edge.id
        assert "knows" == edge.label
        assert edge.inV == Vertex("x", "xLabel")
        assert edge.outV == Vertex("y", "vertex")
        ##
        property = self.graphson_reader.read_object("""
        {"@type":"g:Property", "@value":{"key":"aKey","value":{"@type":"g:Int64","@value":17}}}""")
        # print property
        assert isinstance(property, Property)
        assert "aKey" == property.key
        assert 17 == property.value

    def test_path(self):
        path = self.graphson_reader.read_object(
            """{"@type":"g:Path","@value":{"labels":{"@type":"g:List","@value":[{"@type":"g:Set","@value":["a"]},{"@type":"g:Set","@value":["b","c"]},{"@type":"g:Set","@value":[]}]},"objects":{"@type":"g:List","@value":[{"@type":"g:Vertex","@value":{"id":{"@type":"g:Int32","@value":1},"label":["person"],"properties":{"name":[{"@type":"g:VertexProperty","@value":{"id":{"@type":"g:Int64","@value":0},"value":"marko","label":["name"]}}],"age":[{"@type":"g:VertexProperty","@value":{"id":{"@type":"g:Int64","@value":1},"value":{"@type":"g:Int32","@value":29},"label":["age"]}}]}}},{"@type":"g:Vertex","@value":{"id":{"@type":"g:Int32","@value":3},"label":["software"],"properties":{"name":[{"@type":"g:VertexProperty","@value":{"id":{"@type":"g:Int64","@value":4},"value":"lop","label":["name"]}}],"lang":[{"@type":"g:VertexProperty","@value":{"id":{"@type":"g:Int64","@value":5},"value":"java","label":["lang"]}}]}}},"lop"]}}}"""
        )
        assert isinstance(path, Path)
        assert "path[v[1], v[3], lop]" == str(path)
        assert Vertex(1) == path[0]
        assert Vertex(1) == path["a"]
        assert "lop" == path[2]
        assert 3 == len(path)

    def test_custom_mapping(self):

        # extended mapping
        class X:
            pass

        type_string = "test:Xtype"
        override_string = "g:Int64"
        serdes = Mock()

        reader = GraphSONReader(deserializer_map={type_string: serdes})
        assert type_string in reader.deserializers

        # base dicts are not modified
        assert type_string not in gremlin_python.structure.io.graphsonV4._deserializers

        x = X()
        o = reader.to_object({GraphSONUtil.TYPE_KEY: type_string, GraphSONUtil.VALUE_KEY: x})
        serdes.objectify.assert_called_once_with(x, reader)
        assert o is serdes.objectify()

        # overridden mapping
        type_string = "g:Int64"
        serdes = Mock()
        reader = GraphSONReader(deserializer_map={type_string: serdes, override_string: serdes})
        assert gremlin_python.structure.io.graphsonV4._deserializers[type_string] is not reader.deserializers[
            type_string]

        value = 3
        o = reader.to_object({GraphSONUtil.TYPE_KEY: type_string, GraphSONUtil.VALUE_KEY: value})
        serdes.objectify.assert_called_once_with(value, reader)
        assert o is serdes.objectify()

    def test_datetime(self):
        tz = datetime.timezone(datetime.timedelta(seconds=36000))
        ms = 12345678912
        expected = datetime.datetime(2022, 5, 20, tzinfo=tz) + datetime.timedelta(microseconds=ms)
        dt = self.graphson_reader.read_object(json.dumps({"@type": "g:DateTime", "@value": expected.isoformat()}))
        assert isinstance(dt, datetime.datetime)
        assert dt == expected

    def test_datetime_zulu(self):
        tz = datetime.timezone.utc
        ms = 12345678912
        expected = datetime.datetime(2022, 5, 20, tzinfo=tz) + datetime.timedelta(microseconds=ms)
        # simulate zulu format
        expected_zulu = expected.isoformat()[:-6] + 'Z'
        dt = self.graphson_reader.read_object(json.dumps({"@type": "g:DateTime", "@value": expected_zulu}))
        assert isinstance(dt, datetime.datetime)
        assert dt == expected

    def test_duration(self):
        d = self.graphson_reader.read_object(json.dumps({"@type": "g:Duration", "@value": "PT120H"}))
        assert isinstance(d, datetime.timedelta)
        assert d == datetime.timedelta(hours=120)

    def test_uuid(self):
        prop = self.graphson_reader.read_object(
            json.dumps({'@type': 'g:UUID', '@value': "41d2e28a-20a4-4ab0-b379-d810dede3786"}))
        assert isinstance(prop, uuid.UUID)
        assert str(prop) == '41d2e28a-20a4-4ab0-b379-d810dede3786'

    def test_binary(self):
        bb = self.graphson_reader.read_object(
            json.dumps({"@type": "g:Binary", "@value": "c29tZSBieXRlcyBmb3IgeW91"}))
        assert isinstance(bb, bytes)
        assert bytes("some bytes for you", "utf8") == bb

    def test_char(self):
        c = self.graphson_reader.read_object(json.dumps({"@type": "g:Char", "@value": "L"}))
        assert isinstance(c, SingleChar)
        assert chr(76) == c

    def test_null(self):
        c = self.graphson_reader.read_object(json.dumps(None))
        assert c is None


class TestGraphSONWriter:
    graphson_writer = GraphSONWriter()
    graphson_reader = GraphSONReader()

    def test_collections(self):
        assert {"@type": "g:List", "@value": [{"@type": "g:Int32", "@value": 1},
                                              {"@type": "g:Int32", "@value": 2},
                                              {"@type": "g:Int32", "@value": 3}]} == json.loads(
            self.graphson_writer.write_object([1, 2, 3]))
        assert {"@type": "g:Set", "@value": [{"@type": "g:Int32", "@value": 1},
                                             {"@type": "g:Int32", "@value": 2},
                                             {"@type": "g:Int32", "@value": 3}]} == json.loads(
            self.graphson_writer.write_object({1, 2, 3, 3}))
        assert {"@type": "g:Map",
                "@value": ['a', {"@type": "g:Int32", "@value": 1}]} == json.loads(
            self.graphson_writer.write_object({'a': 1}))

    def test_numbers(self):
        assert {"@type": "g:Byte", "@value": 1} == json.loads(self.graphson_writer.write_object(int.__new__(SingleByte, 1)))
        assert {"@type": "g:Int16", "@value": 16} == json.loads(self.graphson_writer.write_object(short(16)))
        assert {"@type": "g:Int16", "@value": -16} == json.loads(self.graphson_writer.write_object(short(-16)))
        assert {"@type": "g:Int64", "@value": 2} == json.loads(self.graphson_writer.write_object(long(2)))
        assert {"@type": "g:Int64", "@value": 851401972585122} == json.loads(self.graphson_writer.write_object(long(851401972585122)))
        assert {"@type": "g:Int64", "@value": -2} == json.loads(self.graphson_writer.write_object(long(-2)))
        assert {"@type": "g:Int64", "@value": -851401972585122} == json.loads(self.graphson_writer.write_object(long(-851401972585122)))
        assert {"@type": "g:Int32", "@value": 1} == json.loads(self.graphson_writer.write_object(1))
        assert {"@type": "g:Int32", "@value": -1} == json.loads(self.graphson_writer.write_object(-1))
        assert {"@type": "g:Int64", "@value": 851401972585122} == json.loads(self.graphson_writer.write_object(851401972585122))
        assert {"@type": "g:Double", "@value": 3.2} == json.loads(self.graphson_writer.write_object(3.2))
        assert {"@type": "g:Double", "@value": "NaN"} == json.loads(self.graphson_writer.write_object(float('nan')))
        assert {"@type": "g:Double", "@value": "Infinity"} == json.loads(self.graphson_writer.write_object(float('inf')))
        assert {"@type": "g:Double", "@value": "-Infinity"} == json.loads(self.graphson_writer.write_object(float('-inf')))
        assert {"@type": "g:BigDecimal", "@value": "123.456789987654321123456789987654321"} == json.loads(self.graphson_writer.write_object(to_bigdecimal("123.456789987654321123456789987654321")))
        assert {"@type": "g:BigInteger", "@value": "123456789987654321123456789987654321"} == json.loads(self.graphson_writer.write_object(long(123456789987654321123456789987654321)))
        assert {"@type": "g:BigInteger", "@value": "123456789987654321123456789987654321"} == json.loads(self.graphson_writer.write_object(123456789987654321123456789987654321))
        assert """true""" == self.graphson_writer.write_object(True)

    def test_enum(self):
        assert {"@type": "g:Direction", "@value": "OUT"} == json.loads(self.graphson_writer.write_object(Direction.OUT))
        assert {"@type": "g:Direction", "@value": "OUT"} == json.loads(self.graphson_writer.write_object(Direction.from_))

    def test_graph(self):
        assert {"@type": "g:Vertex",
                "@value": {"id": {"@type": "g:Int64", "@value": 12}, "label": ["person"]}} == json.loads(
            self.graphson_writer.write_object(Vertex(long(12), "person")))

        assert {'@type': 'g:Edge', '@value': {'id': {'@type': 'g:Int32', '@value': 7},
                                              'inV': {'id': {'@type': 'g:Int32', '@value': 1},
                                                      'label': ['dog']},
                                              'label': ['knows'],
                                              'outV': {'id': {'@type': 'g:Int32', '@value': 0},
                                                       'label': ['person']}}} == json.loads(
            self.graphson_writer.write_object(Edge(7, Vertex(0, "person"), "knows", Vertex(1, "dog"))))
        assert {"@type": "g:VertexProperty", "@value": {"id": "blah", "label": ["keyA"], "value": True}} == json.loads(
            self.graphson_writer.write_object(VertexProperty("blah", "keyA", True, Vertex("stephen"))))

        assert {"@type": "g:Property",
                "@value": {"key": "name", "value": "marko"}} == json.loads(
            self.graphson_writer.write_object(
                Property("name", "marko", VertexProperty(1234, "aKey", 21345, Vertex("vertexId")))))

        vertex = self.graphson_reader.read_object(self.graphson_writer.write_object(Vertex(1, "person")))
        assert 1 == vertex.id
        assert "person" == vertex.label

        edge = self.graphson_reader.read_object(
            self.graphson_writer.write_object(Edge(3, Vertex(1, "person"), "knows", Vertex(2, "dog"))))
        assert "knows" == edge.label
        assert 3 == edge.id
        assert 1 == edge.outV.id
        assert 2 == edge.inV.id

        vertex_property = self.graphson_reader.read_object(
            self.graphson_writer.write_object(VertexProperty(1, "age", 32, Vertex(1))))
        assert 1 == vertex_property.id
        assert "age" == vertex_property.key
        assert 32 == vertex_property.value

        property = self.graphson_reader.read_object(self.graphson_writer.write_object(Property("age", 32.2, Edge(1, Vertex(2), "knows", Vertex(3)))))
        assert "age" == property.key
        assert 32.2 == property.value

    def test_custom_mapping(self):
        # extended mapping
        class X:
            pass

        serdes = Mock()
        writer = GraphSONWriter(serializer_map={X: serdes})
        assert X in writer.serializers

        # base dicts are not modified
        assert X not in gremlin_python.structure.io.graphsonV4._serializers

        obj = X()
        d = writer.to_dict(obj)
        serdes.dictify.assert_called_once_with(obj, writer)
        assert d is serdes.dictify()

        # overridden mapping
        serdes = Mock()
        writer = GraphSONWriter(serializer_map={int: serdes})
        assert gremlin_python.structure.io.graphsonV4._serializers[int] is not writer.serializers[int]

        value = 3
        d = writer.to_dict(value)
        serdes.dictify.assert_called_once_with(value, writer)
        assert d is serdes.dictify()

    def test_write_long(self):
        mapping = self.graphson_writer.to_dict(1)
        assert mapping['@type'] == 'g:Int32'
        assert mapping['@value'] == 1

        mapping = self.graphson_writer.to_dict(long(1))
        assert mapping['@type'] == 'g:Int64'
        assert mapping['@value'] == 1

    def test_datetime(self):
        expected = json.dumps({"@type": "g:DateTime", "@value": "2022-05-20T03:25:45.678912+10:00"}, separators=(',', ':'))
        tz = datetime.timezone(datetime.timedelta(seconds=36000))
        ms = 12345678912
        dt = datetime.datetime(2022, 5, 20, tzinfo=tz) + datetime.timedelta(microseconds=ms)
        output = self.graphson_writer.write_object(dt)
        assert expected == output

    def test_duration(self):
        expected = json.dumps({"@type": "g:Duration", "@value": "P5D"}, separators=(',', ':'))
        d = datetime.timedelta(hours=120)
        output = self.graphson_writer.write_object(d)
        assert expected == output

    def test_uuid(self):
        expected = json.dumps({'@type': 'g:UUID', '@value': "41d2e28a-20a4-4ab0-b379-d810dede3786"}, separators=(',', ':'))
        prop = uuid.UUID("41d2e28a-20a4-4ab0-b379-d810dede3786")
        output = self.graphson_writer.write_object(prop)
        assert expected == output

    def test_binary(self):
        expected = json.dumps({'@type': 'g:Binary', '@value': 'c29tZSBieXRlcyBmb3IgeW91'}, separators=(',', ':'))
        bb = bytes("some bytes for you", "utf8")
        output = self.graphson_writer.write_object(bb)
        assert expected == output

    def test_char(self):
        expected = json.dumps({'@type': 'g:Char', '@value': 'L'}, separators=(',', ':'))
        c = str.__new__(SingleChar, chr(76))
        output = self.graphson_writer.write_object(c)
        assert expected == output