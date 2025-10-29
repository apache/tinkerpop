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
from gremlin_python.structure.io.graphsonV3d0 import GraphSONWriter, GraphSONReader, GraphSONUtil
import gremlin_python.structure.io.graphsonV3d0
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

        # BulkSet gets coerced to a List - both have the same behavior
        x = self.graphson_reader.read_object(
            json.dumps({"@type": "g:BulkSet",
                        "@value": ["marko", {"@type": "g:Int64", "@value": 1}, "josh", {"@type": "g:Int64", "@value": 3}]}))
        assert isinstance(x, list)
        assert len(x) == 4
        assert x.count("marko") == 1
        assert x.count("josh") == 3

    def test_number_input(self):
        x = self.graphson_reader.read_object(json.dumps({
            "@type": "gx:Byte",
            "@value": 1
        }))
        assert isinstance(x, SingleByte)
        assert 1 == x
        x = self.graphson_reader.read_object(json.dumps({
            "@type": "gx:Int16",
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
            "@type": "gx:BigDecimal",
            "@value": 31.2
        }))
        assert isinstance(x, BigDecimal)
        assert bigdecimal(31.2) == x
        ##
        x = self.graphson_reader.read_object(json.dumps({
            "@type": "gx:BigDecimal",
            "@value": 123456789987654321123456789987654321
        }))
        assert isinstance(x, BigDecimal)
        assert bigdecimal('123456789987654321123456789987654321') == x
        ##
        x = self.graphson_reader.read_object(json.dumps({
            "@type": "gx:BigInteger",
            "@value": 31
        }))
        assert isinstance(x, long)
        assert 31 == x
        ##
        x = self.graphson_reader.read_object(json.dumps({
            "@type": "gx:BigInteger",
            "@value": 123456789987654321123456789987654321
        }))
        assert isinstance(x, long)
        assert 123456789987654321123456789987654321 == x

    def test_graph(self):
        vertex = self.graphson_reader.read_object("""
        {"@type":"g:Vertex", "@value":{"id":{"@type":"g:Int32","@value":1},"label":"person","properties":{"name":[{"@type":"g:VertexProperty","@value":{"id":{"@type":"g:Int64","@value":0},"value":"marko","label":"name"}}],"age":[{"@type":"g:VertexProperty","@value":{"id":{"@type":"g:Int64","@value":1},"value":{"@type":"g:Int32","@value":29},"label":"age"}}]}}}""")
        assert isinstance(vertex, Vertex)
        assert "person" == vertex.label
        assert 1 == vertex.id
        assert isinstance(vertex.id, int)
        assert vertex == Vertex(1)
        assert 2 == len(vertex.properties)
        # assert actual property values - Vertex.properties should be VertexProperty objects
        assert any(vp.label == 'name' and vp.value == 'marko' for vp in vertex.properties)
        assert any(vp.label == 'age' and vp.value == 29 for vp in vertex.properties)
        ##
        vertex = self.graphson_reader.read_object("""
        {"@type":"g:Vertex", "@value":{"id":{"@type":"g:Float","@value":45.23}}}""")
        assert isinstance(vertex, Vertex)
        assert 45.23 == vertex.id
        assert isinstance(vertex.id, FloatType)
        assert "vertex" == vertex.label
        assert vertex == Vertex(45.23)
        # properties key omitted should yield empty list
        assert vertex.properties == []
        ##
        # vertex with explicit label and without properties
        vertex = self.graphson_reader.read_object(
            """
        {"@type":"g:Vertex", "@value":{"id":{"@type":"g:Int32","@value":2},"label":"person"}}
            """)
        assert isinstance(vertex, Vertex)
        assert vertex.label == 'person'
        assert vertex.id == 2
        assert vertex.properties == []
        ##
        vertex_property = self.graphson_reader.read_object("""
        {"@type":"g:VertexProperty", "@value":{"id":"anId","label":"aKey","value":true,"vertex":{"@type":"g:Int32","@value":9}}}""")
        assert isinstance(vertex_property, VertexProperty)
        assert "anId" == vertex_property.id
        assert "aKey" == vertex_property.label
        assert vertex_property.value
        assert vertex_property.vertex == Vertex(9)
        # no properties key should yield empty list of meta-properties
        assert vertex_property.properties == []
        ##
        vertex_property = self.graphson_reader.read_object("""
        {"@type":"g:VertexProperty", "@value":{"id":{"@type":"g:Int32","@value":1},"label":"name","value":"marko"}}""")
        assert isinstance(vertex_property, VertexProperty)
        assert 1 == vertex_property.id
        assert "name" == vertex_property.label
        assert "marko" == vertex_property.value
        assert vertex_property.vertex is None
        # no properties key should yield empty list of meta-properties
        assert vertex_property.properties == []
        ##
        edge = self.graphson_reader.read_object("""
        {"@type":"g:Edge", "@value":{"id":{"@type":"g:Int64","@value":17},"label":"knows","inV":"x","outV":"y","inVLabel":"xLab","properties":{"aKey":"aValue","bKey":true}}}""")
        # print edge
        assert isinstance(edge, Edge)
        assert 17 == edge.id
        assert "knows" == edge.label
        assert edge.inV == Vertex("x", "xLabel")
        assert edge.outV == Vertex("y", "vertex")
        ##
        # edge without properties should yield empty properties list
        edge2 = self.graphson_reader.read_object(
            """
        {"@type":"g:Edge", "@value":{"id":{"@type":"g:Int64","@value":18},"label":"knows","inV":"a","outV":"b","inVLabel":"xLab"}}
            """)
        assert isinstance(edge2, Edge)
        assert edge2.properties == []
        ##
        property = self.graphson_reader.read_object("""
        {"@type":"g:Property", "@value":{"key":"aKey","value":{"@type":"g:Int64","@value":17},"element":{"@type":"g:Edge","@value":{"id":{"@type":"g:Int64","@value":122},"label":"knows","inV":"x","outV":"y","inVLabel":"xLab"}}}}""")
        # print property
        assert isinstance(property, Property)
        assert "aKey" == property.key
        assert 17 == property.value
        assert Edge(122, Vertex("x"), "knows", Vertex("y")) == property.element

    def test_path(self):
        # original path with vertices that include properties
        path = self.graphson_reader.read_object(
            """{"@type":"g:Path","@value":{"labels":{"@type":"g:List","@value":[{"@type":"g:Set","@value":["a"]},{"@type":"g:Set","@value":["b","c"]},{"@type":"g:Set","@value":[]}]},"objects":{"@type":"g:List","@value":[{"@type":"g:Vertex","@value":{"id":{"@type":"g:Int32","@value":1},"label":"person","properties":{"name":[{"@type":"g:VertexProperty","@value":{"id":{"@type":"g:Int64","@value":0},"value":"marko","label":"name"}}],"age":[{"@type":"g:VertexProperty","@value":{"id":{"@type":"g:Int64","@value":1},"value":{"@type":"g:Int32","@value":29},"label":"age"}}]}}},{"@type":"g:Vertex","@value":{"id":{"@type":"g:Int32","@value":3},"label":"software","properties":{"name":[{"@type":"g:VertexProperty","@value":{"id":{"@type":"g:Int64","@value":4},"value":"lop","label":"name"}}],"lang":[{"@type":"g:VertexProperty","@value":{"id":{"@type":"g:Int64","@value":5},"value":"java","label":"lang"}}]}}},"lop"]}}}"""
        )
        assert isinstance(path, Path)
        assert "path[v[1], v[3], lop]" == str(path)
        assert Vertex(1) == path[0]
        assert Vertex(1) == path["a"]
        assert "lop" == path[2]
        assert 3 == len(path)
        # ensure element properties were populated from GraphSON
        assert any(vp.label == 'name' and vp.value == 'marko' for vp in path[0].properties)
        assert any(vp.label == 'age' and vp.value == 29 for vp in path[0].properties)
        assert any(vp.label == 'name' and vp.value == 'lop' for vp in path[1].properties)
        assert any(vp.label == 'lang' and vp.value == 'java' for vp in path[1].properties)

        # path with elements that exclude properties (no "properties" key)
        path2 = self.graphson_reader.read_object(
            """{"@type":"g:Path","@value":{"labels":{"@type":"g:List","@value":[{"@type":"g:Set","@value":["x"]},{"@type":"g:Set","@value":["y"]},{"@type":"g:Set","@value":["z"]}]},"objects":{"@type":"g:List","@value":[{"@type":"g:Vertex","@value":{"id":{"@type":"g:Int32","@value":11},"label":"person"}},{"@type":"g:Edge","@value":{"id":{"@type":"g:Int64","@value":77},"label":"knows","outV":{"@type":"g:Int32","@value":11},"outVLabel":"person","inV":{"@type":"g:Int32","@value":12},"inVLabel":"person"}},"hello"]}}}"""
        )
        assert isinstance(path2, Path)
        assert 3 == len(path2)
        assert Vertex(11, 'person') == path2[0]
        assert path2[0].properties == []
        assert isinstance(path2[1], Edge)
        assert path2[1].properties == []
        assert path2[2] == "hello"

        # mixed path: first vertex with properties, second vertex without
        path3 = self.graphson_reader.read_object(
            """{"@type":"g:Path","@value":{"labels":{"@type":"g:List","@value":[{"@type":"g:Set","@value":["a"]},{"@type":"g:Set","@value":["b"]}]},"objects":{"@type":"g:List","@value":[{"@type":"g:Vertex","@value":{"id":{"@type":"g:Int32","@value":1},"label":"person","properties":{"name":[{"@type":"g:VertexProperty","@value":{"id":{"@type":"g:Int64","@value":0},"value":"marko","label":"name"}}]}}},{"@type":"g:Vertex","@value":{"id":{"@type":"g:Int32","@value":2},"label":"person"}}]}}}"""
        )
        assert isinstance(path3, Path)
        assert 2 == len(path3)
        assert Vertex(1) == path3[0]
        assert any(vp.label == 'name' and vp.value == 'marko' for vp in path3[0].properties)
        assert Vertex(2) == path3[1]
        assert path3[1].properties == []

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
        assert type_string not in gremlin_python.structure.io.graphsonV3d0._deserializers

        x = X()
        o = reader.to_object({GraphSONUtil.TYPE_KEY: type_string, GraphSONUtil.VALUE_KEY: x})
        serdes.objectify.assert_called_once_with(x, reader)
        assert o is serdes.objectify()

        # overridden mapping
        type_string = "g:Int64"
        serdes = Mock()
        reader = GraphSONReader(deserializer_map={type_string: serdes, override_string: serdes})
        assert gremlin_python.structure.io.graphsonV3d0._deserializers[type_string] is not reader.deserializers[
            type_string]

        value = 3
        o = reader.to_object({GraphSONUtil.TYPE_KEY: type_string, GraphSONUtil.VALUE_KEY: value})
        serdes.objectify.assert_called_once_with(value, reader)
        assert o is serdes.objectify()

    def test_datetime(self):
        expected = datetime.datetime(2016, 12, 14, 16, 14, 36, 295000)
        pts = calendar.timegm(expected.utctimetuple()) + expected.microsecond / 1e6
        ts = int(round(pts * 1000))
        output = self.graphson_reader.read_object(json.dumps({"@type": "g:Date", "@value": ts}))
        assert isinstance(output, datetime.datetime)
        # TINKERPOP-1848
        assert expected == output

    def test_offsetdatetime(self):
        tz = datetime.timezone(datetime.timedelta(seconds=36000))
        ms = 12345678912
        expected = datetime.datetime(2022, 5, 20, tzinfo=tz) + datetime.timedelta(microseconds=ms)
        output = self.graphson_reader.read_object(json.dumps({"@type": "gx:OffsetDateTime", "@value": expected.isoformat()}))
        assert isinstance(output, datetime.datetime)
        assert expected == output

    def test_offsetdatetime_zulu(self):
        tz = datetime.timezone.utc
        ms = 12345678912
        expected = datetime.datetime(2022, 5, 20, tzinfo=tz) + datetime.timedelta(microseconds=ms)
        # simulate zulu format
        expected_zulu = expected.isoformat()[:-6] + 'Z'
        output = self.graphson_reader.read_object(json.dumps({"@type": "gx:OffsetDateTime", "@value": expected_zulu}))
        assert isinstance(output, datetime.datetime)
        assert expected == output

    def test_timestamp(self):
        dt = self.graphson_reader.read_object(json.dumps({"@type": "g:Timestamp", "@value": 1481750076295}))
        assert isinstance(dt, timestamp)
        assert float(dt) == 1481750076.295

    def test_duration(self):
        d = self.graphson_reader.read_object(json.dumps({"@type": "gx:Duration", "@value": "PT120H"}))
        assert isinstance(d, datetime.timedelta)
        assert d == datetime.timedelta(hours=120)

    def test_uuid(self):
        prop = self.graphson_reader.read_object(
            json.dumps({'@type': 'g:UUID', '@value': "41d2e28a-20a4-4ab0-b379-d810dede3786"}))
        assert isinstance(prop, uuid.UUID)
        assert str(prop) == '41d2e28a-20a4-4ab0-b379-d810dede3786'

    def test_metrics(self):
        prop = self.graphson_reader.read_object(
            json.dumps([{'@type': 'g:TraversalMetrics', '@value': {'dur': 1.468594, 'metrics': [
                {'@type': 'g:Metrics', '@value': {'dur': 1.380957, 'counts': {}, 'name': 'GraphStep(__.V())', 'annotations': {'percentDur': 94.03259171697556}, 'id': '4.0.0()'}},
                {'@type': 'g:Metrics', '@value': {'dur': 0.087637, 'counts': {}, 'name': 'ReferenceElementStep', 'annotations': {'percentDur': 5.967408283024444}, 'id': '3.0.0()'}}
            ]}}]))
        assert isinstance(prop, list)
        assert prop == [{'dur': 1.468594, 'metrics': [
                {'dur': 1.380957, 'counts': {}, 'name': 'GraphStep(__.V())', 'annotations': {'percentDur': 94.03259171697556}, 'id': '4.0.0()'},
                {'dur': 0.087637, 'counts': {}, 'name': 'ReferenceElementStep', 'annotations': {'percentDur': 5.967408283024444}, 'id': '3.0.0()'}
                ]}]

    def test_bytebuffer(self):
        bb = self.graphson_reader.read_object(
            json.dumps({"@type": "gx:ByteBuffer", "@value": "c29tZSBieXRlcyBmb3IgeW91"}))
        assert isinstance(bb, ByteBufferType)
        assert ByteBufferType("c29tZSBieXRlcyBmb3IgeW91", "utf8") == bb

    def test_char(self):
        c = self.graphson_reader.read_object(json.dumps({"@type": "gx:Char", "@value": "L"}))
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
        assert {"@type": "gx:Byte", "@value": 1} == json.loads(self.graphson_writer.write_object(int.__new__(SingleByte, 1)))
        assert {"@type": "gx:Int16", "@value": 16} == json.loads(self.graphson_writer.write_object(short(16)))
        assert {"@type": "gx:Int16", "@value": -16} == json.loads(self.graphson_writer.write_object(short(-16)))
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
        assert {"@type": "gx:BigDecimal", "@value": "123456789987654321123456789987654321"} == json.loads(self.graphson_writer.write_object(bigdecimal('123456789987654321123456789987654321')))
        assert {"@type": "gx:BigInteger", "@value": "123456789987654321123456789987654321"} == json.loads(self.graphson_writer.write_object(long(123456789987654321123456789987654321)))
        assert {"@type": "gx:BigInteger", "@value": "123456789987654321123456789987654321"} == json.loads(self.graphson_writer.write_object(123456789987654321123456789987654321))
        assert """true""" == self.graphson_writer.write_object(True)

    def test_enum(self):
        assert {"@type": "g:Merge", "@value": "onMatch"} == json.loads(self.graphson_writer.write_object(Merge.on_match))
        assert {"@type": "g:Order", "@value": "shuffle"} == json.loads(self.graphson_writer.write_object(Order.shuffle))
        assert {"@type": "g:Barrier", "@value": "normSack"} == json.loads(self.graphson_writer.write_object(Barrier.norm_sack))
        assert {"@type": "g:Operator", "@value": "sum"} == json.loads(self.graphson_writer.write_object(Operator.sum_))
        assert {"@type": "g:Operator", "@value": "sumLong"} == json.loads(self.graphson_writer.write_object(Operator.sum_long))
        assert {"@type": "g:Direction", "@value": "OUT"} == json.loads(self.graphson_writer.write_object(Direction.OUT))
        assert {"@type": "g:Direction", "@value": "OUT"} == json.loads(self.graphson_writer.write_object(Direction.from_))

    def test_P(self):
        result = {'@type': 'g:P',
                  '@value': {
                      'predicate': 'and',
                      'value': [{
                          '@type': 'g:P',
                          '@value': {
                              'predicate': 'or',
                              'value': [{
                                  '@type': 'g:P',
                                  '@value': {'predicate': 'lt', 'value': 'b'}
                              },
                                  {'@type': 'g:P', '@value': {'predicate': 'gt', 'value': 'c'}}
                              ]
                          }
                      },
                          {'@type': 'g:P', '@value': {'predicate': 'neq', 'value': 'd'}}]}}

        assert result == json.loads(
            self.graphson_writer.write_object(P.lt("b").or_(P.gt("c")).and_(P.neq("d"))))

        result = {'@type': 'g:P', '@value': {'predicate': 'within', 'value': {'@type': 'g:List', '@value': [
            {"@type": "g:Int32", "@value": 1}, {"@type": "g:Int32", "@value": 2}]}}}
        assert result == json.loads(self.graphson_writer.write_object(P.within([1, 2])))
        assert result == json.loads(self.graphson_writer.write_object(P.within({1, 2})))
        assert result == json.loads(self.graphson_writer.write_object(P.within(1, 2)))

        result = {'@type': 'g:P', '@value': {'predicate': 'within', 'value': {'@type': 'g:List', '@value': [
            {"@type": "g:Int32", "@value": 1}]}}}
        assert result == json.loads(self.graphson_writer.write_object(P.within([1])))
        assert result == json.loads(self.graphson_writer.write_object(P.within({1})))
        assert result == json.loads(self.graphson_writer.write_object(P.within(1)))

    def test_strategies(self):
        assert {"@type": "g:SubgraphStrategy", "@value": {'conf': {}, 'fqcn': 'org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy'}} == json.loads(
            self.graphson_writer.write_object(SubgraphStrategy))
        assert {"@type": "g:SubgraphStrategy", "@value": {'conf': { "vertices": {"@type": "g:Bytecode", "@value": {"step": [["has", "name", "marko"]]}}}, 'fqcn': 'org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy'}} == json.loads(
            self.graphson_writer.write_object(SubgraphStrategy(vertices=__.has("name", "marko"))))

    def test_graph(self):
        # TODO: this assert is not compatible with python 3 and now that we test with both 2 and 3 it fails
        assert {"@type": "g:Vertex",
                "@value": {"id": {"@type": "g:Int64", "@value": 12}, "label": "person"}} == json.loads(
            self.graphson_writer.write_object(Vertex(long(12), "person")))

        assert {"@type": "g:Edge", "@value": {"id": {"@type": "g:Int32", "@value": 7},
                                              "outV": {"@type": "g:Int32", "@value": 0},
                                              "outVLabel": "person",
                                              "label": "knows",
                                              "inV": {"@type": "g:Int32", "@value": 1},
                                              "inVLabel": "dog"}} == json.loads(
            self.graphson_writer.write_object(Edge(7, Vertex(0, "person"), "knows", Vertex(1, "dog"))))
        assert {"@type": "g:VertexProperty", "@value": {"id": "blah", "label": "keyA", "value": True,
                                                        "vertex": "stephen"}} == json.loads(
            self.graphson_writer.write_object(VertexProperty("blah", "keyA", True, Vertex("stephen"))))

        assert {"@type": "g:Property",
                "@value": {"key": "name", "value": "marko", "element": {"@type": "g:VertexProperty",
                                                                        "@value": {
                                                                            "vertex": "vertexId",
                                                                            "id": {"@type": "g:Int32", "@value": 1234},
                                                                            "label": "aKey"}}}} == json.loads(
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
        assert X not in gremlin_python.structure.io.graphsonV3d0._serializers

        obj = X()
        d = writer.to_dict(obj)
        serdes.dictify.assert_called_once_with(obj, writer)
        assert d is serdes.dictify()

        # overridden mapping
        serdes = Mock()
        writer = GraphSONWriter(serializer_map={int: serdes})
        assert gremlin_python.structure.io.graphsonV3d0._serializers[int] is not writer.serializers[int]

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
        expected = json.dumps({"@type": "g:Date", "@value": 1481750076295}, separators=(',', ':'))
        dt = datetime.datetime.utcfromtimestamp(1481750076295 / 1000.0)
        output = self.graphson_writer.write_object(dt)
        assert expected == output

    def test_timestamp(self):
        expected = json.dumps({"@type": "g:Timestamp", "@value": 1481750076295}, separators=(',', ':'))
        ts = timestamp(1481750076295 / 1000.0)
        output = self.graphson_writer.write_object(ts)
        assert expected == output

    def test_duration(self):
        expected = json.dumps({"@type": "gx:Duration", "@value": "P5D"}, separators=(',', ':'))
        d = datetime.timedelta(hours=120)
        output = self.graphson_writer.write_object(d)
        assert expected == output

    def test_uuid(self):
        expected = json.dumps({'@type': 'g:UUID', '@value': "41d2e28a-20a4-4ab0-b379-d810dede3786"}, separators=(',', ':'))
        prop = uuid.UUID("41d2e28a-20a4-4ab0-b379-d810dede3786")
        output = self.graphson_writer.write_object(prop)
        assert expected == output

    def test_bytebuffer(self):
        expected = json.dumps({'@type': 'gx:ByteBuffer', '@value': 'c29tZSBieXRlcyBmb3IgeW91'}, separators=(',', ':'))
        bb = ByteBufferType("c29tZSBieXRlcyBmb3IgeW91", "utf8")
        output = self.graphson_writer.write_object(bb)
        assert expected == output

    def test_char(self):
        expected = json.dumps({'@type': 'gx:Char', '@value': 'L'}, separators=(',', ':'))
        c = str.__new__(SingleChar, chr(76))
        output = self.graphson_writer.write_object(c)
        assert expected == output
