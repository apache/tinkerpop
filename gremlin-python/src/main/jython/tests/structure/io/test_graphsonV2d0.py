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
import time
import json
import uuid
import math
from decimal import *

from mock import Mock

import six

from gremlin_python.statics import *
from gremlin_python.structure.graph import Vertex, Edge, Property, VertexProperty, Graph, Path
from gremlin_python.structure.io.graphsonV2d0 import GraphSONWriter, GraphSONReader, GraphSONUtil
import gremlin_python.structure.io.graphsonV2d0
from gremlin_python.process.traversal import P
from gremlin_python.process.strategies import SubgraphStrategy
from gremlin_python.process.graph_traversal import __


class TestGraphSONReader(object):
    graphson_reader = GraphSONReader()

    def test_number_input(self):
        x = self.graphson_reader.readObject(json.dumps({
            "@type": "gx:Byte",
            "@value": 1
        }))
        assert isinstance(x, SingleByte)
        assert 1 == x
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
        ##
        x = self.graphson_reader.readObject(json.dumps({
            "@type": "g:Double",
            "@value": "NaN"
        }))
        assert isinstance(x, float)
        assert math.isnan(x)
        ##
        x = self.graphson_reader.readObject(json.dumps({
            "@type": "g:Double",
            "@value": "Infinity"
        }))
        assert isinstance(x, float)
        assert math.isinf(x) and x > 0
        ##
        x = self.graphson_reader.readObject(json.dumps({
            "@type": "g:Double",
            "@value": "-Infinity"
        }))
        assert isinstance(x, float)
        assert math.isinf(x) and x < 0
        ##
        x = self.graphson_reader.readObject(json.dumps({
            "@type": "gx:BigDecimal",
            "@value": 31.2
        }))
        assert isinstance(x, Decimal)
        assert Decimal(31.2) == x
        ##
        x = self.graphson_reader.readObject(json.dumps({
            "@type": "gx:BigDecimal",
            "@value": 123456789987654321123456789987654321
        }))
        assert isinstance(x, Decimal)
        assert Decimal('123456789987654321123456789987654321') == x
        ##
        x = self.graphson_reader.readObject(json.dumps({
            "@type": "gx:BigDecimal",
            "@value": "NaN"
        }))
        assert isinstance(x, Decimal)
        assert math.isnan(x)
        ##
        x = self.graphson_reader.readObject(json.dumps({
            "@type": "gx:BigDecimal",
            "@value": "Infinity"
        }))
        assert isinstance(x, Decimal)
        assert math.isinf(x) and x > 0
        ##
        x = self.graphson_reader.readObject(json.dumps({
            "@type": "gx:BigDecimal",
            "@value": "-Infinity"
        }))
        assert isinstance(x, Decimal)
        assert math.isinf(x) and x < 0
        ##
        x = self.graphson_reader.readObject(json.dumps({
            "@type": "gx:BigInteger",
            "@value": 31
        }))
        assert isinstance(x, long)
        assert 31 == x
        ##
        x = self.graphson_reader.readObject(json.dumps({
            "@type": "gx:BigInteger",
            "@value": 123456789987654321123456789987654321
        }))
        assert isinstance(x, long)
        assert 123456789987654321123456789987654321 == x

    def test_graph(self):
        vertex = self.graphson_reader.readObject("""
        {"@type":"g:Vertex", "@value":{"id":{"@type":"g:Int32","@value":1},"label":"person","outE":{"created":[{"id":{"@type":"g:Int32","@value":9},"inV":{"@type":"g:Int32","@value":3},"properties":{"weight":{"@type":"g:Double","@value":0.4}}}],"knows":[{"id":{"@type":"g:Int32","@value":7},"inV":{"@type":"g:Int32","@value":2},"properties":{"weight":{"@type":"g:Double","@value":0.5}}},{"id":{"@type":"g:Int32","@value":8},"inV":{"@type":"g:Int32","@value":4},"properties":{"weight":{"@type":"g:Double","@value":1.0}}}]},"properties":{"name":[{"id":{"@type":"g:Int64","@value":0},"value":"marko"}],"age":[{"id":{"@type":"g:Int64","@value":1},"value":{"@type":"g:Int32","@value":29}}]}}}""")
        assert isinstance(vertex, Vertex)
        assert "person" == vertex.label
        assert 1 == vertex.id
        assert isinstance(vertex.id, int)
        assert vertex == Vertex(1)
        ##
        vertex = self.graphson_reader.readObject("""
        {"@type":"g:Vertex", "@value":{"id":{"@type":"g:Float","@value":45.23}}}""")
        assert isinstance(vertex, Vertex)
        assert 45.23 == vertex.id
        assert isinstance(vertex.id, FloatType)
        assert "vertex" == vertex.label
        assert vertex == Vertex(45.23)
        ##
        vertex_property = self.graphson_reader.readObject("""
        {"@type":"g:VertexProperty", "@value":{"id":"anId","label":"aKey","value":true,"vertex":{"@type":"g:Int32","@value":9}}}""")
        assert isinstance(vertex_property, VertexProperty)
        assert "anId" == vertex_property.id
        assert "aKey" == vertex_property.label
        assert vertex_property.value
        assert vertex_property.vertex == Vertex(9)
        ##
        vertex_property = self.graphson_reader.readObject("""
        {"@type":"g:VertexProperty", "@value":{"id":{"@type":"g:Int32","@value":1},"label":"name","value":"marko"}}""")
        assert isinstance(vertex_property, VertexProperty)
        assert 1 == vertex_property.id
        assert "name" == vertex_property.label
        assert "marko" == vertex_property.value
        assert vertex_property.vertex is None
        ##
        edge = self.graphson_reader.readObject("""
        {"@type":"g:Edge", "@value":{"id":{"@type":"g:Int64","@value":17},"label":"knows","inV":"x","outV":"y","inVLabel":"xLab","properties":{"aKey":"aValue","bKey":true}}}""")
        # print edge
        assert isinstance(edge, Edge)
        assert 17 == edge.id
        assert "knows" == edge.label
        assert edge.inV == Vertex("x", "xLabel")
        assert edge.outV == Vertex("y", "vertex")
        ##
        property = self.graphson_reader.readObject("""
        {"@type":"g:Property", "@value":{"key":"aKey","value":{"@type":"g:Int64","@value":17},"element":{"@type":"g:Edge","@value":{"id":{"@type":"g:Int64","@value":122},"label":"knows","inV":"x","outV":"y","inVLabel":"xLab"}}}}""")
        # print property
        assert isinstance(property, Property)
        assert "aKey" == property.key
        assert 17 == property.value
        assert Edge(122, Vertex("x"), "knows", Vertex("y")) == property.element

    def test_path(self):
        path = self.graphson_reader.readObject(
            """{"@type":"g:Path","@value":{"labels":[["a"],["b","c"],[]],"objects":[{"@type":"g:Vertex","@value":{"id":{"@type":"g:Int32","@value":1},"label":"person","properties":{"name":[{"@type":"g:VertexProperty","@value":{"id":{"@type":"g:Int64","@value":0},"value":"marko","label":"name"}}],"age":[{"@type":"g:VertexProperty","@value":{"id":{"@type":"g:Int64","@value":1},"value":{"@type":"g:Int32","@value":29},"label":"age"}}]}}},{"@type":"g:Vertex","@value":{"id":{"@type":"g:Int32","@value":3},"label":"software","properties":{"name":[{"@type":"g:VertexProperty","@value":{"id":{"@type":"g:Int64","@value":4},"value":"lop","label":"name"}}],"lang":[{"@type":"g:VertexProperty","@value":{"id":{"@type":"g:Int64","@value":5},"value":"java","label":"lang"}}]}}},"lop"]}}"""
        )
        assert isinstance(path, Path)
        assert "path[v[1], v[3], lop]" == str(path)
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
        assert type_string not in gremlin_python.structure.io.graphsonV2d0._deserializers

        x = X()
        o = reader.toObject({GraphSONUtil.TYPE_KEY: type_string, GraphSONUtil.VALUE_KEY: x})
        serdes.objectify.assert_called_once_with(x, reader)
        assert o is serdes.objectify()

        # overridden mapping
        type_string = "g:Int64"
        serdes = Mock()
        reader = GraphSONReader(deserializer_map={type_string: serdes, override_string: serdes})
        assert gremlin_python.structure.io.graphsonV2d0._deserializers[type_string] is not reader.deserializers[type_string]

        value = 3
        o = reader.toObject({GraphSONUtil.TYPE_KEY: type_string, GraphSONUtil.VALUE_KEY: value})
        serdes.objectify.assert_called_once_with(value, reader)
        assert o is serdes.objectify()

    def test_datetime(self):
        expected = datetime.datetime(2016, 12, 14, 16, 14, 36, 295000)
        pts = time.mktime(expected.timetuple()) + expected.microsecond / 1e6 - \
                         (time.mktime(datetime.datetime(1970, 1, 1).timetuple()))
        ts = int(round(pts * 1000))
        dt = self.graphson_reader.readObject(json.dumps({"@type": "g:Date", "@value": ts}))
        assert isinstance(dt, datetime.datetime)
        # TINKERPOP-1848
        assert dt == expected

    def test_timestamp(self):
        dt = self.graphson_reader.readObject(json.dumps({"@type": "g:Timestamp", "@value": 1481750076295}))
        assert isinstance(dt, timestamp)
        assert float(dt) == 1481750076.295

    def test_duration(self):
        d = self.graphson_reader.readObject(json.dumps({"@type": "gx:Duration", "@value": "PT120H"}))
        assert isinstance(d, datetime.timedelta)
        assert d == datetime.timedelta(hours=120)

    def test_uuid(self):
        prop = self.graphson_reader.readObject(
            json.dumps({'@type': 'g:UUID', '@value': "41d2e28a-20a4-4ab0-b379-d810dede3786"}))
        assert isinstance(prop, uuid.UUID)
        assert str(prop) == '41d2e28a-20a4-4ab0-b379-d810dede3786'

    def test_metrics(self):
        prop = self.graphson_reader.readObject(
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
        bb = self.graphson_reader.readObject(
            json.dumps({"@type": "gx:ByteBuffer", "@value": "c29tZSBieXRlcyBmb3IgeW91"}))
        assert isinstance(bb, ByteBufferType)
        assert ByteBufferType("c29tZSBieXRlcyBmb3IgeW91", "utf8") == bb

    def test_char(self):
        c = self.graphson_reader.readObject(json.dumps({"@type": "gx:Char", "@value": "L"}))
        assert isinstance(c, SingleChar)
        assert chr(76) == c


class TestGraphSONWriter(object):
    graphson_writer = GraphSONWriter()
    graphson_reader = GraphSONReader()

    def test_number_output(self):
        assert {"@type": "g:Int64", "@value": 2} == json.loads(self.graphson_writer.writeObject(long(2)))
        assert {"@type": "g:Int32", "@value": 1} == json.loads(self.graphson_writer.writeObject(1))
        assert {"@type": "g:Double", "@value": 3.2} == json.loads(self.graphson_writer.writeObject(3.2))
        assert """true""" == self.graphson_writer.writeObject(True)

    def test_numbers(self):
        assert {"@type": "gx:Byte", "@value": 1} == json.loads(self.graphson_writer.writeObject(int.__new__(SingleByte, 1)))
        assert {"@type": "g:Int64", "@value": 2} == json.loads(self.graphson_writer.writeObject(long(2)))
        assert {"@type": "g:Int32", "@value": 1} == json.loads(self.graphson_writer.writeObject(1))
        assert {"@type": "g:Double", "@value": 3.2} == json.loads(self.graphson_writer.writeObject(3.2))
        assert {"@type": "g:Double", "@value": "NaN"} == json.loads(self.graphson_writer.writeObject(float('nan')))
        assert {"@type": "g:Double", "@value": "Infinity"} == json.loads(self.graphson_writer.writeObject(float('inf')))
        assert {"@type": "g:Double", "@value": "-Infinity"} == json.loads(self.graphson_writer.writeObject(float('-inf')))
        assert {"@type": "gx:BigDecimal", "@value": "123456789987654321123456789987654321"} == json.loads(self.graphson_writer.writeObject(Decimal('123456789987654321123456789987654321')))
        assert {"@type": "gx:BigDecimal", "@value": "NaN"} == json.loads(self.graphson_writer.writeObject(Decimal('nan')))
        assert {"@type": "gx:BigDecimal", "@value": "Infinity"} == json.loads(self.graphson_writer.writeObject(Decimal('inf')))
        assert {"@type": "gx:BigDecimal", "@value": "-Infinity"} == json.loads(self.graphson_writer.writeObject(Decimal('-inf')))
        assert {"@type": "gx:BigInteger", "@value": "123456789987654321123456789987654321"} == json.loads(self.graphson_writer.writeObject(long(123456789987654321123456789987654321)))
        assert {"@type": "gx:BigInteger", "@value": "123456789987654321123456789987654321"} == json.loads(self.graphson_writer.writeObject(123456789987654321123456789987654321))
        assert """true""" == self.graphson_writer.writeObject(True)

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
            self.graphson_writer.writeObject(P.lt("b").or_(P.gt("c")).and_(P.neq("d"))))

        result = {'@type': 'g:P', '@value': {'predicate':'within','value': [{"@type": "g:Int32", "@value": 1},{"@type": "g:Int32", "@value": 2}]}}
        assert result == json.loads(self.graphson_writer.writeObject(P.within([1, 2])))
        assert result == json.loads(self.graphson_writer.writeObject(P.within(1, 2)))

        result = {'@type': 'g:P', '@value': {'predicate':'within','value': [{"@type": "g:Int32", "@value": 1}]}}
        assert result == json.loads(self.graphson_writer.writeObject(P.within([1])))
        assert result == json.loads(self.graphson_writer.writeObject(P.within(1)))

    def test_strategies(self):
        # we have a proxy model for now given that we don't want to have to have g:XXX all registered on the Gremlin traversal machine (yet)
        assert {"@type": "g:SubgraphStrategy", "@value": {}} == json.loads(
            self.graphson_writer.writeObject(SubgraphStrategy))
        assert {"@type": "g:SubgraphStrategy", "@value": {
            "vertices": {"@type": "g:Bytecode", "@value": {"step": [["has", "name", "marko"]]}}}} == json.loads(
            self.graphson_writer.writeObject(SubgraphStrategy(vertices=__.has("name", "marko"))))

    def test_graph(self):
        # TODO: this assert is not compatible with python 3 and now that we test with both 2 and 3 it fails
        assert {"@type": "g:Vertex", "@value": {"id": {"@type": "g:Int64", "@value": 12}, "label": "person"}} == json.loads(self.graphson_writer.writeObject(Vertex(long(12), "person")))

        assert {"@type": "g:Edge", "@value": {"id": {"@type": "g:Int32", "@value": 7},
                                              "outV": {"@type": "g:Int32", "@value": 0},
                                              "outVLabel": "person",
                                              "label": "knows",
                                              "inV": {"@type": "g:Int32", "@value": 1},
                                              "inVLabel": "dog"}} == json.loads(
            self.graphson_writer.writeObject(Edge(7, Vertex(0, "person"), "knows", Vertex(1, "dog"))))
        assert {"@type": "g:VertexProperty", "@value": {"id": "blah", "label": "keyA", "value": True,
                                                        "vertex": "stephen"}} == json.loads(
            self.graphson_writer.writeObject(VertexProperty("blah", "keyA", True, Vertex("stephen"))))

        assert {"@type": "g:Property",
                "@value": {"key": "name", "value": "marko", "element": {"@type": "g:VertexProperty",
                                                                        "@value": {
                                                                            "vertex": "vertexId",
                                                                            "id": {"@type": "g:Int32", "@value": 1234},
                                                                            "label": "aKey"}}}} == json.loads(
            self.graphson_writer.writeObject(
                Property("name", "marko", VertexProperty(1234, "aKey", 21345, Vertex("vertexId")))))

        vertex = self.graphson_reader.readObject(self.graphson_writer.writeObject(Vertex(1, "person")))
        assert 1 == vertex.id
        assert "person" == vertex.label

        edge = self.graphson_reader.readObject(
            self.graphson_writer.writeObject(Edge(3, Vertex(1, "person"), "knows", Vertex(2, "dog"))))
        assert "knows" == edge.label
        assert 3 == edge.id
        assert 1 == edge.outV.id
        assert 2 == edge.inV.id

        vertex_property = self.graphson_reader.readObject(
            self.graphson_writer.writeObject(VertexProperty(1, "age", 32, Vertex(1))))
        assert 1 == vertex_property.id
        assert "age" == vertex_property.key
        assert 32 == vertex_property.value

        property = self.graphson_reader.readObject(self.graphson_writer.writeObject(Property("age", 32.2, Edge(1,Vertex(2),"knows",Vertex(3)))))
        assert "age" == property.key
        assert 32.2 == property.value

    def test_custom_mapping(self):
        # extended mapping
        class X(object):
            pass

        serdes = Mock()
        writer = GraphSONWriter(serializer_map={X: serdes})
        assert X in writer.serializers

        # base dicts are not modified
        assert X not in gremlin_python.structure.io.graphsonV2d0._serializers

        obj = X()
        d = writer.toDict(obj)
        serdes.dictify.assert_called_once_with(obj, writer)
        assert d is serdes.dictify()

        # overridden mapping
        serdes = Mock()
        writer = GraphSONWriter(serializer_map={int: serdes})
        assert gremlin_python.structure.io.graphsonV2d0._serializers[int] is not writer.serializers[int]

        value = 3
        d = writer.toDict(value)
        serdes.dictify.assert_called_once_with(value, writer)
        assert d is serdes.dictify()

    def test_write_long(self):
        mapping = self.graphson_writer.toDict(1)
        assert mapping['@type'] == 'g:Int32'
        assert mapping['@value'] == 1

        mapping = self.graphson_writer.toDict(long(1))
        assert mapping['@type'] == 'g:Int64'
        assert mapping['@value'] == 1

    def test_datetime(self):
        expected = json.dumps({"@type": "g:Date", "@value": 1481750076295}, separators=(',', ':'))
        dt = datetime.datetime.utcfromtimestamp(1481750076295 / 1000.0)
        output = self.graphson_writer.writeObject(dt)
        assert expected == output

    def test_timestamp(self):
        expected = json.dumps({"@type": "g:Timestamp", "@value": 1481750076295}, separators=(',', ':'))
        ts = timestamp(1481750076295 / 1000.0)
        output = self.graphson_writer.writeObject(ts)
        assert expected == output

    def test_duration(self):
        expected = json.dumps({"@type": "gx:Duration", "@value": "P5D"}, separators=(',', ':'))
        d = datetime.timedelta(hours=120)
        output = self.graphson_writer.writeObject(d)
        assert expected == output

    def test_uuid(self):
        expected = json.dumps({'@type': 'g:UUID', '@value': "41d2e28a-20a4-4ab0-b379-d810dede3786"}, separators=(',', ':'))
        prop = uuid.UUID("41d2e28a-20a4-4ab0-b379-d810dede3786")
        output = self.graphson_writer.writeObject(prop)
        assert expected == output

    def test_bytebuffer(self):
        expected = json.dumps({'@type': 'gx:ByteBuffer', '@value': 'c29tZSBieXRlcyBmb3IgeW91'}, separators=(',', ':'))
        bb = ByteBufferType("c29tZSBieXRlcyBmb3IgeW91", "utf8")
        output = self.graphson_writer.writeObject(bb)
        assert expected == output

    def test_char(self):
        expected = json.dumps({'@type': 'gx:Char', '@value': 'L'}, separators=(',', ':'))
        c = str.__new__(SingleChar, chr(76))
        output = self.graphson_writer.writeObject(c)
        assert expected == output


class TestFunctionalGraphSONIO(object):
    """Functional IO tests"""

    def test_timestamp(self, remote_connection_v2):
        g = Graph().traversal().withRemote(remote_connection_v2)
        ts = timestamp(1481750076295 / 1000)
        resp = g.addV('test_vertex').property('ts', ts)
        resp = resp.toList()
        vid = resp[0].id
        try:
            ts_prop = g.V(vid).properties('ts').toList()[0]
            assert isinstance(ts_prop.value, timestamp)
            assert ts_prop.value == ts
        except OSError:
            assert False, "Error making request"
        finally:
            g.V(vid).drop().iterate()

    def test_datetime(self, remote_connection_v2):
        g = Graph().traversal().withRemote(remote_connection_v2)
        dt = datetime.datetime.utcfromtimestamp(1481750076295 / 1000)
        resp = g.addV('test_vertex').property('dt', dt).toList()
        vid = resp[0].id
        try:
            dt_prop = g.V(vid).properties('dt').toList()[0]
            assert isinstance(dt_prop.value, datetime.datetime)
            assert dt_prop.value == dt
        except OSError:
            assert False, "Error making request"
        finally:
            g.V(vid).drop().iterate()

    def test_uuid(self, remote_connection_v2):
        g = Graph().traversal().withRemote(remote_connection_v2)
        uid = uuid.UUID("41d2e28a-20a4-4ab0-b379-d810dede3786")
        resp = g.addV('test_vertex').property('uuid', uid).toList()
        vid = resp[0].id
        try:
            uid_prop = g.V(vid).properties('uuid').toList()[0]
            assert isinstance(uid_prop.value, uuid.UUID)
            assert uid_prop.value == uid
        except OSError:
            assert False, "Error making request"
        finally:
            g.V(vid).drop().iterate()
