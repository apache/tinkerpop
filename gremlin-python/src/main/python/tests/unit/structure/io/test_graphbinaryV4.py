"""
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
"""

import uuid
import math
from collections import OrderedDict

from datetime import datetime, timedelta, timezone
from gremlin_python.statics import long, bigint, BigDecimal, SingleByte, SingleChar
from gremlin_python.structure.graph import Graph, Vertex, Edge, Property, VertexProperty, Path, CompositePDT
from gremlin_python.structure.io.graphbinaryV4 import GraphBinaryWriter, GraphBinaryReader
from gremlin_python.process.traversal import Direction
from gremlin_python.structure.io.util import Marker


class TestGraphBinaryV4(object):
    graphbinary_writer = GraphBinaryWriter()
    graphbinary_reader = GraphBinaryReader()

    def test_null(self):
        c = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(None))
        assert c is None

    def test_int(self):
        x = 100
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output

    def test_long(self):
        x = long(100)
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output

    def test_bigint(self):
        x = bigint(0x1000_0000_0000_0000_0000)
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output

    def test_bigint_boundaries(self):
        # Boundary values around signed byte-width transitions, including
        # negative boundaries that previously raised OverflowError (TINKERPOP-3275).
        # One representative per equivalence class of the length formula: zero,
        # positive byte-width boundary (127/128), negative power-of-two (-128),
        # negative just past a boundary (-129/-255, previously OverflowError), the
        # obj+1==0 edge (-1), and arbitrary-precision values of both signs.
        values = [0, 1, -1, 127, 128, -128, -129, -255,
                  0x1000_0000_0000_0000_0000, -0x1000_0000_0000_0000_0000]
        for v in values:
            x = bigint(v)
            output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
            assert x == output

    def test_bigint_wire_bytes(self):
        # Lock in exact minimal signed two's-complement bytes, matching the Java
        # reference BigInteger.toByteArray() (guards against non-minimal encoding).
        from gremlin_python.structure.io.graphbinaryV4 import BigIntIO
        cases = {
            0: b'\x00\x00\x00\x01\x00',
            127: b'\x00\x00\x00\x01\x7f',
            128: b'\x00\x00\x00\x02\x00\x80',
            -128: b'\x00\x00\x00\x01\x80',
            -129: b'\x00\x00\x00\x02\xff\x7f',
            -256: b'\x00\x00\x00\x02\xff\x00',
        }
        for value, expected in cases.items():
            assert bytes(BigIntIO.write_bigint(value, bytearray())) == expected

    def test_float(self):
        x = float(100.001)
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output

        x = float('nan')
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert math.isnan(output)

        x = float('-inf')
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert math.isinf(output) and output < 0

        x = float('inf')
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert math.isinf(output) and output > 0

    def test_double(self):
        x = 100.001
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output

    def test_bigdecimal(self):
        x = BigDecimal(100, 234)
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x.scale == output.scale
        assert x.unscaled_value == output.unscaled_value

    def test_bigdecimal_negative_boundaries(self):
        # Negative unscaled values at byte-width boundaries previously raised
        # OverflowError during serialization (TINKERPOP-3275).
        for x in [BigDecimal(0, -129), BigDecimal(2, -255)]:
            output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
            assert x.scale == output.scale
            assert x.unscaled_value == output.unscaled_value

    def test_datetime(self):
        tz = timezone(timedelta(seconds=36000))
        ms = 12345678912
        x = datetime(2022, 5, 20, tzinfo=tz) + timedelta(microseconds=ms)
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output

    def test_datetime_format(self):
        x = datetime.strptime('2022-05-20T03:25:45.678912Z', '%Y-%m-%dT%H:%M:%S.%f%z')
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output

    def test_datetime_local(self):
        x = datetime.now().astimezone()
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output

    def test_datetime_epoch(self):
        x = datetime.fromtimestamp(1690934400).astimezone(timezone.utc)
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output

    def test_string(self):
        x = "serialize this!"
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output

    def test_homogeneous_list(self):
        x = ["serialize this!", "serialize that!", "serialize that!","stop telling me what to serialize"]
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output

    def test_heterogeneous_list(self):
        x = ["serialize this!", 0, "serialize that!", "serialize that!", 1, "stop telling me what to serialize", 2]
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output

    def test_heterogeneous_list_with_none(self):
        x = ["serialize this!", 0, "serialize that!", "serialize that!", 1, "stop telling me what to serialize", 2, None]
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output

    def test_homogeneous_set(self):
        x = {"serialize this!", "serialize that!", "stop telling me what to serialize"}
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output

    def test_heterogeneous_set(self):
        x = {"serialize this!", 0, "serialize that!", 1, "stop telling me what to serialize", 2}
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output

    def test_set_with_unhashable_dict_elements(self):
        # test that sets containing dicts are coerced to list - see TINKERPOP-3232
        x = [{"name": "marko", "age": 29}, {"name": "josh", "age": 32}]
        list_payload = self.graphbinary_writer.write_object(x)
        # patch outer type from list (0x09) to set (0x0b)
        set_payload = bytearray(list_payload)
        set_payload[0] = 0x0b
        output = self.graphbinary_reader.read_object(set_payload)
        assert isinstance(output, list)
        assert len(output) == 2

    def test_set_with_unhashable_list_elements(self):
        # test that sets containing lists are coerced to list - see TINKERPOP-3232
        list_payload = self.graphbinary_writer.write_object([["marko", "josh"], ["vadas", "peter"]])
        # the first byte is the DataType for list (0x09), change it to set (0x0b)
        set_payload = bytearray(list_payload)
        set_payload[0] = 0x0b
        output = self.graphbinary_reader.read_object(set_payload)
        assert isinstance(output, list)
        assert len(output) == 2

    def test_set_with_mixed_hashable_and_unhashable_elements(self):
        # test that sets containing a mix of hashable and unhashable elements are coerced to list - see TINKERPOP-3232
        x = ["marko", {"name": "josh"}, 42]
        list_payload = self.graphbinary_writer.write_object(x)
        set_payload = bytearray(list_payload)
        set_payload[0] = 0x0b
        output = self.graphbinary_reader.read_object(set_payload)
        assert isinstance(output, list)
        assert len(output) == 3

    def test_set_with_nested_unhashable_elements(self):
        # test that sets containing dicts with list values are coerced to list - see TINKERPOP-3232
        x = [{"name": "marko", "langs": ["java", "python"]}, {"name": "josh", "langs": ["gremlin"]}]
        list_payload = self.graphbinary_writer.write_object(x)
        # patch outer type from list (0x09) to set (0x0b)
        set_payload = bytearray(list_payload)
        set_payload[0] = 0x0b
        output = self.graphbinary_reader.read_object(set_payload)
        assert isinstance(output, list)
        assert len(output) == 2

    def test_dict(self):
        x = {"yo": "what?",
             "go": "no!",
             "number": 123,
             321: "crazy with the number for a key",
             987: ["go", "deep", {"here": "!"}]}
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output

        x = {"marko": [666], "noone": ["blah"]}
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output

        x = {"ripple": [], "peter": ["created"], "noone": ["blah"], "vadas": [],
             "josh": ["created", "created"], "lop": [], "marko": [666, "created", "knows", "knows"]}
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output

    def test_ordered_dict(self):
        x = OrderedDict()
        x['a'] = 1
        x['b'] = 2
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output

    def test_uuid(self):
        x = uuid.UUID("41d2e28a-20a4-4ab0-b379-d810dede3786")
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output

    def test_edge(self):
        x = Edge(123, Vertex(1, 'person'), "developed", Vertex(10, "software"))
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output
        assert x.inV == output.inV
        assert x.outV == output.outV

    def test_edge_with_multi_labelled_endpoints(self):
        # the edge itself remains single-labelled, but its endpoint vertices may carry
        # multiple labels - see TINKERPOP-3261
        in_vertex = Vertex(1, labels=["person", "employee"])
        out_vertex = Vertex(10, labels=["software", "product"])
        x = Edge(123, out_vertex, "developed", in_vertex)
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output
        assert output.inV.labels == frozenset(["person", "employee"])
        assert output.outV.labels == frozenset(["software", "product"])

    def test_edge_with_zero_label_endpoints(self):
        # endpoint vertices may carry no labels at all under ZERO_OR_MORE cardinality - see TINKERPOP-3261
        in_vertex = Vertex(1, labels=[])
        out_vertex = Vertex(10, labels=[])
        x = Edge(123, out_vertex, "developed", in_vertex)
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output
        assert output.inV.labels == frozenset()
        assert output.outV.labels == frozenset()
        assert output.inV.label == ""
        assert output.outV.label == ""

    def test_path(self):
        x = Path(["x", "y", "z"], [1, 2, 3])
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output

    def test_property(self):
        x = Property("name", "stephen", None)
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output

    def test_vertex(self):
        x = Vertex(123, "person")
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output

    def test_vertex_with_multiple_labels(self):
        # vertices may carry multiple labels under ONE_OR_MORE/ZERO_OR_MORE cardinality - see TINKERPOP-3261
        x = Vertex(123, labels=["person", "employee"])
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output
        assert output.labels == frozenset(["person", "employee"])

    def test_vertex_with_no_labels(self):
        # vertices may carry no labels under ZERO_OR_MORE cardinality - see TINKERPOP-3261
        x = Vertex(123, labels=[])
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output
        assert output.labels == frozenset()
        assert output.label == ""

    def test_vertexproperty(self):
        x = VertexProperty(123, "name", "stephen", None)
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output

    def test_direction(self):
        x = Direction.OUT
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output

        x = Direction.from_
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output

    def test_byte(self):
        x = int.__new__(SingleByte, 1)
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output

    def test_binary(self):
        x = bytes("some bytes for you", "utf8")
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output

    def test_boolean(self):
        x = True
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output

        x = False
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output

    def test_char(self):
        x = str.__new__(SingleChar, chr(76))
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output

        x = str.__new__(SingleChar, chr(57344))
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output

    def test_duration(self):
        x = timedelta(seconds=1000, microseconds=1000)
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output

    def test_marker(self):
        x = Marker.end_of_stream()
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output

    def test_graph(self):
        graph = Graph()
        v1 = Vertex(1, "person")
        v2 = Vertex(2, "person")
        graph.vertices[1] = v1
        graph.vertices[2] = v2
        e1 = Edge(3, v1, "knows", v2)
        graph.edges[3] = e1

        # Add some properties
        vp1 = VertexProperty(4, "name", "marko", v1)
        v1.properties.append(vp1)
        vp1.properties.append(Property("acl", "public", vp1))

        e1.properties.append(Property("weight", 0.5, e1))

        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(graph))

        assert isinstance(output, Graph)
        assert len(output.vertices) == 2
        assert len(output.edges) == 1

        rv1 = output.vertices[1]
        assert rv1.label == "person"
        assert len(rv1.properties) == 1
        rvp1 = rv1.properties[0]
        assert rvp1.value == "marko"
        assert len(rvp1.properties) == 1
        assert rvp1.properties[0].key == "acl"
        assert rvp1.properties[0].value == "public"

        re1 = output.edges[3]
        assert re1.label == "knows"
        assert re1.outV.id == 1
        assert re1.inV.id == 2
        assert len(re1.properties) == 1
        assert re1.properties[0].key == "weight"
        assert re1.properties[0].value == 0.5

    def test_graph_with_multi_labelled_vertices(self):
        # a graph containing vertices with multiple labels - see TINKERPOP-3261
        graph = Graph()
        graph.vertices[1] = Vertex(1, labels=["person", "employee"])
        graph.vertices[2] = Vertex(2, labels=["software", "product"])
        graph.edges[3] = Edge(3, graph.vertices[1], "created", graph.vertices[2])

        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(graph))

        assert isinstance(output, Graph)
        assert output.vertices[1].labels == frozenset(["person", "employee"])
        assert output.vertices[2].labels == frozenset(["software", "product"])

    def test_graph_with_zero_labelled_vertices(self):
        # a graph containing vertices with no labels - see TINKERPOP-3261
        graph = Graph()
        graph.vertices[1] = Vertex(1, labels=[])
        graph.vertices[2] = Vertex(2, labels=[])
        graph.edges[3] = Edge(3, graph.vertices[1], "created", graph.vertices[2])

        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(graph))

        assert isinstance(output, Graph)
        assert output.vertices[1].labels == frozenset()
        assert output.vertices[1].label == ""
        assert output.vertices[2].labels == frozenset()
        assert output.vertices[2].label == ""

    def test_provider_defined_type(self):
        pdt = CompositePDT('Point', {'x': 1, 'y': 2})
        result = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(pdt))
        assert isinstance(result, CompositePDT)
        assert result.name == 'Point'
        assert result.fields == {'x': 1, 'y': 2}

    def test_provider_defined_type_nested(self):
        inner = CompositePDT('Address', {'street': 'Main'})
        outer = CompositePDT('Person', {'name': 'Alice', 'address': inner})
        result = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(outer))
        assert result.name == 'Person'
        assert result.fields['name'] == 'Alice'
        assert isinstance(result.fields['address'], CompositePDT)
        assert result.fields['address'].name == 'Address'

    def test_provider_defined_type_null_field(self):
        pdt = CompositePDT('NullableType', {'value': None, 'name': 'test'})
        result = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(pdt))
        assert result.fields['value'] is None
        assert result.fields['name'] == 'test'
