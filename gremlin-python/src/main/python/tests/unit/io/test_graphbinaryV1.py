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

from datetime import datetime, timedelta, timezone
from gremlin_python.statics import long, bigint, BigDecimal, SingleByte, SingleChar, ByteBufferType, timestamp
from gremlin_python.structure.graph import Vertex, Edge, Property, VertexProperty, Path
from gremlin_python.structure.io.graphbinaryV1 import GraphBinaryWriter, GraphBinaryReader
from gremlin_python.process.traversal import Barrier, Binding, Bytecode, Merge, Direction


class TestGraphBinaryReader(object):
    graphbinary_reader = GraphBinaryReader()


class TestGraphBinaryWriter(object):
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
        from gremlin_python.structure.io.graphbinaryV1 import BigIntIO
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

    def test_date(self):
        x = datetime(2016, 12, 14, 16, 14, 36, 295000)
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output

    def test_timestamp(self):
        x = timestamp(1481750076295 / 1000)
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output

    def test_offsetdatetime(self):
        tz = timezone(timedelta(seconds=36000))
        ms = 12345678912
        x = datetime(2022, 5, 20, tzinfo=tz) + timedelta(microseconds=ms)
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output

    def test_offsetdatetime_format(self):
        x = datetime.strptime('2022-05-20T03:25:45.678912Z', '%Y-%m-%dT%H:%M:%S.%f%z')
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output

    def test_offsetdatetime_local(self):
        x = datetime.now().astimezone()
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output

    def test_offsetdatetime_epoch(self):
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

    def test_vertexproperty(self):
        x = VertexProperty(123, "name", "stephen", None)
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output
        
    def test_barrier(self):
        x = Barrier.norm_sack
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output

    def test_merge(self):
        x = Merge.on_match
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output

    def test_direction(self):
        x = Direction.OUT
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output

        x = Direction.from_
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output

    def test_binding(self):
        x = Binding("name", "marko")
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output

    def test_bytecode(self):
        x = Bytecode()
        x.source_instructions.append(["withStrategies", "SubgraphStrategy"])
        x.step_instructions.append(["V", 1, 2, 3])
        x.step_instructions.append(["out"])
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output

    def test_byte(self):
        x = int.__new__(SingleByte, 1)
        output = self.graphbinary_reader.read_object(self.graphbinary_writer.write_object(x))
        assert x == output

    def test_bytebuffer(self):
        x = ByteBufferType("c29tZSBieXRlcyBmb3IgeW91", "utf8")
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
