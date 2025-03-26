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
from gremlin_python.structure.graph import Vertex, Edge, Property, VertexProperty, Path
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
