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

import datetime
import calendar
import time
import uuid
import math
from decimal import *

from mock import Mock

from gremlin_python.statics import timestamp, long, SingleByte, SingleChar, ByteBufferType
from gremlin_python.structure.graph import Vertex, Edge, Property, VertexProperty, Graph, Path
from gremlin_python.structure.io.graphbinaryV1 import GraphBinaryWriter, GraphBinaryReader, DataType
from gremlin_python.process.traversal import P, Barrier, Binding, Bytecode
from gremlin_python.process.strategies import SubgraphStrategy
from gremlin_python.process.graph_traversal import __


class TestGraphBinaryReader(object):
    graphbinary_reader = GraphBinaryReader()


class TestGraphSONWriter(object):
    graphbinary_writer = GraphBinaryWriter()
    graphbinary_reader = GraphBinaryReader()

    def test_null(self):
        c = self.graphbinary_reader.readObject(self.graphbinary_writer.writeObject(None))
        assert c is None

    def test_int(self):
        x = 100
        output = self.graphbinary_reader.readObject(self.graphbinary_writer.writeObject(x))
        assert x == output

    def test_long(self):
        x = long(100)
        output = self.graphbinary_reader.readObject(self.graphbinary_writer.writeObject(x))
        assert x == output

    def test_float(self):
        x = float(100.001)
        output = self.graphbinary_reader.readObject(self.graphbinary_writer.writeObject(x))
        assert x == output

        x = float('nan')
        output = self.graphbinary_reader.readObject(self.graphbinary_writer.writeObject(x))
        assert math.isnan(output)

        x = float('-inf')
        output = self.graphbinary_reader.readObject(self.graphbinary_writer.writeObject(x))
        assert math.isinf(output) and output < 0

        x = float('inf')
        output = self.graphbinary_reader.readObject(self.graphbinary_writer.writeObject(x))
        assert math.isinf(output) and output > 0

    def test_double(self):
        x = 100.001
        output = self.graphbinary_reader.readObject(self.graphbinary_writer.writeObject(x))
        assert x == output

    def test_date(self):
        x = datetime.datetime(2016, 12, 14, 16, 14, 36, 295000)
        output = self.graphbinary_reader.readObject(self.graphbinary_writer.writeObject(x))
        assert x == output

    def test_timestamp(self):
        x = timestamp(1481750076295 / 1000)
        output = self.graphbinary_reader.readObject(self.graphbinary_writer.writeObject(x))
        assert x == output

    def test_string(self):
        x = "serialize this!"
        output = self.graphbinary_reader.readObject(self.graphbinary_writer.writeObject(x))
        assert x == output

    def test_homogeneous_list(self):
        x = ["serialize this!", "serialize that!", "serialize that!","stop telling me what to serialize"]
        output = self.graphbinary_reader.readObject(self.graphbinary_writer.writeObject(x))
        assert x == output

    def test_heterogeneous_list(self):
        x = ["serialize this!", 0, "serialize that!", "serialize that!", 1, "stop telling me what to serialize", 2]
        output = self.graphbinary_reader.readObject(self.graphbinary_writer.writeObject(x))
        assert x == output

    def test_heterogeneous_list_with_none(self):
        x = ["serialize this!", 0, "serialize that!", "serialize that!", 1, "stop telling me what to serialize", 2, None]
        output = self.graphbinary_reader.readObject(self.graphbinary_writer.writeObject(x))
        assert x == output

    def test_homogeneous_set(self):
        x = {"serialize this!", "serialize that!", "stop telling me what to serialize"}
        output = self.graphbinary_reader.readObject(self.graphbinary_writer.writeObject(x))
        assert x == output

    def test_heterogeneous_set(self):
        x = {"serialize this!", 0, "serialize that!", 1, "stop telling me what to serialize", 2}
        output = self.graphbinary_reader.readObject(self.graphbinary_writer.writeObject(x))
        assert x == output

    def test_dict(self):
        x = {"yo": "what?",
             "go": "no!",
             "number": 123,
             321: "crazy with the number for a key",
             987: ["go", "deep", {"here": "!"}]}
        output = self.graphbinary_reader.readObject(self.graphbinary_writer.writeObject(x))
        assert x == output

        x = {"marko": [666], "noone": ["blah"]}
        output = self.graphbinary_reader.readObject(self.graphbinary_writer.writeObject(x))
        assert x == output

        x = {"ripple": [], "peter": ["created"], "noone": ["blah"], "vadas": [],
             "josh": ["created", "created"], "lop": [], "marko": [666, "created", "knows", "knows"]}
        output = self.graphbinary_reader.readObject(self.graphbinary_writer.writeObject(x))
        assert x == output

    def test_uuid(self):
        x = uuid.UUID("41d2e28a-20a4-4ab0-b379-d810dede3786")
        output = self.graphbinary_reader.readObject(self.graphbinary_writer.writeObject(x))
        assert x == output

    def test_edge(self):
        x = Edge(123, Vertex(1, 'person'), "developed", Vertex(10, "software"))
        output = self.graphbinary_reader.readObject(self.graphbinary_writer.writeObject(x))
        assert x == output
        assert x.inV == output.inV
        assert x.outV == output.outV

    def test_path(self):
        x = Path(["x", "y", "z"], [1, 2, 3])
        output = self.graphbinary_reader.readObject(self.graphbinary_writer.writeObject(x))
        assert x == output

    def test_property(self):
        x = Property("name", "stephen", None)
        output = self.graphbinary_reader.readObject(self.graphbinary_writer.writeObject(x))
        assert x == output

    def test_vertex(self):
        x = Vertex(123, "person")
        output = self.graphbinary_reader.readObject(self.graphbinary_writer.writeObject(x))
        assert x == output

    def test_vertexproperty(self):
        x = VertexProperty(123, "name", "stephen", None)
        output = self.graphbinary_reader.readObject(self.graphbinary_writer.writeObject(x))
        assert x == output
        
    def test_barrier(self):
        x = Barrier.normSack
        output = self.graphbinary_reader.readObject(self.graphbinary_writer.writeObject(x))
        assert x == output

    def test_binding(self):
        x = Binding("name", "marko")
        output = self.graphbinary_reader.readObject(self.graphbinary_writer.writeObject(x))
        assert x == output

    def test_bytecode(self):
        x = Bytecode()
        x.source_instructions.append(["withStrategies", "SubgraphStrategy"])
        x.step_instructions.append(["V", 1, 2, 3])
        x.step_instructions.append(["out"])
        output = self.graphbinary_reader.readObject(self.graphbinary_writer.writeObject(x))
        assert x == output

    def test_byte(self):
        x = int.__new__(SingleByte, 1)
        output = self.graphbinary_reader.readObject(self.graphbinary_writer.writeObject(x))
        assert x == output

    def test_bytebuffer(self):
        x = ByteBufferType("c29tZSBieXRlcyBmb3IgeW91", "utf8")
        output = self.graphbinary_reader.readObject(self.graphbinary_writer.writeObject(x))
        assert x == output

    def test_boolean(self):
        x = True
        output = self.graphbinary_reader.readObject(self.graphbinary_writer.writeObject(x))
        assert x == output

        x = False
        output = self.graphbinary_reader.readObject(self.graphbinary_writer.writeObject(x))
        assert x == output

    def test_char(self):
        x = str.__new__(SingleChar, chr(76))
        output = self.graphbinary_reader.readObject(self.graphbinary_writer.writeObject(x))
        assert x == output

        x = str.__new__(SingleChar, chr(57344))
        output = self.graphbinary_reader.readObject(self.graphbinary_writer.writeObject(x))
        assert x == output

    def test_duration(self):
        x = datetime.timedelta(seconds=1000, microseconds=1000)
        output = self.graphbinary_reader.readObject(self.graphbinary_writer.writeObject(x))
        assert x == output
        
