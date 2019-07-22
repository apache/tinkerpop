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
import time
import uuid
import math
from decimal import *

from mock import Mock

import six

from gremlin_python.statics import *
from gremlin_python.structure.graph import Vertex, Edge, Property, VertexProperty, Graph, Path
from gremlin_python.structure.io.graphbinaryV1 import GraphBinaryWriter, GraphBinaryReader, DataType
from gremlin_python.process.traversal import P
from gremlin_python.process.strategies import SubgraphStrategy
from gremlin_python.process.graph_traversal import __


class TestGraphBinaryReader(object):
    graphbinary_reader = GraphBinaryReader()


class TestGraphSONWriter(object):
    graphbinary_writer = GraphBinaryWriter()
    graphbinary_reader = GraphBinaryReader()

    def test_string(self):
        x = "serialize this!"
        output = self.graphbinary_reader.readObject(self.graphbinary_writer.writeObject(x))
        assert x == output
