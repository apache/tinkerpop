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
import uuid
from collections import OrderedDict
from gremlin_python.statics import short, long, bigint, BigDecimal, SingleByte, SingleChar
from gremlin_python.structure.graph import Graph, Vertex, Edge, Property, VertexProperty, Path, Tree, CompositePDT, PrimitivePDT
from gremlin_python.process.traversal import Direction, Merge, T

"""
The following models aren't supported.
max-offsetdatetime   Too large for datetime
min-offsetdatetime   Too small for datetime
forever-duration     Too large for duration
max-float            single precision float not supported in Python
min-float            single precision float not supported in Python
neg-max-float        single precision float not supported in Python
neg-min-float        single precision float not supported in Python
nan-float            single precision float not supported in Python
pos-inf-float        single precision float not supported in Python
neg-inf-float        single precision float not supported in Python
neg-zero-float       single precision float not supported in Python
"""

model = {}

model["pos-bigdecimal"] = BigDecimal(33, 123456789987654321123456789987654321)
model["neg-bigdecimal"] = BigDecimal(33, -123456789987654321123456789987654321)
model["zero-bigdecimal"] = BigDecimal(0, 0)
model["scale-zero-bigdecimal"] = BigDecimal(0, 1234)
model["negative-scale-bigdecimal"] = BigDecimal(-2, 1234)
model["small-decimal-bigdecimal"] = BigDecimal(2, 1234)
model["pos-biginteger"] = bigint(123456789987654321123456789987654321)
model["neg-biginteger"] = bigint(-123456789987654321123456789987654321)
model["zero-biginteger"] = bigint(0)
model["sign-boundary-pos-biginteger"] = bigint(128)
model["sign-boundary-neg-biginteger"] = bigint(-129)
model["uint8-primitive-pdt"] = PrimitivePDT("Uint8", "10")
model["point-composite-pdt"] = CompositePDT("Point", {"x": 1, "y": 2})
model["min-byte"] = SingleByte(-128)
model["max-byte"] = SingleByte(127)
model["empty-binary"] = bytes("", "utf8")
model["str-binary"] = bytes("some bytes for you", "utf8")
model["max-double"] = 1.7976931348623157E308
model["min-double"] = 4.9E-324
model["neg-max-double"] = -1.7976931348623157E308
model["neg-min-double"] = -4.9E-324
model["nan-double"] = float('nan')
model["pos-inf-double"] = float('inf')
model["neg-inf-double"] = float('-inf')
model["neg-zero-double"] = -0.0
model["single-byte-char"] = 'a'
model["two-byte-char"] = '\u03A9'
model["three-byte-char"] = SingleChar('\u20AC')
model["four-byte-char"] = SingleChar('\U0001F600')
model["unspecified-null"] = None
model["null-int"] = None
model["null-long"] = None
model["null-string"] = None
model["null-list"] = None
model["null-map"] = None
model["null-set"] = None
model["true-boolean"] = True
model["false-boolean"] = False
model["single-byte-string"] = "abc"
model["mixed-string"] = "abc\u0391\u0392\u0393"
model["empty-string"] = ""
model["var-bulklist"] = ["marko", "josh", "josh"]
model["empty-bulklist"] = []
model["zero-duration"] = datetime.timedelta()
model["positive-duration"] = datetime.timedelta(seconds=123)
model["negative-duration"] = datetime.timedelta(seconds=-123)
model["nanos-duration"] = datetime.timedelta(seconds=123, microseconds=456.789)
model["traversal-edge"] = Edge(
    13,
    Vertex(1, 'person'),
    "develops",
    Vertex(10, "software"),
    [Property("since", 2009, None)]
)
model["no-prop-edge"] = Edge(
    13,
    Vertex(1, 'person'),
    "develops",
    Vertex(10, "software")
)
model["max-int"] = 2147483647
model["min-int"] = -2**31
model["max-long"] = long(2**63-1)
model["min-long"] = long(-2**63)
model["var-type-list"] = [1, "person", True, None]
model["empty-list"] = []
model["var-type-map"] = {
    "test": 123,
    datetime.datetime(1970, 1, 1, 0, 24, 41, 295000, tzinfo=datetime.timezone.utc): "red",
    (1,2,3): datetime.datetime(1970, 1, 1, 0, 24, 41, 295000, tzinfo=datetime.timezone.utc),
    None: None
}
model["empty-map"] = {}
model["ordered-string-int-map"] = OrderedDict([
    ("delta", 4),
    ("alpha", 1),
    ("charlie", 3),
    ("bravo", 2),
    ("echo", 5),
    ("foxtrot", 6)
])
model["traversal-path"] = Path(
    [set(), set(), set()],
    [Vertex(1, "person"), Vertex(10, "software"), Vertex(11, "software")]
)
model["empty-path"] = Path([], [])
model["path-zero-labels"] = Path([set()], ["marko"])
model["path-multiple-labels"] = Path([{"a", "b"}], ["marko"])

# tree for g.V(10).out().tree(): v[10] -> v[11]
_traversal_tree = Tree()
_traversal_tree.get_or_create_child(Vertex(10, "software")).get_or_create_child(Vertex(11, "software"))
model["traversal-tree"] = _traversal_tree
model["empty-tree"] = Tree()
_tree_null_key = Tree()
_tree_null_key.get_or_create_child(None)
model["tree-null-key"] = _tree_null_key
_tree_mixed_key_types = Tree()
_tree_mixed_key_types.get_or_create_child("name")
_tree_mixed_key_types.get_or_create_child(123)
model["tree-mixed-key-types"] = _tree_mixed_key_types
_tree_deep_nesting = Tree()
_tree_deep_nesting.get_or_create_child("root").get_or_create_child("branch").get_or_create_child("leaf")
model["tree-deep-nesting"] = _tree_deep_nesting

model["prop-path"] = Path(
    [set(), set(), set()],
    [
        Vertex(1, "person", VertexProperty(
            123,
            "name",
            "stephen",
            [
                VertexProperty(0, "name", "marko", None),
                VertexProperty(6, "location", [], None)
            ]
        )),
        Vertex(10, "software"),
        Vertex(11, "software")
    ]
)
model["edge-property"] = Property("since", 2009, None)
model["null-property"] = Property("", None, None)
model["var-type-set"] = {2, "person", True, None}
model["empty-set"] = set()
model["max-short"] = short(32767)
model["min-short"] = short(-32768)
model["specified-uuid"] = uuid.UUID("41d2e28a-20a4-4ab0-b379-d810dede3786")
model["nil-uuid"] = uuid.UUID(int = 0)
model["no-prop-vertex"] = Vertex(1, "person")
model["traversal-vertexproperty"] = VertexProperty(0, "name", "marko", None)
model["meta-vertexproperty"] = VertexProperty(1, "person", "stephen", None, [Property("a", "b", None)])
model["set-cardinality-vertexproperty"] = VertexProperty(1, "person", {"stephen", "marko"}, None, [Property("a", "b", None)])
model["id-t"] = T.id
model["out-direction"] = Direction.OUT
model["merge-on-create"] = Merge.on_create
model["merge-on-match"] = Merge.on_match
model["merge-out-v"] = Merge.out_v
model["merge-in-v"] = Merge.in_v

santa_fe = VertexProperty(
    9, "location", "santa fe", None,
    [Property("startTime", 2005, None)]
)
brussels = VertexProperty(
    8, "location", "brussels", None,
    [Property("startTime", 2005, None), Property("endTime", 2005, None)]
)
santa_cruz = VertexProperty(
    7, "location", "santa cruz", None,
    [Property("startTime", 2001, None), Property("endTime", 2004, None)]
)
san_diego = VertexProperty(
    6, "location", "san diego", None,
    [Property("startTime", 1997, None), Property("endTime", 2001, None)]
)
name = VertexProperty(0, "name", "marko", None)

model["traversal-vertex"] = Vertex(1, "person", [
    Property("name", name, None), Property("location", [
        san_diego, santa_cruz, brussels, santa_fe
    ], None)])
model["multi-label-vertex"] = Vertex(1, labels=["person", "employee"])
model["empty-label-vertex"] = Vertex(1, labels=[])


class _CrewGraphFactory:
    @staticmethod
    def vertex_property(id, label, value, vertex, meta_properties=None):
        vp = VertexProperty(id, label, value, vertex)
        for key, meta_value in meta_properties or []:
            vp.properties.append(Property(key, meta_value, vp))
        return vp

    @staticmethod
    def add_vertex(graph, id, label, property_specs):
        vertex = Vertex(id, label)
        for spec in property_specs:
            vertex.properties.append(
                _CrewGraphFactory.vertex_property(
                    spec[0], spec[1], spec[2], vertex, spec[3] if len(spec) > 3 else []))
        graph.vertices[id] = vertex
        return vertex

    @staticmethod
    def add_edge(graph, id, out_v, label, in_v, properties=None):
        edge = Edge(id, out_v, label, in_v)
        for key, value in properties or []:
            edge.properties.append(Property(key, value, edge))
        graph.edges[id] = edge
        return edge

    @staticmethod
    def create():
        graph = Graph()
        v1 = _CrewGraphFactory.add_vertex(graph, 1, "person", [
            (0, "name", "marko"),
            (6, "location", "san diego", [("startTime", 1997), ("endTime", 2001)]),
            (7, "location", "santa cruz", [("startTime", 2001), ("endTime", 2004)]),
            (8, "location", "brussels", [("startTime", 2004), ("endTime", 2005)]),
            (9, "location", "santa fe", [("startTime", 2005)])
        ])
        v7 = _CrewGraphFactory.add_vertex(graph, 7, "person", [
            (1, "name", "stephen"),
            (10, "location", "centreville", [("startTime", 1990), ("endTime", 2000)]),
            (11, "location", "dulles", [("startTime", 2000), ("endTime", 2006)]),
            (12, "location", "purcellville", [("startTime", 2006)])
        ])
        v8 = _CrewGraphFactory.add_vertex(graph, 8, "person", [
            (2, "name", "matthias"),
            (13, "location", "bremen", [("startTime", 2004), ("endTime", 2007)]),
            (14, "location", "baltimore", [("startTime", 2007), ("endTime", 2011)]),
            (15, "location", "oakland", [("startTime", 2011), ("endTime", 2014)]),
            (16, "location", "seattle", [("startTime", 2014)])
        ])
        v9 = _CrewGraphFactory.add_vertex(graph, 9, "person", [
            (3, "name", "daniel"),
            (17, "location", "spremberg", [("startTime", 1982), ("endTime", 2005)]),
            (18, "location", "kaiserslautern", [("startTime", 2005), ("endTime", 2009)]),
            (19, "location", "aachen", [("startTime", 2009)])
        ])
        v10 = _CrewGraphFactory.add_vertex(graph, 10, "software", [(4, "name", "gremlin")])
        v11 = _CrewGraphFactory.add_vertex(graph, 11, "software", [(5, "name", "tinkergraph")])
        _CrewGraphFactory.add_edge(graph, 13, v1, "develops", v10, [("since", 2009)])
        _CrewGraphFactory.add_edge(graph, 14, v1, "develops", v11, [("since", 2010)])
        _CrewGraphFactory.add_edge(graph, 15, v1, "uses", v10, [("skill", 4)])
        _CrewGraphFactory.add_edge(graph, 16, v1, "uses", v11, [("skill", 5)])
        _CrewGraphFactory.add_edge(graph, 17, v7, "develops", v10, [("since", 2010)])
        _CrewGraphFactory.add_edge(graph, 18, v7, "develops", v11, [("since", 2011)])
        _CrewGraphFactory.add_edge(graph, 19, v7, "uses", v10, [("skill", 5)])
        _CrewGraphFactory.add_edge(graph, 20, v7, "uses", v11, [("skill", 4)])
        _CrewGraphFactory.add_edge(graph, 21, v8, "develops", v10, [("since", 2012)])
        _CrewGraphFactory.add_edge(graph, 22, v8, "uses", v10, [("skill", 3)])
        _CrewGraphFactory.add_edge(graph, 23, v8, "uses", v11, [("skill", 3)])
        _CrewGraphFactory.add_edge(graph, 24, v9, "uses", v10, [("skill", 5)])
        _CrewGraphFactory.add_edge(graph, 25, v9, "uses", v11, [("skill", 3)])
        _CrewGraphFactory.add_edge(graph, 26, v10, "traverses", v11)
        return graph


model["tinker-graph"] = _CrewGraphFactory.create()
