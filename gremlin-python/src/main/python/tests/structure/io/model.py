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
from gremlin_python.statics import short, long, bigint, BigDecimal, SingleByte
from gremlin_python.structure.graph import Vertex, Edge, Property, VertexProperty, Path
from gremlin_python.process.traversal import Direction, Traverser, T

"""
The following models aren't supported.
tinker-graph         Graph type not implemented
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
traversal-tree       Tree type not implemented
"""

model = {}

model["pos-bigdecimal"] = BigDecimal(33, 123456789987654321123456789987654321)
model["neg-bigdecimal"] = BigDecimal(33, -123456789987654321123456789987654321)
model["pos-biginteger"] = bigint(123456789987654321123456789987654321)
model["neg-biginteger"] = bigint(-123456789987654321123456789987654321)
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
model["multi-byte-char"] = '\u03A9'
model["unspecified-null"] = None
model["true-boolean"] = True
model["false-boolean"] = False
model["single-byte-string"] = "abc"
model["mixed-string"] = "abc\u0391\u0392\u0393"
model["var-bulklist"] = ["marko", "josh", "josh"]
model["empty-bulklist"] = []
model["zero-duration"] = datetime.timedelta()
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
model["traversal-path"] = Path(
    [set(), set(), set()],
    [Vertex(1, "person"), Vertex(10, "software"), Vertex(11, "software")]
)
model["empty-path"] = Path([], [])
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
model["vertex-traverser"] = Traverser(model["traversal-vertex"], 1)
