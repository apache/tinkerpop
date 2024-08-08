import datetime
import uuid
from gremlin_python.statics import timestamp, short, long, bigint, BigDecimal, SingleByte, SingleChar, ByteBufferType
from gremlin_python.structure.graph import Vertex, Edge, Property, VertexProperty, Path
from gremlin_python.structure.io.graphbinaryV4 import GraphBinaryWriter, GraphBinaryReader
from gremlin_python.process.traversal import Barrier, Binding, Bytecode, Merge, Direction, Traverser, T
from gremlin_python.structure.io.util import HashableDict, Marker

unsupported = {"tinker-graph", "max-offsetdatetime", "min-offsetdatetime"}
model = {}

model["pos-bigdecimal"] = BigDecimal(33, 123456789987654321123456789987654321)
model["neg-bigdecimal"] = BigDecimal(33, -123456789987654321123456789987654321)
model["pos-biginteger"] = bigint(123456789987654321123456789987654321)
model["neg-biginteger"] = bigint(-123456789987654321123456789987654321)
model["min-byte"] = SingleByte(-128)
model["max-byte"] = SingleByte(127)
model["empty-binary"] = ByteBufferType("", "utf8")
model["str-binary"] = ByteBufferType("some bytes for you", "utf8")
model["max-double"] = 1.7976931348623157E308
model["min-double"] = 4.9E-324
model["neg-max-double"] = -1.7976931348623157E308
model["neg-min-double"] = -4.9E-324
model["nan-double"] = float('nan')
model["pos-inf-double"] = float('inf')
model["neg-inf-double"] = float('-inf')
model["neg-zero-double"] = -0.0
model["zero-duration"] = datetime.timedelta()
#model["forever-duration"] = datetime.timedelta(seconds=2**63-1, microseconds=999999999)
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
model["max-float"] = float(3.4028235E38)
model["min-float"] = float(1.4E-45)
model["neg-max-float"] = float(-3.4028235E38)
model["neg-min-float"] = float(-1.4E-45)
model["nan-float"] = float('nan')
model["pos-inf-float"] = float('inf')
model["neg-inf-float"] = float('-inf')
model["neg-zero-float"] = float(-0.0)
model["max-int"] = 2147483647
model["min-int"] = -2**31
model["max-long"] = long(2**63-1)
model["min-long"] = long(-2**63)
model["var-type-list"] = [1, "person", True, None]
model["empty-list"] = []
model["var-type-map"] = {
    "test": 123,
    datetime.datetime.fromtimestamp(1481295): "red",
    (1,2,3): datetime.datetime.fromtimestamp(1481295),
    None: None
}
model["empty-map"] = {}
model["traversal-path"] = Path(
    [{}, {}, {}],
    [Vertex(1, "person"), Vertex(10, "software"), Vertex(11, "software")]
)
model["empty-path"] = Path([], [])
model["prop-path"] = Path(
    [{}, {}, {}],
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
model["var-type-set"] = {1, "person", True, None}
model["empty-set"] = set()
model["max-short"] = short(32767)
model["min-short"] = short(-32768)
# model["vertex-traverser"]
model["bulked-traverser"] = Traverser(Vertex(11, "software", [VertexProperty(5, "name", "tinkergraph", None)]), 7)
model["empty-traverser"] = Traverser(None, 0)
model["specified-uuid"] = uuid.UUID("41d2e28a-20a4-4ab0-b379-d810dede3786")
model["nil-uuid"] = uuid.UUID(int = 0)
#model["traversal-vertex"]
model["no-prop-vertex"] = Vertex(1, "person")
model["id-t"] = T.id
model["out-direction"] = Direction.OUT
#model["zero-date"] = datetime.datetime(1970, 1, 1)