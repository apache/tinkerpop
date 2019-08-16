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

import six
import datetime
import calendar
import struct
import uuid
import math
import io
from collections import OrderedDict
import logging

from aenum import Enum
from gremlin_python import statics
from gremlin_python.statics import FloatType, FunctionType, IntType, LongType, TypeType, DictType, ListType, SetType, \
                                   SingleByte, ByteBufferType, GremlinType
from gremlin_python.process.traversal import Barrier, Binding, Bytecode, Cardinality, Column, Direction, Operator, \
                                             Order, Pick, Pop, P, Scope, TextP, Traversal, Traverser, \
                                             TraversalStrategy, T
from gremlin_python.process.graph_traversal import GraphTraversal
from gremlin_python.structure.graph import Graph, Edge, Property, Vertex, VertexProperty, Path

log = logging.getLogger(__name__)

# When we fall back to a superclass's serializer, we iterate over this map.
# We want that iteration order to be consistent, so we use an OrderedDict,
# not a dict.
_serializers = OrderedDict()
_deserializers = {}


class DataType(Enum):
    null = 0xfe
    int = 0x01
    long = 0x02
    string = 0x03
    date = 0x04
    timestamp = 0x05
    clazz = 0x06
    double = 0x07
    float = 0x08
    list = 0x09
    map = 0x0a
    set = 0x0b
    uuid = 0x0c
    edge = 0x0d
    path = 0x0e
    property = 0x0f
    graph = 0x10                  # no graph object in python yet
    vertex = 0x11
    vertexproperty = 0x12
    barrier = 0x13
    binding = 0x14
    bytecode = 0x15
    cardinality = 0x16
    column = 0x17
    direction = 0x18
    operator = 0x19
    order = 0x1a
    pick = 0x1b
    pop = 0x1c
    lambda_ = 0x1d
    p = 0x1e
    scope = 0x1f
    t = 0x20
    traverser = 0x21
    bigdecimal = 0x22             #todo
    biginteger = 0x23             #todo
    byte = 0x24
    bytebuffer = 0x25
    short = 0x26                  #todo?
    boolean = 0x27
    textp = 0x28
    traversalstrategy = 0x29
    bulkset = 0x2a
    tree = 0x2b                   # no tree object in Python yet
    metrics = 0x2c
    traversalmetrics = 0x2d
    custom = 0x00                 #todo


NULL_BYTES = [DataType.null.value, 0x01]


class GraphBinaryTypeType(type):
    def __new__(mcs, name, bases, dct):
        cls = super(GraphBinaryTypeType, mcs).__new__(mcs, name, bases, dct)
        if not name.startswith('_'):
            if cls.python_type:
                _serializers[cls.python_type] = cls
            if cls.graphbinary_type:
                _deserializers[cls.graphbinary_type] = cls
        return cls


class GraphBinaryWriter(object):
    def __init__(self, serializer_map=None):
        self.serializers = _serializers.copy()
        if serializer_map:
            self.serializers.update(serializer_map)

    def writeObject(self, objectData):
        return self.toDict(objectData)

    def toDict(self, obj):
        try:
            t = type(obj)
            
            # coerce unicode to str so the serializer will be found properly in the cache...better way?
            if not six.PY3:
                t = str if isinstance(obj, unicode) else t
            return self.serializers[t].dictify(obj, self)
        except KeyError:
            for key, serializer in self.serializers.items():
                if isinstance(obj, key):
                    return serializer.dictify(obj, self)

        if isinstance(obj, dict):
            return dict((self.toDict(k), self.toDict(v)) for k, v in obj.items())
        elif isinstance(obj, set):
            return set([self.toDict(o) for o in obj])
        elif isinstance(obj, list):
            return [self.toDict(o) for o in obj]
        else:
            return obj


class GraphBinaryReader(object):
    def __init__(self, deserializer_map=None):
        self.deserializers = _deserializers.copy()
        if deserializer_map:
            self.deserializers.update(deserializer_map)

    def readObject(self, b):
        if isinstance(b, bytearray):
            return self.toObject(io.BytesIO(b))
        elif isinstance(b, io.BufferedIOBase):
            return self.toObject(b)

    def toObject(self, buff, data_type=None, nullable=True):
        if data_type is None:
            bt = struct.unpack('>B', buff.read(1))[0]
            if bt == DataType.null.value:
                if nullable:
                    buff.read(1)
                return None
            return self.deserializers[DataType(bt)].objectify(buff, self, nullable)
        else:
            return self.deserializers[data_type].objectify(buff, self, nullable)


@six.add_metaclass(GraphBinaryTypeType)
class _GraphBinaryTypeIO(object):
    python_type = None
    graphbinary_type = None

    symbolMap = {"global_": "global", "as_": "as", "in_": "in", "and_": "and",
                 "or_": "or", "is_": "is", "not_": "not", "from_": "from",
                 "set_": "set", "list_": "list", "all_": "all", "with_": "with",
                 "filter_": "filter", "id_": "id", "max_": "max", "min_": "min", "sum_": "sum"}

    @classmethod
    def as_bytes(cls, graphbin_type, as_value=False, nullable=True, *args):
        ba = bytearray() if as_value else bytearray([graphbin_type.value])

        if nullable:
            ba.extend(struct.pack(">b", 0))

        for arg in args:
            if isinstance(arg, (bytes, bytearray)):
                ba.extend(arg)
            else:
                raise Exception("MISSING")
        return ba

    @classmethod
    def string_as_bytes(cls, s):
        ba = bytearray()
        ba.extend(struct.pack(">i", len(s)))
        ba.extend(s.encode("utf-8"))
        return ba

    @classmethod
    def read_int(cls, buff):
        return struct.unpack(">i", buff.read(4))[0]

    @classmethod
    def read_string(cls, buff):
        return buff.read(cls.read_int(buff)).decode("utf-8")

    @classmethod
    def unmangleKeyword(cls, symbol):
        return cls.symbolMap.get(symbol, symbol)
    
    @classmethod
    def write_as_value(cls, graphbin_type, as_value):
        return None if as_value else graphbin_type

    @classmethod
    def is_null(cls, buff, reader, else_opt, nullable=True):
        return None if nullable and buff.read(1)[0] == 0x01 else else_opt(buff, reader)

    def dictify(self, obj, writer, as_value=False, nullable=True):
        raise NotImplementedError()

    def objectify(self, d, reader, nullable=True):
        raise NotImplementedError()
        

class LongIO(_GraphBinaryTypeIO):

    python_type = LongType
    graphbinary_type = DataType.long
    byte_format = ">q"

    @classmethod
    def dictify(cls, obj, writer, as_value=False, nullable=True):
        if obj < -9223372036854775808 or obj > 9223372036854775807:
            raise Exception("TODO: don't forget bigint")
        else:
            return cls.as_bytes(cls.graphbinary_type, as_value, nullable, struct.pack(cls.byte_format, obj))

    @classmethod
    def objectify(cls, buff, reader, nullable=True):
        return cls.is_null(buff, reader, lambda b, r: struct.unpack(cls.byte_format, b.read(8))[0], nullable)


class IntIO(LongIO):

    python_type = IntType
    graphbinary_type = DataType.int
    byte_format = ">i"

    @classmethod
    def objectify(cls, buff, reader, nullable=True):
        return cls.is_null(buff, reader, lambda b, r: cls.read_int(b), nullable)


class DateIO(_GraphBinaryTypeIO):

    python_type = datetime.datetime
    graphbinary_type = DataType.date

    @classmethod
    def dictify(cls, obj, writer, as_value=False, nullable=True):
        try:
            timestamp_seconds = calendar.timegm(obj.utctimetuple())
            pts = timestamp_seconds * 1e3 + getattr(obj, 'microsecond', 0) / 1e3
        except AttributeError:
            pts = calendar.timegm(obj.timetuple()) * 1e3

        ts = int(round(pts))
        return cls.as_bytes(cls.graphbinary_type, as_value, nullable, struct.pack(">q", ts))

    @classmethod
    def objectify(cls, buff, reader, nullable=True):
        return cls.is_null(buff, reader,
                           lambda b, r: datetime.datetime.utcfromtimestamp(struct.unpack(">q", b.read(8))[0] / 1000.0),
                           nullable)


# Based on current implementation, this class must always be declared before FloatIO.
# Seems pretty fragile for future maintainers. Maybe look into this.
class TimestampIO(_GraphBinaryTypeIO):
    python_type = statics.timestamp
    graphbinary_type = DataType.timestamp

    @classmethod
    def dictify(cls, obj, writer, as_value=False, nullable=True):
        # Java timestamp expects milliseconds integer - Have to use int because of legacy Python
        ts = int(round(obj * 1000))
        return cls.as_bytes(cls.graphbinary_type, as_value, nullable, struct.pack(">q", ts))

    @classmethod
    def objectify(cls, buff, reader, nullable=True):
        # Python timestamp expects seconds
        return cls.is_null(buff, reader, lambda b, r: statics.timestamp(struct.unpack(">q", b.read(8))[0] / 1000.0),
                           nullable)
    

def _long_bits_to_double(bits):
    return struct.unpack('d', struct.pack('Q', bits))[0]


NAN = _long_bits_to_double(0x7ff8000000000000)
POSITIVE_INFINITY = _long_bits_to_double(0x7ff0000000000000)
NEGATIVE_INFINITY = _long_bits_to_double(0xFff0000000000000)


class FloatIO(LongIO):

    python_type = FloatType
    graphbinary_type = DataType.float
    graphbinary_base_type = DataType.float
    byte_format = ">f"

    @classmethod
    def dictify(cls, obj, writer, as_value=False, nullable=True):
        if math.isnan(obj):
            return cls.as_bytes(cls.graphbinary_type, as_value, nullable, struct.pack(cls.byte_format, NAN))
        elif math.isinf(obj) and obj > 0:
            return cls.as_bytes(cls.graphbinary_type, as_value, nullable,
                                struct.pack(cls.byte_format, POSITIVE_INFINITY))
        elif math.isinf(obj) and obj < 0:
            return cls.as_bytes(cls.graphbinary_type, as_value, nullable,
                                struct.pack(cls.byte_format, NEGATIVE_INFINITY))
        else:
            return cls.as_bytes(cls.graphbinary_type, as_value, nullable, struct.pack(cls.byte_format, obj))

    @classmethod
    def objectify(cls, buff, reader, nullable=True):
        return cls.is_null(buff, reader, lambda b, r: struct.unpack(cls.byte_format, b.read(4))[0], nullable)


class DoubleIO(FloatIO):
    """
    Floats basically just fall through to double serialization.
    """

    graphbinary_type = DataType.double
    graphbinary_base_type = DataType.double
    byte_format = ">d"

    @classmethod
    def objectify(cls, buff, reader, nullable=True):
        return cls.is_null(buff, reader, lambda b, r: struct.unpack(cls.byte_format, b.read(8))[0], nullable)


class StringIO(_GraphBinaryTypeIO):

    python_type = str
    graphbinary_type = DataType.string

    @classmethod
    def dictify(cls, obj, writer, as_value=False, nullable=True):
        ba = bytearray()
        ba.extend(struct.pack(">i", len(obj)))
        ba.extend(obj.encode("utf-8"))
        return cls.as_bytes(cls.graphbinary_type, as_value, nullable, ba)

    @classmethod
    def objectify(cls, buff, reader, nullable=True):
        return cls.is_null(buff, reader, lambda b, r: b.read(cls.read_int(b)).decode("utf-8"), nullable)


class ListIO(_GraphBinaryTypeIO):

    python_type = list
    graphbinary_type = DataType.list

    @classmethod
    def dictify(cls, obj, writer, as_value=False, nullable=True):
        list_data = bytearray()
        list_data.extend(struct.pack(">i", len(obj)))
        for item in obj:
            list_data.extend(writer.writeObject(item))

        return cls.as_bytes(cls.graphbinary_type, as_value, nullable, list_data)

    @classmethod
    def objectify(cls, buff, reader, nullable=True):
        return cls.is_null(buff, reader, cls._read_list, nullable)

    @classmethod
    def _read_list(cls, b, r):
        size = cls.read_int(b)
        the_list = []
        while size > 0:
            the_list.append(r.readObject(b))
            size = size - 1

        return the_list


class SetDeserializer(ListIO):

    python_type = SetType
    graphbinary_type = DataType.set

    @classmethod
    def objectify(cls, buff, reader, nullable=True):
        return set(ListIO.objectify(buff, reader, nullable))


class MapIO(_GraphBinaryTypeIO):

    python_type = DictType
    graphbinary_type = DataType.map

    @classmethod
    def dictify(cls, obj, writer, as_value=False, nullable=True):
        map_data = bytearray()
        map_data.extend(struct.pack(">i", len(obj)))
        for k, v in obj.items():
            map_data.extend(writer.writeObject(k))
            map_data.extend(writer.writeObject(v))

        return cls.as_bytes(cls.graphbinary_type, as_value, nullable, map_data)

    @classmethod
    def objectify(cls, buff, reader, nullable=True):
        return cls.is_null(buff, reader, cls._read_map, nullable)

    @classmethod
    def _read_map(cls, b, r):
        size = cls.read_int(b)
        the_dict = {}
        while size > 0:
            k = r.readObject(b)
            v = r.readObject(b)
            the_dict[k] = v
            size = size - 1

        return the_dict


class UuidIO(_GraphBinaryTypeIO):

    python_type = uuid.UUID
    graphbinary_type = DataType.uuid

    @classmethod
    def dictify(cls, obj, writer, as_value=False, nullable=True):
        return cls.as_bytes(cls.graphbinary_type, as_value, nullable, obj.bytes)

    @classmethod
    def objectify(cls, buff, reader, nullable=True):
        return cls.is_null(buff, reader, lambda b, r: uuid.UUID(bytes=b.read(16)), nullable)


class EdgeIO(_GraphBinaryTypeIO):

    python_type = Edge
    graphbinary_type = DataType.edge

    @classmethod
    def dictify(cls, obj, writer, as_value=False, nullable=True):
        ba = bytearray()
        ba.extend(writer.writeObject(obj.id))
        ba.extend(cls.string_as_bytes(obj.label))
        ba.extend(writer.writeObject(obj.inV.id))
        ba.extend(cls.string_as_bytes(obj.inV.label))
        ba.extend(writer.writeObject(obj.outV.id))
        ba.extend(cls.string_as_bytes(obj.outV.label))
        ba.extend(NULL_BYTES)
        ba.extend(NULL_BYTES)
        return cls.as_bytes(cls.graphbinary_type, as_value, nullable, ba)

    @classmethod
    def objectify(cls, buff, reader, nullable=True):
        return cls.is_null(buff, reader, cls._read_edge, nullable)

    @classmethod
    def _read_edge(cls, b, r):
        edgeid = r.readObject(b)
        edgelbl = cls.read_string(b)
        edge = Edge(edgeid, Vertex(r.readObject(b), cls.read_string(b)),
                    edgelbl, Vertex(r.readObject(b), cls.read_string(b)))
        b.read(4)
        return edge


class PathIO(_GraphBinaryTypeIO):

    python_type = Path
    graphbinary_type = DataType.path

    @classmethod
    def dictify(cls, obj, writer, as_value=False, nullable=True):
        ba = bytearray()
        ba.extend(writer.writeObject(obj.labels))
        ba.extend(writer.writeObject(obj.objects))
        return cls.as_bytes(cls.graphbinary_type, as_value, nullable, ba)

    @classmethod
    def objectify(cls, buff, reader, nullable=True):
        return cls.is_null(buff, reader, lambda b, r: Path(r.readObject(b), r.readObject(b)), nullable)


class PropertyIO(_GraphBinaryTypeIO):

    python_type = Property
    graphbinary_type = DataType.property

    @classmethod
    def dictify(cls, obj, writer, as_value=False, nullable=True):
        ba = bytearray()
        ba.extend(cls.string_as_bytes(obj.key))
        ba.extend(writer.writeObject(obj.value))
        ba.extend(NULL_BYTES)
        return cls.as_bytes(cls.graphbinary_type, as_value, nullable, ba)

    @classmethod
    def objectify(cls, buff, reader, nullable=True):
        return cls.is_null(buff, reader, cls._read_property, nullable)

    @classmethod
    def _read_property(cls, b, r):
        p = Property(cls.read_string(b), r.readObject(b), None)
        b.read(2)
        return p


class TinkerGraphIO(_GraphBinaryTypeIO):

    python_type = Graph
    graphbinary_type = DataType.graph

    @classmethod
    def dictify(cls, obj, writer, as_value=False, nullable=True):
        raise AttributeError("TinkerGraph serialization is not currently supported by gremlin-python")

    @classmethod
    def objectify(cls, b, reader, as_value=False):
        raise AttributeError("TinkerGraph deserialization is not currently supported by gremlin-python")


class VertexIO(_GraphBinaryTypeIO):

    python_type = Vertex
    graphbinary_type = DataType.vertex

    @classmethod
    def dictify(cls, obj, writer, as_value=False, nullable=True):
        ba = bytearray()
        ba.extend(writer.writeObject(obj.id))
        ba.extend(cls.string_as_bytes(obj.label))
        ba.extend(NULL_BYTES)
        return cls.as_bytes(cls.graphbinary_type, as_value, nullable, ba)

    @classmethod
    def objectify(cls, buff, reader, nullable=True):
        return cls.is_null(buff, reader, cls._read_vertex, nullable)

    @classmethod
    def _read_vertex(cls, b, r):
        vertex = Vertex(r.readObject(b), cls.read_string(b))
        b.read(2)
        return vertex


class VertexPropertyIO(_GraphBinaryTypeIO):

    python_type = VertexProperty
    graphbinary_type = DataType.vertexproperty

    @classmethod
    def dictify(cls, obj, writer, as_value=False, nullable=True):
        ba = bytearray()
        ba.extend(writer.writeObject(obj.id))
        ba.extend(cls.string_as_bytes(obj.label))
        ba.extend(writer.writeObject(obj.value))
        ba.extend(NULL_BYTES)
        ba.extend(NULL_BYTES)
        return cls.as_bytes(cls.graphbinary_type, as_value, nullable, ba)

    @classmethod
    def objectify(cls, buff, reader, nullable=True):
        return cls.is_null(buff, reader, cls._read_vertexproperty, nullable)

    @classmethod
    def _read_vertexproperty(cls, b, r):
        vp = VertexProperty(r.readObject(b), cls.read_string(b), r.readObject(b), None)
        b.read(4)
        return vp


class _EnumIO(_GraphBinaryTypeIO):

    @classmethod
    def dictify(cls, obj, writer, as_value=False, nullable=True):
        ba = bytearray()
        ba.extend(StringIO.dictify(cls.unmangleKeyword(str(obj.name)), writer))
        return cls.as_bytes(cls.graphbinary_type, as_value, nullable, ba)

    @classmethod
    def objectify(cls, buff, reader, nullable=True):
        return cls.is_null(buff, reader, cls._read_enumval, nullable)

    @classmethod
    def _read_enumval(cls, b, r):
        enum_name = r.toObject(b)
        return cls.python_type[enum_name]


class BarrierIO(_EnumIO):
    graphbinary_type = DataType.barrier
    python_type = Barrier


class CardinalityIO(_EnumIO):
    graphbinary_type = DataType.cardinality
    python_type = Cardinality


class ColumnIO(_EnumIO):
    graphbinary_type = DataType.column
    python_type = Column


class DirectionIO(_EnumIO):
    graphbinary_type = DataType.direction
    python_type = Direction


class OperatorIO(_EnumIO):
    graphbinary_type = DataType.operator
    python_type = Operator


class OrderIO(_EnumIO):
    graphbinary_type = DataType.order
    python_type = Order


class PickIO(_EnumIO):
    graphbinary_type = DataType.pick
    python_type = Pick


class PopIO(_EnumIO):
    graphbinary_type = DataType.pop
    python_type = Pop


class BindingIO(_GraphBinaryTypeIO):

    python_type = Binding
    graphbinary_type = DataType.binding

    @classmethod
    def dictify(cls, obj, writer, as_value=False, nullable=True):
        ba = bytearray()
        ba.extend(cls.string_as_bytes(obj.key))
        ba.extend(writer.writeObject(obj.value))
        return cls.as_bytes(cls.graphbinary_type, as_value, nullable, ba)

    @classmethod
    def objectify(cls, buff, reader, nullable=True):
        return cls.is_null(buff, reader, lambda b, r: Binding(cls.read_string(b), reader.readObject(b)), nullable)


class BytecodeIO(_GraphBinaryTypeIO):
    python_type = Bytecode
    graphbinary_type = DataType.bytecode

    @classmethod
    def dictify(cls, obj, writer, as_value=False, nullable=True):
        bc = obj.bytecode if isinstance(obj, Traversal) else obj
        ba = bytearray()
        ba.extend(struct.pack(">i", len(bc.step_instructions)))
        for inst in bc.step_instructions:
            inst_name, inst_args = inst[0], inst[1:] if len(inst) > 1 else []
            ba.extend(cls.string_as_bytes(inst_name))
            ba.extend(struct.pack(">i", len(inst_args)))
            for arg in inst_args:
                ba.extend(writer.writeObject(arg))

        ba.extend(struct.pack(">i", len(bc.source_instructions)))
        for inst in bc.source_instructions:
            inst_name, inst_args = inst[0], inst[1:] if len(inst) > 1 else []
            ba.extend(cls.string_as_bytes(inst_name))
            ba.extend(struct.pack(">i", len(inst_args)))
            for arg in inst_args:
                if isinstance(arg, TypeType):
                    ba.extend(writer.writeObject(GremlinType(arg().fqcn)))
                else:
                    ba.extend(writer.writeObject(arg))

        return cls.as_bytes(cls.graphbinary_type, as_value, nullable, ba)

    @classmethod
    def objectify(cls, buff, reader, nullable=True):
        return cls.is_null(buff, reader, cls._read_bytecode, nullable)

    @classmethod
    def _read_bytecode(cls, b, r):
        bytecode = Bytecode()

        step_count = cls.read_int(b)
        ix = 0
        while ix < step_count:
            inst = [cls.read_string(b)]
            inst_ct = cls.read_int(b)
            iy = 0
            while iy < inst_ct:
                inst.append(r.readObject(b))
                iy += 1
            bytecode.step_instructions.append(inst)
            ix += 1

        source_count = cls.read_int(b)
        ix = 0
        while ix < source_count:
            inst = [cls.read_string(b)]
            inst_ct = cls.read_int(b)
            iy = 0
            while iy < inst_ct:
                inst.append(r.readObject(b))
                iy += 1
            bytecode.source_instructions.append(inst)
            ix += 1

        return bytecode


class TraversalIO(BytecodeIO):
    python_type = GraphTraversal


class LambdaSerializer(_GraphBinaryTypeIO):

    python_type = FunctionType
    graphbinary_type = DataType.lambda_

    @classmethod
    def dictify(cls, obj, writer, as_value=False, nullable=True):
        ba = bytearray()
        lambda_result = obj()
        script = lambda_result if isinstance(lambda_result, str) else lambda_result[0]
        language = statics.default_lambda_language if isinstance(lambda_result, str) else lambda_result[1]

        ba.extend(StringIO.dictify(language, writer, True, False))

        script_cleaned = script
        script_args = -1

        if language == "gremlin-jython" or language == "gremlin-python":
            if not script.strip().startswith("lambda"):
                script_cleaned = "lambda " + script
            script_args = six.get_function_code(eval(script_cleaned)).co_argcount

        ba.extend(StringIO.dictify(script_cleaned, writer, True, False))
        ba.extend(struct.pack(">i", script_args))

        return cls.as_bytes(cls.graphbinary_type, as_value, nullable, ba)


class PSerializer(_GraphBinaryTypeIO):
    graphbinary_type = DataType.p
    python_type = P

    @classmethod
    def dictify(cls, obj, writer, as_value=False, nullable=True):
        ba = bytearray()
        ba.extend(cls.string_as_bytes(obj.operator))
        
        args = []
        if obj.other is None:
            if isinstance(obj.value, ListType):
                args = obj.value
            else:
                args.append(obj.value)
        else:
            args.append(obj.value)
            args.append(obj.other)
        
        ba.extend(struct.pack(">i", len(args)))
        for a in args:
            ba.extend(writer.writeObject(a))

        return cls.as_bytes(cls.graphbinary_type, as_value, nullable, ba)


class ScopeIO(_EnumIO):
    graphbinary_type = DataType.scope
    python_type = Scope


class TIO(_EnumIO):
    graphbinary_type = DataType.t
    python_type = T


class TraverserIO(_GraphBinaryTypeIO):
    graphbinary_type = DataType.traverser
    python_type = Traverser

    @classmethod
    def dictify(cls, obj, writer, as_value=False, nullable=True):
        ba = bytearray()
        ba.extend(struct.pack(">q", obj.bulk))
        ba.extend(writer.writeObject(obj.object))
        return cls.as_bytes(cls.graphbinary_type, as_value, nullable, ba)

    @classmethod
    def objectify(cls, buff, reader, nullable=True):
        return cls.is_null(buff, reader, cls._read_traverser, nullable)

    @classmethod
    def _read_traverser(cls, b, r):
        bulk = struct.unpack(">q", b.read(8))[0]
        obj = r.readObject(b)
        return Traverser(obj, bulk=bulk)


class ByteIO(_GraphBinaryTypeIO):
    python_type = SingleByte
    graphbinary_type = DataType.byte

    @classmethod
    def dictify(cls, obj, writer, as_value=False, nullable=True):
        return cls.as_bytes(cls.graphbinary_type, as_value, nullable, struct.pack(">b", obj))

    @classmethod
    def objectify(cls, buff, reader, nullable=True):
        return cls.is_null(buff, reader,
                           lambda b, r: int.__new__(SingleByte, struct.unpack_from(">b", b.read(1))[0]),
                           nullable)


class ByteBufferIO(_GraphBinaryTypeIO):
    python_type = ByteBufferType
    graphbinary_type = DataType.bytebuffer

    @classmethod
    def dictify(cls, obj, writer, as_value=False, nullable=True):
        ba = bytearray()
        ba.extend(struct.pack(">i", len(obj)))
        ba.extend(obj)
        return cls.as_bytes(cls.graphbinary_type, as_value, nullable, ba)

    @classmethod
    def objectify(cls, buff, reader, nullable=True):
        return cls.is_null(buff, reader, cls._read_bytebuffer, nullable)

    @classmethod
    def _read_bytebuffer(cls, b, r):
        size = cls.read_int(b)
        return ByteBufferType(b.read(size))


class BooleanIO(_GraphBinaryTypeIO):
    python_type = bool
    graphbinary_type = DataType.boolean

    @classmethod
    def dictify(cls, obj, writer, as_value=False, nullable=True):
        return cls.as_bytes(cls.graphbinary_type, as_value, nullable, struct.pack(">b", 0x01 if obj else 0x00))

    @classmethod
    def objectify(cls, buff, reader, nullable=True):
        return cls.is_null(buff, reader,
                           lambda b, r: True if struct.unpack_from(">b", b.read(1))[0] == 0x01 else False,
                           nullable)


class TextPSerializer(_GraphBinaryTypeIO):
    graphbinary_type = DataType.textp
    python_type = TextP

    @classmethod
    def dictify(cls, obj, writer, as_value=False, nullable=True):
        ba = bytearray()
        ba.extend(cls.string_as_bytes(obj.operator))

        args = []
        if obj.other is None:
            if isinstance(obj.value, ListType):
                args = obj.value
            else:
                args.append(obj.value)
        else:
            args.append(obj.value)
            args.append(obj.other)

        ba.extend(struct.pack(">i", len(args)))
        for a in args:
            ba.extend(writer.writeObject(a))

        return cls.as_bytes(cls.graphbinary_type, as_value, nullable, ba)


class BulkSetDeserializer(_GraphBinaryTypeIO):

    graphbinary_type = DataType.bulkset

    @classmethod
    def objectify(cls, buff, reader, nullable=True):
        return cls.is_null(buff, reader, cls._read_bulkset, nullable)

    @classmethod
    def _read_bulkset(cls, b, r):
        size = cls.read_int(b)
        the_list = []
        while size > 0:
            itm = r.readObject(b)
            bulk = struct.unpack(">q", b.read(8))[0]
            for y in range(bulk):
                the_list.append(itm)            
            size = size - 1

        return the_list


class MetricsDeserializer(_GraphBinaryTypeIO):

    graphbinary_type = DataType.metrics

    @classmethod
    def objectify(cls, buff, reader, nullable=True):
        return cls.is_null(buff, reader, cls._read_metrics, nullable)

    @classmethod
    def _read_metrics(cls, b, r):
        metricid = cls.read_string(b)
        name = cls.read_string(b)
        duration = r.toObject(b, DataType.long, nullable=False)
        counts = r.toObject(b, DataType.map, nullable=False)
        annotations = r.toObject(b, DataType.map, nullable=False)
        metrics = r.toObject(b, DataType.list, nullable=False)

        return {"id": metricid,
                "name": name,
                "dur": duration,
                "counts": counts,
                "annotations": annotations,
                "metrics": metrics}


class TraversalMetricsDeserializer(_GraphBinaryTypeIO):

    graphbinary_type = DataType.traversalmetrics

    @classmethod
    def objectify(cls, buff, reader, nullable=True):
        return cls.is_null(buff, reader, cls._read_traversalmetrics, nullable)

    @classmethod
    def _read_traversalmetrics(cls, b, r):
        duration = r.toObject(b, DataType.long, nullable=False)
        metrics = r.toObject(b, DataType.list, nullable=False)

        return {"dur": duration,
                "metrics": metrics}


class ClassSerializer(_GraphBinaryTypeIO):
    graphbinary_type = DataType.clazz
    python_type = GremlinType

    @classmethod
    def dictify(cls, obj, writer, as_value=False, nullable=True):
        return cls.as_bytes(cls.graphbinary_type, as_value, nullable,
                           StringIO.dictify(obj.gremlin_type, writer, True, False))


class TraversalStrategySerializer(_GraphBinaryTypeIO):
    graphbinary_type = DataType.traversalstrategy
    python_type = TraversalStrategy

    @classmethod
    def dictify(cls, obj, writer, as_value=False, nullable=True):
        ba = bytearray()
        ba.extend(ClassSerializer.dictify(GremlinType(obj.fqcn), writer, True, False))
        conf = {k: cls._convert(v) for k, v in obj.configuration.items()}
        ba.extend(MapIO.dictify(conf, writer, True, False))
        return cls.as_bytes(cls.graphbinary_type, as_value, nullable, ba)

    @classmethod
    def _convert(cls, v):
        return v.bytecode if isinstance(v, Traversal) else v
