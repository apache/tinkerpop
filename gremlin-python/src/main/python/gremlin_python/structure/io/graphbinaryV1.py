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
import uuid
import math
import io
import struct
from collections import OrderedDict
import logging

from struct import pack, unpack
from aenum import Enum
from datetime import timedelta
from gremlin_python import statics
from gremlin_python.statics import FloatType, BigDecimal, FunctionType, ShortType, IntType, LongType, BigIntType, \
                                   TypeType, DictType, ListType, SetType, SingleByte, ByteBufferType, GremlinType, \
                                   SingleChar
from gremlin_python.process.traversal import Barrier, Binding, Bytecode, Cardinality, Column, Direction, DT, GType, \
                                             Merge,Operator, Order, Pick, Pop, P, Scope, TextP, Traversal, Traverser, \
                                             TraversalStrategy, T
from gremlin_python.process.graph_traversal import GraphTraversal
from gremlin_python.structure.graph import Graph, Edge, Property, Vertex, VertexProperty, Path
from gremlin_python.structure.io.util import HashableDict, SymbolUtil

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
    graph = 0x10                  # not supported - no graph object in python yet
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
    bigdecimal = 0x22
    biginteger = 0x23
    byte = 0x24
    bytebuffer = 0x25
    short = 0x26
    boolean = 0x27
    textp = 0x28
    traversalstrategy = 0x29
    bulkset = 0x2a
    tree = 0x2b                   # not supported - no tree object in Python yet
    metrics = 0x2c
    traversalmetrics = 0x2d
    merge = 0x2e
    dt = 0x2f
    gtype = 0x30
    char = 0x80
    duration = 0x81
    inetaddress = 0x82            # todo
    instant = 0x83                # todo
    localdate = 0x84              # todo
    localdatetime = 0x85          # todo
    localtime = 0x86              # todo
    monthday = 0x87               # todo
    offsetdatetime = 0x88
    offsettime = 0x89             # todo
    period = 0x8a                 # todo
    year = 0x8b                   # todo
    yearmonth = 0x8c              # todo
    zonedatetime = 0x8d           # todo
    zoneoffset = 0x8e             # todo
    custom = 0x00                 # todo


NULL_BYTES = [DataType.null.value, 0x01]


def _make_packer(format_string):
    packer = struct.Struct(format_string)
    pack = packer.pack
    unpack = lambda s: packer.unpack(s)[0]
    return pack, unpack


int64_pack, int64_unpack = _make_packer('>q')
int32_pack, int32_unpack = _make_packer('>i')
int16_pack, int16_unpack = _make_packer('>h')
int8_pack, int8_unpack = _make_packer('>b')
uint64_pack, uint64_unpack = _make_packer('>Q')
uint8_pack, uint8_unpack = _make_packer('>B')
float_pack, float_unpack = _make_packer('>f')
double_pack, double_unpack = _make_packer('>d')


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

    def write_object(self, object_data):
        return self.to_dict(object_data)

    def to_dict(self, obj, to_extend=None):
        if to_extend is None:
            to_extend = bytearray()

        if obj is None:
            to_extend.extend(NULL_BYTES)
            return

        try:
            t = type(obj)
            return self.serializers[t].dictify(obj, self, to_extend)
        except KeyError:
            for key, serializer in self.serializers.items():
                if isinstance(obj, key):
                    return serializer.dictify(obj, self, to_extend)

        if isinstance(obj, dict):
            return dict((self.to_dict(k, to_extend), self.to_dict(v, to_extend)) for k, v in obj.items())
        elif isinstance(obj, set):
            return set([self.to_dict(o, to_extend) for o in obj])
        elif isinstance(obj, list):
            return [self.to_dict(o, to_extend) for o in obj]
        else:
            return obj


class GraphBinaryReader(object):
    def __init__(self, deserializer_map=None):
        self.deserializers = _deserializers.copy()
        if deserializer_map:
            self.deserializers.update(deserializer_map)

    def read_object(self, b):
        if isinstance(b, bytearray):
            return self.to_object(io.BytesIO(b))
        elif isinstance(b, io.BufferedIOBase):
            return self.to_object(b)

    def to_object(self, buff, data_type=None, nullable=True):
        if data_type is None:
            bt = uint8_unpack(buff.read(1))
            if bt == DataType.null.value:
                if nullable:
                    buff.read(1)
                return None
            return self.deserializers[DataType(bt)].objectify(buff, self, nullable)
        else:
            return self.deserializers[data_type].objectify(buff, self, nullable)


class _GraphBinaryTypeIO(object, metaclass=GraphBinaryTypeType):
    python_type = None
    graphbinary_type = None

    @classmethod
    def prefix_bytes(cls, graphbin_type, as_value=False, nullable=True, to_extend=None):
        if to_extend is None:
            to_extend = bytearray()

        if not as_value:
            to_extend += uint8_pack(graphbin_type.value)

        if nullable:
            to_extend += int8_pack(0)

        return to_extend

    @classmethod
    def read_int(cls, buff):
        return int32_unpack(buff.read(4))

    @classmethod
    def is_null(cls, buff, reader, else_opt, nullable=True):
        return None if nullable and buff.read(1)[0] == 0x01 else else_opt(buff, reader)

    def dictify(self, obj, writer, to_extend, as_value=False, nullable=True):
        raise NotImplementedError()

    def objectify(self, d, reader, nullable=True):
        raise NotImplementedError()
        

class LongIO(_GraphBinaryTypeIO):

    python_type = LongType
    graphbinary_type = DataType.long
    byte_format_pack = int64_pack
    byte_format_unpack = int64_unpack

    @classmethod
    def dictify(cls, obj, writer, to_extend, as_value=False, nullable=True):
        if obj < -9223372036854775808 or obj > 9223372036854775807:
            raise Exception("Value too big, please use bigint Gremlin type")
        else:
            cls.prefix_bytes(cls.graphbinary_type, as_value, nullable, to_extend)
            to_extend.extend(cls.byte_format_pack(obj))
            return to_extend

    @classmethod
    def objectify(cls, buff, reader, nullable=True):
        return cls.is_null(buff, reader, lambda b, r: int64_unpack(buff.read(8)), nullable)


class IntIO(LongIO):

    python_type = IntType
    graphbinary_type = DataType.int
    byte_format_pack = int32_pack
    byte_format_unpack = int32_unpack

    @classmethod
    def objectify(cls, buff, reader, nullable=True):
        return cls.is_null(buff, reader, lambda b, r: cls.read_int(b), nullable)


class ShortIO(LongIO):

    python_type = ShortType
    graphbinary_type = DataType.short
    byte_format_pack = int16_pack
    byte_format_unpack = int16_unpack

    @classmethod
    def objectify(cls, buff, reader, nullable=True):
        return cls.is_null(buff, reader, lambda b, r: int16_unpack(buff.read(2)), nullable)


class BigIntIO(_GraphBinaryTypeIO):

    python_type = BigIntType
    graphbinary_type = DataType.biginteger

    @classmethod
    def write_bigint(cls, obj, to_extend):
        length = (obj.bit_length() + 7) // 8
        if obj > 0:
            b = obj.to_bytes(length, byteorder='big')
            to_extend.extend(int32_pack(length + 1))
            to_extend.extend(int8_pack(0))
            to_extend.extend(b)
        else:
            # handle negative
            b = obj.to_bytes(length, byteorder='big', signed=True)
            to_extend.extend(int32_pack(length))
            to_extend.extend(b)
        return to_extend

    @classmethod
    def dictify(cls, obj, writer, to_extend, as_value=False, nullable=True):
        cls.prefix_bytes(cls.graphbinary_type, as_value, nullable, to_extend)
        return cls.write_bigint(obj, to_extend)

    @classmethod
    def read_bigint(cls, buff):
        size = cls.read_int(buff)
        return int.from_bytes(buff.read(size), byteorder='big', signed=True)

    @classmethod
    def objectify(cls, buff, reader, nullable=False):
        return cls.is_null(buff, reader, lambda b, r: cls.read_bigint(b), nullable)


class DateIO(_GraphBinaryTypeIO):

    python_type = datetime.datetime
    graphbinary_type = DataType.date

    @classmethod
    def dictify(cls, obj, writer, to_extend, as_value=False, nullable=True):
        try:
            timestamp_seconds = calendar.timegm(obj.utctimetuple())
            pts = timestamp_seconds * 1e3 + getattr(obj, 'microsecond', 0) / 1e3
        except AttributeError:
            pts = calendar.timegm(obj.timetuple()) * 1e3

        ts = int(round(pts))
        cls.prefix_bytes(cls.graphbinary_type, as_value, nullable, to_extend)
        to_extend.extend(int64_pack(ts))
        return to_extend

    @classmethod
    def objectify(cls, buff, reader, nullable=True):
        return cls.is_null(buff, reader,
                           lambda b, r: datetime.datetime.utcfromtimestamp(int64_unpack(b.read(8)) / 1000.0),
                           nullable)


class OffsetDateTimeIO(_GraphBinaryTypeIO):

    python_type = datetime.datetime
    graphbinary_type = DataType.offsetdatetime

    @classmethod
    def dictify(cls, obj, writer, to_extend, as_value=False, nullable=True):
        if obj.tzinfo is None:
            return DateIO.dictify(obj, writer, to_extend, as_value, nullable)
        cls.prefix_bytes(cls.graphbinary_type, as_value, nullable, to_extend)
        IntIO.dictify(obj.year, writer, to_extend, True, False)
        ByteIO.dictify(obj.month, writer, to_extend, True, False)
        ByteIO.dictify(obj.day, writer, to_extend, True, False)
        # construct time of day in nanoseconds
        h = obj.time().hour
        m = obj.time().minute
        s = obj.time().second
        ms = obj.time().microsecond
        ns = round((h*60*60*1e9) + (m*60*1e9) + (s*1e9) + (ms*1e3))
        LongIO.dictify(ns, writer, to_extend, True, False)
        os = round(obj.utcoffset().total_seconds())
        IntIO.dictify(os, writer, to_extend, True, False)
        return to_extend

    @classmethod
    def objectify(cls, buff, reader, nullable=True):
        return cls.is_null(buff, reader, cls._read_dt, nullable)

    @classmethod
    def _read_dt(cls, b, r):
        year = r.to_object(b, DataType.int, False)
        month = r.to_object(b, DataType.byte, False)
        day = r.to_object(b, DataType.byte, False)
        ns = r.to_object(b, DataType.long, False)
        offset = r.to_object(b, DataType.int, False)
        tz = datetime.timezone(timedelta(seconds=offset))
        return datetime.datetime(year, month, day, tzinfo=tz) + timedelta(microseconds=ns/1000)

# Based on current implementation, this class must always be declared before FloatIO.
# Seems pretty fragile for future maintainers. Maybe look into this.
class TimestampIO(_GraphBinaryTypeIO):
    python_type = statics.timestamp
    graphbinary_type = DataType.timestamp

    @classmethod
    def dictify(cls, obj, writer, to_extend, as_value=False, nullable=True):
        # Java timestamp expects milliseconds integer - Have to use int because of legacy Python
        ts = int(round(obj * 1000))
        cls.prefix_bytes(cls.graphbinary_type, as_value, nullable, to_extend)
        to_extend.extend(int64_pack(ts))
        return to_extend

    @classmethod
    def objectify(cls, buff, reader, nullable=True):
        # Python timestamp expects seconds
        return cls.is_null(buff, reader, lambda b, r: statics.timestamp(int64_unpack(b.read(8)) / 1000.0),
                           nullable)
    

def _long_bits_to_double(bits):
    return unpack('d', pack('Q', bits))[0]


NAN = _long_bits_to_double(0x7ff8000000000000)
POSITIVE_INFINITY = _long_bits_to_double(0x7ff0000000000000)
NEGATIVE_INFINITY = _long_bits_to_double(0xFff0000000000000)


class FloatIO(LongIO):

    python_type = FloatType
    graphbinary_type = DataType.float
    graphbinary_base_type = DataType.float
    byte_format_pack = float_pack
    byte_format_unpack = float_unpack

    @classmethod
    def dictify(cls, obj, writer, to_extend, as_value=False, nullable=True):
        if math.isnan(obj):
            cls.prefix_bytes(cls.graphbinary_type, as_value, nullable, to_extend)
            to_extend.extend(cls.byte_format_pack(NAN))
        elif math.isinf(obj) and obj > 0:
            cls.prefix_bytes(cls.graphbinary_type, as_value, nullable, to_extend)
            to_extend.extend(cls.byte_format_pack(POSITIVE_INFINITY))
        elif math.isinf(obj) and obj < 0:
            cls.prefix_bytes(cls.graphbinary_type, as_value, nullable, to_extend)
            to_extend.extend(cls.byte_format_pack(NEGATIVE_INFINITY))
        else:
            cls.prefix_bytes(cls.graphbinary_type, as_value, nullable, to_extend)
            to_extend.extend(cls.byte_format_pack(obj))

        return to_extend

    @classmethod
    def objectify(cls, buff, reader, nullable=True):
        return cls.is_null(buff, reader, lambda b, r: float_unpack(b.read(4)), nullable)


class DoubleIO(FloatIO):
    """
    Floats basically just fall through to double serialization.
    """

    graphbinary_type = DataType.double
    graphbinary_base_type = DataType.double
    byte_format_pack = double_pack
    byte_format_unpack = double_unpack

    @classmethod
    def objectify(cls, buff, reader, nullable=True):
        return cls.is_null(buff, reader, lambda b, r: double_unpack(b.read(8)), nullable)


class BigDecimalIO(_GraphBinaryTypeIO):

    python_type = BigDecimal
    graphbinary_type = DataType.bigdecimal

    @classmethod
    def dictify(cls, obj, writer, to_extend, as_value=False, nullable=True):
        cls.prefix_bytes(cls.graphbinary_type, as_value, nullable, to_extend)
        to_extend.extend(int32_pack(obj.scale))
        return BigIntIO.write_bigint(obj.unscaled_value, to_extend)

    @classmethod
    def _read(cls, buff):
        scale = int32_unpack(buff.read(4))
        unscaled_value = BigIntIO.read_bigint(buff)
        return BigDecimal(scale, unscaled_value)

    @classmethod
    def objectify(cls, buff, reader, nullable=False):
        return cls.is_null(buff, reader, lambda b, r: cls._read(b), nullable)


class CharIO(_GraphBinaryTypeIO):
    python_type = SingleChar
    graphbinary_type = DataType.char

    @classmethod
    def dictify(cls, obj, writer, to_extend, as_value=False, nullable=True):
        cls.prefix_bytes(cls.graphbinary_type, as_value, nullable, to_extend)
        to_extend.extend(obj.encode("utf-8"))
        return to_extend

    @classmethod
    def objectify(cls, buff, reader, nullable=True):
        return cls.is_null(buff, reader, cls._read_char, nullable)

    @classmethod
    def _read_char(cls, b, r):
        max_bytes = 4
        x = b.read(1)
        while max_bytes > 0:
            max_bytes = max_bytes - 1
            try:
                return x.decode("utf-8")
            except UnicodeDecodeError:
                x += b.read(1)


class StringIO(_GraphBinaryTypeIO):

    python_type = str
    graphbinary_type = DataType.string

    @classmethod
    def dictify(cls, obj, writer, to_extend, as_value=False, nullable=True):
        cls.prefix_bytes(cls.graphbinary_type, as_value, nullable, to_extend)
        str_bytes = obj.encode("utf-8")
        to_extend += int32_pack(len(str_bytes))
        to_extend += str_bytes
        return to_extend

    @classmethod
    def objectify(cls, buff, reader, nullable=True):
        return cls.is_null(buff, reader, lambda b, r: b.read(cls.read_int(b)).decode("utf-8"), nullable)


class ListIO(_GraphBinaryTypeIO):

    python_type = list
    graphbinary_type = DataType.list

    @classmethod
    def dictify(cls, obj, writer, to_extend, as_value=False, nullable=True):
        cls.prefix_bytes(cls.graphbinary_type, as_value, nullable, to_extend)
        to_extend.extend(int32_pack(len(obj)))
        for item in obj:
            writer.to_dict(item, to_extend)

        return to_extend

    @classmethod
    def objectify(cls, buff, reader, nullable=True):
        return cls.is_null(buff, reader, cls._read_list, nullable)

    @classmethod
    def _read_list(cls, b, r):
        size = cls.read_int(b)
        the_list = []
        while size > 0:
            the_list.append(r.read_object(b))
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
    def dictify(cls, obj, writer, to_extend, as_value=False, nullable=True):
        cls.prefix_bytes(cls.graphbinary_type, as_value, nullable, to_extend)

        to_extend.extend(int32_pack(len(obj)))
        for k, v in obj.items():
            writer.to_dict(k, to_extend)
            writer.to_dict(v, to_extend)

        return to_extend

    @classmethod
    def objectify(cls, buff, reader, nullable=True):
        return cls.is_null(buff, reader, cls._read_map, nullable)

    @classmethod
    def _read_map(cls, b, r):
        size = cls.read_int(b)
        the_dict = {}
        while size > 0:
            k = HashableDict.of(r.read_object(b))
            v = r.read_object(b)
            the_dict[k] = v
            size = size - 1

        return the_dict


class UuidIO(_GraphBinaryTypeIO):

    python_type = uuid.UUID
    graphbinary_type = DataType.uuid

    @classmethod
    def dictify(cls, obj, writer, to_extend, as_value=False, nullable=True):
        cls.prefix_bytes(cls.graphbinary_type, as_value, nullable, to_extend)
        to_extend.extend(obj.bytes)
        return to_extend

    @classmethod
    def objectify(cls, buff, reader, nullable=True):
        return cls.is_null(buff, reader, lambda b, r: uuid.UUID(bytes=b.read(16)), nullable)


class EdgeIO(_GraphBinaryTypeIO):

    python_type = Edge
    graphbinary_type = DataType.edge

    @classmethod
    def dictify(cls, obj, writer, to_extend, as_value=False, nullable=True):
        cls.prefix_bytes(cls.graphbinary_type, as_value, nullable, to_extend)

        writer.to_dict(obj.id, to_extend)
        StringIO.dictify(obj.label, writer, to_extend, True, False)
        writer.to_dict(obj.inV.id, to_extend)
        StringIO.dictify(obj.inV.label, writer, to_extend, True, False)
        writer.to_dict(obj.outV.id, to_extend)
        StringIO.dictify(obj.outV.label, writer, to_extend, True, False)
        to_extend.extend(NULL_BYTES)
        to_extend.extend(NULL_BYTES)

        return to_extend

    @classmethod
    def objectify(cls, buff, reader, nullable=True):
        return cls.is_null(buff, reader, cls._read_edge, nullable)

    @classmethod
    def _read_edge(cls, b, r):
        edgeid = r.read_object(b)
        edgelbl = r.to_object(b, DataType.string, False)
        inv = Vertex(r.read_object(b), r.to_object(b, DataType.string, False))
        outv = Vertex(r.read_object(b), r.to_object(b, DataType.string, False))
        b.read(2)
        props = r.read_object(b)
        # null properties are returned as empty lists
        properties = [] if props is None else props
        edge = Edge(edgeid, outv, edgelbl, inv, properties)
        return edge


class PathIO(_GraphBinaryTypeIO):

    python_type = Path
    graphbinary_type = DataType.path

    @classmethod
    def dictify(cls, obj, writer, to_extend, as_value=False, nullable=True):
        cls.prefix_bytes(cls.graphbinary_type, as_value, nullable, to_extend)
        writer.to_dict(obj.labels, to_extend)
        writer.to_dict(obj.objects, to_extend)
        return to_extend

    @classmethod
    def objectify(cls, buff, reader, nullable=True):
        return cls.is_null(buff, reader, lambda b, r: Path(r.read_object(b), r.read_object(b)), nullable)


class PropertyIO(_GraphBinaryTypeIO):

    python_type = Property
    graphbinary_type = DataType.property

    @classmethod
    def dictify(cls, obj, writer, to_extend, as_value=False, nullable=True):
        cls.prefix_bytes(cls.graphbinary_type, as_value, nullable, to_extend)
        StringIO.dictify(obj.key, writer, to_extend, True, False)
        writer.to_dict(obj.value, to_extend)
        to_extend.extend(NULL_BYTES)
        return to_extend

    @classmethod
    def objectify(cls, buff, reader, nullable=True):
        return cls.is_null(buff, reader, cls._read_property, nullable)

    @classmethod
    def _read_property(cls, b, r):
        p = Property(r.to_object(b, DataType.string, False), r.read_object(b), None)
        b.read(2)
        return p


class TinkerGraphIO(_GraphBinaryTypeIO):

    python_type = Graph
    graphbinary_type = DataType.graph

    @classmethod
    def dictify(cls, obj, writer, to_extend, as_value=False, nullable=True):
        raise AttributeError("TinkerGraph serialization is not currently supported by gremlin-python")

    @classmethod
    def objectify(cls, b, reader, as_value=False):
        raise AttributeError("TinkerGraph deserialization is not currently supported by gremlin-python")


class VertexIO(_GraphBinaryTypeIO):

    python_type = Vertex
    graphbinary_type = DataType.vertex

    @classmethod
    def dictify(cls, obj, writer, to_extend, as_value=False, nullable=True):
        cls.prefix_bytes(cls.graphbinary_type, as_value, nullable, to_extend)
        writer.to_dict(obj.id, to_extend)
        StringIO.dictify(obj.label, writer, to_extend, True, False)
        to_extend.extend(NULL_BYTES)
        return to_extend

    @classmethod
    def objectify(cls, buff, reader, nullable=True):
        return cls.is_null(buff, reader, cls._read_vertex, nullable)

    @classmethod
    def _read_vertex(cls, b, r):
        vertex_id = r.read_object(b)
        vertex_label = r.to_object(b, DataType.string, False)
        props = r.read_object(b)
        # null properties are returned as empty lists
        properties = [] if props is None else props
        vertex = Vertex(vertex_id, vertex_label, properties)
        return vertex


class VertexPropertyIO(_GraphBinaryTypeIO):

    python_type = VertexProperty
    graphbinary_type = DataType.vertexproperty

    @classmethod
    def dictify(cls, obj, writer, to_extend, as_value=False, nullable=True):
        cls.prefix_bytes(cls.graphbinary_type, as_value, nullable, to_extend)
        writer.to_dict(obj.id, to_extend)
        StringIO.dictify(obj.label, writer, to_extend, True, False)
        writer.to_dict(obj.value, to_extend)
        to_extend.extend(NULL_BYTES)
        to_extend.extend(NULL_BYTES)
        return to_extend

    @classmethod
    def objectify(cls, buff, reader, nullable=True):
        return cls.is_null(buff, reader, cls._read_vertexproperty, nullable)

    @classmethod
    def _read_vertexproperty(cls, b, r):
        vp = VertexProperty(r.read_object(b), r.to_object(b, DataType.string, False), r.read_object(b), None)
        b.read(2)
        properties = r.read_object(b)
        # null properties are returned as empty lists
        vp.properties = [] if properties is None else properties
        return vp


class _EnumIO(_GraphBinaryTypeIO):

    @classmethod
    def dictify(cls, obj, writer, to_extend, as_value=False, nullable=True):
        cls.prefix_bytes(cls.graphbinary_type, as_value, nullable, to_extend)
        StringIO.dictify(SymbolUtil.to_camel_case(str(obj.name)), writer, to_extend)
        return to_extend

    @classmethod
    def objectify(cls, buff, reader, nullable=True):
        return cls.is_null(buff, reader, cls._read_enumval, nullable)

    @classmethod
    def _read_enumval(cls, b, r):
        enum_name = r.to_object(b)
        return cls.python_type[SymbolUtil.to_snake_case(enum_name)]


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

    @classmethod
    def _read_enumval(cls, b, r):
        # Direction needs to retain all CAPS. note that to_/from_ are really just aliases of IN/OUT
        # so they don't need to be accounted for in serialization
        enum_name = r.to_object(b)
        return cls.python_type[enum_name]


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
    def dictify(cls, obj, writer, to_extend, as_value=False, nullable=True):
        cls.prefix_bytes(cls.graphbinary_type, as_value, nullable, to_extend)
        StringIO.dictify(obj.key, writer, to_extend, True, False)
        writer.to_dict(obj.value, to_extend)
        return to_extend

    @classmethod
    def objectify(cls, buff, reader, nullable=True):
        return cls.is_null(buff, reader, lambda b, r: Binding(r.to_object(b, DataType.string, False),
                                                              reader.read_object(b)), nullable)


class BytecodeIO(_GraphBinaryTypeIO):
    python_type = Bytecode
    graphbinary_type = DataType.bytecode

    @classmethod
    def dictify(cls, obj, writer, to_extend, as_value=False, nullable=True):
        cls.prefix_bytes(cls.graphbinary_type, as_value, nullable, to_extend)
        bc = obj.bytecode if isinstance(obj, Traversal) else obj
        to_extend.extend(int32_pack(len(bc.step_instructions)))
        for inst in bc.step_instructions:
            inst_name, inst_args = inst[0], inst[1:] if len(inst) > 1 else []
            StringIO.dictify(inst_name, writer, to_extend, True, False)
            to_extend.extend(int32_pack(len(inst_args)))
            for arg in inst_args:
                writer.to_dict(arg, to_extend)

        to_extend.extend(int32_pack(len(bc.source_instructions)))
        for inst in bc.source_instructions:
            inst_name, inst_args = inst[0], inst[1:] if len(inst) > 1 else []
            StringIO.dictify(inst_name, writer, to_extend, True, False)
            to_extend.extend(int32_pack(len(inst_args)))
            for arg in inst_args:
                if isinstance(arg, TypeType):
                    writer.to_dict(GremlinType(arg().fqcn), to_extend)
                else:
                    writer.to_dict(arg, to_extend)
        return to_extend

    @classmethod
    def objectify(cls, buff, reader, nullable=True):
        return cls.is_null(buff, reader, cls._read_bytecode, nullable)

    @classmethod
    def _read_bytecode(cls, b, r):
        bytecode = Bytecode()

        step_count = cls.read_int(b)
        ix = 0
        while ix < step_count:
            inst = [r.to_object(b, DataType.string, False)]
            inst_ct = cls.read_int(b)
            iy = 0
            while iy < inst_ct:
                inst.append(r.read_object(b))
                iy += 1
            bytecode.step_instructions.append(inst)
            ix += 1

        source_count = cls.read_int(b)
        ix = 0
        while ix < source_count:
            inst = [r.to_object(b, DataType.string, False)]
            inst_ct = cls.read_int(b)
            iy = 0
            while iy < inst_ct:
                inst.append(r.read_object(b))
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
    def dictify(cls, obj, writer, to_extend, as_value=False, nullable=True):
        cls.prefix_bytes(cls.graphbinary_type, as_value, nullable, to_extend)

        lambda_result = obj()
        script = lambda_result if isinstance(lambda_result, str) else lambda_result[0]
        language = statics.default_lambda_language if isinstance(lambda_result, str) else lambda_result[1]

        StringIO.dictify(language, writer, to_extend, True, False)

        script_cleaned = script
        script_args = -1

        if language == "gremlin-groovy" and "->" in script:
            # if the user has explicitly added parameters to the groovy closure then we can easily detect one or two
            # arg lambdas - if we can't detect 1 or 2 then we just go with "unknown"
            args = script[0:script.find("->")]
            script_args = 2 if "," in args else 1

        StringIO.dictify(script_cleaned, writer, to_extend, True, False)
        to_extend.extend(int32_pack(script_args))

        return to_extend


class PSerializer(_GraphBinaryTypeIO):
    graphbinary_type = DataType.p
    python_type = P

    @classmethod
    def dictify(cls, obj, writer, to_extend, as_value=False, nullable=True):
        cls.prefix_bytes(cls.graphbinary_type, as_value, nullable, to_extend)

        StringIO.dictify(obj.operator, writer, to_extend, True, False)

        args = []
        if obj.other is None:
            if isinstance(obj.value, ListType):
                args = obj.value
            else:
                args.append(obj.value)
        else:
            args.append(obj.value)
            args.append(obj.other)

        to_extend.extend(int32_pack(len(args)))
        for a in args:
            writer.to_dict(a, to_extend)

        return to_extend


class DTIO(_EnumIO):
    graphbinary_type = DataType.dt
    python_type = DT


class MergeIO(_EnumIO):
    graphbinary_type = DataType.merge
    python_type = Merge


class ScopeIO(_EnumIO):
    graphbinary_type = DataType.scope
    python_type = Scope


class TIO(_EnumIO):
    graphbinary_type = DataType.t
    python_type = T


class GTYPEIO(_EnumIO):
    graphbinary_type = DataType.gtype
    python_type = GType


class TraverserIO(_GraphBinaryTypeIO):
    graphbinary_type = DataType.traverser
    python_type = Traverser

    @classmethod
    def dictify(cls, obj, writer, to_extend, as_value=False, nullable=True):
        cls.prefix_bytes(cls.graphbinary_type, as_value, nullable, to_extend)
        to_extend.extend(int64_pack(obj.bulk))
        writer.to_dict(obj.object, to_extend)
        return to_extend

    @classmethod
    def objectify(cls, buff, reader, nullable=True):
        return cls.is_null(buff, reader, cls._read_traverser, nullable)

    @classmethod
    def _read_traverser(cls, b, r):
        bulk = int64_unpack(b.read(8))
        obj = r.read_object(b)
        return Traverser(obj, bulk=bulk)


class ByteIO(_GraphBinaryTypeIO):
    python_type = SingleByte
    graphbinary_type = DataType.byte

    @classmethod
    def dictify(cls, obj, writer, to_extend, as_value=False, nullable=True):
        cls.prefix_bytes(cls.graphbinary_type, as_value, nullable, to_extend)
        to_extend.extend(int8_pack(obj))
        return to_extend

    @classmethod
    def objectify(cls, buff, reader, nullable=True):
        return cls.is_null(buff, reader,
                           lambda b, r: int.__new__(SingleByte, int8_unpack(b.read(1))),
                           nullable)


class ByteBufferIO(_GraphBinaryTypeIO):
    python_type = ByteBufferType
    graphbinary_type = DataType.bytebuffer

    @classmethod
    def dictify(cls, obj, writer, to_extend, as_value=False, nullable=True):
        cls.prefix_bytes(cls.graphbinary_type, as_value, nullable, to_extend)
        to_extend.extend(int32_pack(len(obj)))
        to_extend.extend(obj)
        return to_extend

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
    def dictify(cls, obj, writer, to_extend, as_value=False, nullable=True):
        cls.prefix_bytes(cls.graphbinary_type, as_value, nullable, to_extend)
        to_extend.extend(int8_pack(0x01 if obj else 0x00))
        return to_extend

    @classmethod
    def objectify(cls, buff, reader, nullable=True):
        return cls.is_null(buff, reader,
                           lambda b, r: True if int8_unpack(b.read(1)) == 0x01 else False,
                           nullable)


class TextPSerializer(_GraphBinaryTypeIO):
    graphbinary_type = DataType.textp
    python_type = TextP

    @classmethod
    def dictify(cls, obj, writer, to_extend, as_value=False, nullable=True):
        cls.prefix_bytes(cls.graphbinary_type, as_value, nullable, to_extend)

        StringIO.dictify(obj.operator, writer, to_extend, True, False)

        args = []
        if obj.other is None:
            if isinstance(obj.value, ListType):
                args = obj.value
            else:
                args.append(obj.value)
        else:
            args.append(obj.value)
            args.append(obj.other)

        to_extend.extend(int32_pack(len(args)))
        for a in args:
            writer.to_dict(a, to_extend)

        return to_extend


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
            itm = r.read_object(b)
            bulk = int64_unpack(b.read(8))
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
        metricid = r.to_object(b, DataType.string, False)
        name = r.to_object(b, DataType.string, False)
        duration = r.to_object(b, DataType.long, nullable=False)
        counts = r.to_object(b, DataType.map, nullable=False)
        annotations = r.to_object(b, DataType.map, nullable=False)
        metrics = r.to_object(b, DataType.list, nullable=False)

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
        duration = r.to_object(b, DataType.long, nullable=False)
        metrics = r.to_object(b, DataType.list, nullable=False)

        return {"dur": duration,
                "metrics": metrics}


class ClassSerializer(_GraphBinaryTypeIO):
    graphbinary_type = DataType.clazz
    python_type = GremlinType

    @classmethod
    def dictify(cls, obj, writer, to_extend, as_value=False, nullable=True):
        cls.prefix_bytes(cls.graphbinary_type, as_value, nullable, to_extend)
        StringIO.dictify(obj.gremlin_type, writer, to_extend, True, False)
        return to_extend


class TraversalStrategySerializer(_GraphBinaryTypeIO):
    graphbinary_type = DataType.traversalstrategy
    python_type = TraversalStrategy

    @classmethod
    def dictify(cls, obj, writer, to_extend, as_value=False, nullable=True):
        cls.prefix_bytes(cls.graphbinary_type, as_value, nullable, to_extend)

        ClassSerializer.dictify(GremlinType(obj.fqcn), writer, to_extend, True, False)
        conf = {k: cls._convert(v) for k, v in obj.configuration.items()}
        MapIO.dictify(conf, writer, to_extend, True, False)

        return to_extend

    @classmethod
    def _convert(cls, v):
        return v.bytecode if isinstance(v, Traversal) else v


class DurationIO(_GraphBinaryTypeIO):
    python_type = timedelta
    graphbinary_type = DataType.duration

    @classmethod
    def dictify(cls, obj, writer, to_extend, as_value=False, nullable=True):
        cls.prefix_bytes(cls.graphbinary_type, as_value, nullable, to_extend)
        LongIO.dictify(obj.seconds, writer, to_extend, True, False)
        IntIO.dictify(obj.microseconds * 1000, writer, to_extend, True, False)
        return to_extend

    @classmethod
    def objectify(cls, buff, reader, nullable=True):
        return cls.is_null(buff, reader, cls._read_duration, nullable)
    
    @classmethod
    def _read_duration(cls, b, r):
        seconds = r.to_object(b, DataType.long, False)
        nanos = r.to_object(b, DataType.int, False)
        return timedelta(seconds=seconds, microseconds=nanos / 1000)
