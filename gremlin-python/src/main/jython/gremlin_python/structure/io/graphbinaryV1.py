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
import struct
import time
import uuid
import math
import base64
import io
import numbers
from collections import OrderedDict
from decimal import *
import logging
from datetime import timedelta

import six
from aenum import Enum
from isodate import parse_duration, duration_isoformat

from gremlin_python import statics
from gremlin_python.statics import FloatType, FunctionType, IntType, LongType, TypeType, DictType, ListType, SetType, SingleByte, ByteBufferType, SingleChar
from gremlin_python.process.traversal import Binding, Bytecode, P, TextP, Traversal, Traverser, TraversalStrategy, T
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
    tinkergraph = 0x10
    vertex = 0x11
    vertexproperty = 0x12


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
            return self.serializers[type(obj)].dictify(obj, self)
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

    def toObject(self, buff):
        bt = buff.read(1)
        bt_value = struct.unpack('>b', bt)[0]
        return self.deserializers[DataType(bt_value)].objectify(buff, self)


@six.add_metaclass(GraphBinaryTypeType)
class _GraphBinaryTypeIO(object):
    python_type = None
    graphbinary_type = None

    symbolMap = {"global_": "global", "as_": "as", "in_": "in", "and_": "and",
                 "or_": "or", "is_": "is", "not_": "not", "from_": "from",
                 "set_": "set", "list_": "list", "all_": "all", "with_": "with"}

    @classmethod
    def as_bytes(cls, graphbin_type=None, size=None, *args):
        ba = bytearray() if graphbin_type is None else bytearray([graphbin_type.value])
        if size is not None:
            ba.extend(struct.pack(">i", size))

        for arg in args:
            if isinstance(arg, (bytes, bytearray)):
                ba.extend(arg)
            else:
                raise Exception("MISSING")
        return ba

    @classmethod
    def string_as_bytes(cls, s):
        return cls.as_bytes(None, len(s), s.encode("utf-8"))

    @classmethod
    def read_int(cls, buff):
        return struct.unpack(">i", buff.read(4))[0]

    @classmethod
    def read_string(cls, buff):
        return buff.read(cls.read_int(buff)).decode("utf-8")

    @classmethod
    def unmangleKeyword(cls, symbol):
        return cls.symbolMap.get(symbol, symbol)

    def dictify(self, obj, writer):
        raise NotImplementedError()

    def objectify(self, d, reader):
        raise NotImplementedError()


class LongIO(_GraphBinaryTypeIO):

    python_type = LongType
    graphbinary_type = DataType.long
    byte_format = ">q"

    @classmethod
    def dictify(cls, obj, writer):
        if obj < -9223372036854775808 or obj > 9223372036854775807:
            raise Exception("TODO: don't forget bigint")
        else:
            return cls.as_bytes(cls.graphbinary_type, None, struct.pack(cls.byte_format, obj))

    @classmethod
    def objectify(cls, buff, reader):
        return struct.unpack(cls.byte_format, buff.read(8))[0]


class IntIO(LongIO):

    python_type = IntType
    graphbinary_type = DataType.int
    byte_format = ">i"

    @classmethod
    def objectify(cls, buff, reader):
        return cls.read_int(buff)


class DateIO(_GraphBinaryTypeIO):

    python_type = datetime.datetime
    graphbinary_type = DataType.date

    @classmethod
    def dictify(cls, obj, writer):
        try:
            timestamp_seconds = calendar.timegm(obj.utctimetuple())
            pts = timestamp_seconds * 1e3 + getattr(obj, 'microsecond', 0) / 1e3
        except AttributeError:
            pts = calendar.timegm(obj.timetuple()) * 1e3
            
        ts = int(round(pts * 100))
        return cls.as_bytes(cls.graphbinary_type, None, struct.pack(">q", ts))

    @classmethod
    def objectify(cls, buff, reader):
        return datetime.datetime.utcfromtimestamp(struct.unpack(">q", buff.read(8))[0] / 1000.0)


# Based on current implementation, this class must always be declared before FloatIO.
# Seems pretty fragile for future maintainers. Maybe look into this.
class TimestampIO(_GraphBinaryTypeIO):
    python_type = statics.timestamp
    graphbinary_type = DataType.timestamp

    @classmethod
    def dictify(cls, obj, writer):
        # Java timestamp expects milliseconds integer - Have to use int because of legacy Python
        ts = int(round(obj * 1000))
        return cls.as_bytes(cls.graphbinary_type, None, struct.pack(">q", ts))

    @classmethod
    def objectify(cls, buff, reader):
        # Python timestamp expects seconds
        return statics.timestamp(struct.unpack(">q", buff.read(8))[0] / 1000.0)


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
    def dictify(cls, obj, writer):
        if math.isnan(obj):
            return cls.as_bytes(cls.graphbinary_base_type, None, struct.pack(cls.byte_format, NAN))
        elif math.isinf(obj) and obj > 0:
            return cls.as_bytes(cls.graphbinary_base_type, None, struct.pack(cls.byte_format, POSITIVE_INFINITY))
        elif math.isinf(obj) and obj < 0:
            return cls.as_bytes(cls.graphbinary_base_type, None, struct.pack(cls.byte_format, NEGATIVE_INFINITY))
        else:
            return cls.as_bytes(cls.graphbinary_base_type, None, struct.pack(cls.byte_format, obj))

    @classmethod
    def objectify(cls, buff, reader):
        return struct.unpack(cls.byte_format, buff.read(4))[0]


class DoubleIO(FloatIO):
    """
    Floats basically just fall through to double serialization.
    """
    
    graphbinary_type = DataType.double
    graphbinary_base_type = DataType.double
    byte_format = ">d"

    @classmethod
    def objectify(cls, buff, reader):
        return struct.unpack(cls.byte_format, buff.read(8))[0]


class TypeSerializer(_GraphBinaryTypeIO):
    python_type = TypeType

    @classmethod
    def dictify(cls, typ, writer):
        return writer.toDict(typ())


class StringIO(_GraphBinaryTypeIO):

    python_type = str
    graphbinary_type = DataType.string

    @classmethod
    def dictify(cls, obj, writer):
        return cls.as_bytes(cls.graphbinary_type, len(obj), obj.encode("utf-8"))

    @classmethod
    def objectify(cls, b, reader):
        return cls.read_string(b)


class ListIO(_GraphBinaryTypeIO):

    python_type = list
    graphbinary_type = DataType.list

    @classmethod
    def dictify(cls, obj, writer):
        list_data = bytearray()
        for item in obj:
            list_data.extend(writer.writeObject(item))

        return cls.as_bytes(cls.graphbinary_type, len(obj), list_data)

    @classmethod
    def objectify(cls, buff, reader):
        size = cls.read_int(buff)
        the_list = []
        while size > 0:
            the_list.append(reader.readObject(buff))
            size = size - 1

        return the_list


class SetIO(ListIO):

    python_type = SetType
    graphbinary_type = DataType.set

    @classmethod
    def objectify(cls, buff, reader):
        return set(ListIO.objectify(buff, reader))


class MapIO(_GraphBinaryTypeIO):

    python_type = dict
    graphbinary_type = DataType.map

    @classmethod
    def dictify(cls, obj, writer):
        map_data = bytearray()
        for k, v in obj.items():
            map_data.extend(writer.writeObject(k))
            map_data.extend(writer.writeObject(v))

        return cls.as_bytes(cls.graphbinary_type, len(obj), map_data)

    @classmethod
    def objectify(cls, buff, reader):
        size = cls.read_int(buff)
        the_dict = {}
        while size > 0:
            k = reader.readObject(buff)
            v = reader.readObject(buff)
            the_dict[k] = v
            size = size - 1

        return the_dict


class UuidIO(_GraphBinaryTypeIO):

    python_type = uuid.UUID
    graphbinary_type = DataType.uuid

    @classmethod
    def dictify(cls, obj, writer):
        ba = bytearray([cls.graphbinary_type.value])
        ba.extend(obj.bytes)
        return ba

    @classmethod
    def objectify(cls, b, reader):
        return uuid.UUID(bytes=b.read(16))


class EdgeIO(_GraphBinaryTypeIO):

    python_type = Edge
    graphbinary_type = DataType.edge

    @classmethod
    def dictify(cls, obj, writer):
        ba = bytearray([cls.graphbinary_type.value])
        ba.extend(writer.writeObject(obj.id))
        ba.extend(cls.string_as_bytes(obj.label))
        ba.extend(writer.writeObject(obj.inV.id))
        ba.extend(cls.string_as_bytes(obj.inV.label))
        ba.extend(writer.writeObject(obj.outV.id))
        ba.extend(cls.string_as_bytes(obj.outV.label))
        ba.extend([DataType.null.value])
        ba.extend([DataType.null.value])
        return ba

    @classmethod
    def objectify(cls, b, reader):
        edgeid = reader.readObject(b)
        edgelbl = cls.read_string(b)
        edge = Edge(edgeid, Vertex(reader.readObject(b), cls.read_string(b)),
                    edgelbl, Vertex(reader.readObject(b), cls.read_string(b)))
        b.read(2)
        return edge


class PathIO(_GraphBinaryTypeIO):

    python_type = Path
    graphbinary_type = DataType.path

    @classmethod
    def dictify(cls, obj, writer):
        ba = bytearray([cls.graphbinary_type.value])
        ba.extend(writer.writeObject(obj.labels))
        ba.extend(writer.writeObject(obj.objects))
        return ba

    @classmethod
    def objectify(cls, b, reader):
        return Path(reader.readObject(b), reader.readObject(b))


class PropertyIO(_GraphBinaryTypeIO):

    python_type = Property
    graphbinary_type = DataType.property

    @classmethod
    def dictify(cls, obj, writer):
        ba = bytearray([cls.graphbinary_type.value])
        ba.extend(cls.string_as_bytes(obj.key))
        ba.extend(writer.writeObject(obj.value))
        ba.extend([DataType.null.value])
        return ba

    @classmethod
    def objectify(cls, b, reader):
        p = Property(cls.read_string(b), reader.readObject(b), None)
        b.read(1)
        return p


class TinkerGraphIO(_GraphBinaryTypeIO):

    python_type = Graph
    graphbinary_type = DataType.tinkergraph

    @classmethod
    def dictify(cls, obj, writer):
        raise AttributeError("TinkerGraph serialization is not currently supported by gremlin-python")

    @classmethod
    def objectify(cls, b, reader):
        raise AttributeError("TinkerGraph deserialization is not currently supported by gremlin-python")


class VertexIO(_GraphBinaryTypeIO):

    python_type = Vertex
    graphbinary_type = DataType.vertex

    @classmethod
    def dictify(cls, obj, writer):
        ba = bytearray([cls.graphbinary_type.value])
        ba.extend(writer.writeObject(obj.id))
        ba.extend(cls.string_as_bytes(obj.label))
        ba.extend([DataType.null.value])
        return ba

    @classmethod
    def objectify(cls, b, reader):
        vertex = Vertex(reader.readObject(b), cls.read_string(b))
        b.read(1)
        return vertex


class VertexPropertyIO(_GraphBinaryTypeIO):

    python_type = VertexProperty
    graphbinary_type = DataType.vertexproperty

    @classmethod
    def dictify(cls, obj, writer):
        ba = bytearray([cls.graphbinary_type.value])
        ba.extend(writer.writeObject(obj.id))
        ba.extend(cls.string_as_bytes(obj.label))
        ba.extend(writer.writeObject(obj.value))
        ba.extend([DataType.null.value])
        ba.extend([DataType.null.value])
        return ba

    @classmethod
    def objectify(cls, b, reader):
        vp = VertexProperty(reader.readObject(b), cls.read_string(b), reader.readObject(b), None)
        b.read(1)
        b.read(1)
        return vp