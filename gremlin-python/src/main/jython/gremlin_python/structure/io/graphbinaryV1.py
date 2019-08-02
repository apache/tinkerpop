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
from gremlin_python.statics import FloatType, FunctionType, IntType, LongType, TypeType, DictType, ListType, SetType, \
                                   SingleByte, ByteBufferType, SingleChar
from gremlin_python.process.traversal import Barrier, Binding, Bytecode, Cardinality, Column, Direction, Operator, \
                                             Order, Pick, Pop, P, Scope, TextP, Traversal, Traverser, \
                                             TraversalStrategy, T
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
    graph = 0x10
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
    bigdecimal = 0x22    #todo
    biginteger = 0x23    #todo
    byte = 0x24
    bytebuffer = 0x25


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
                 "set_": "set", "list_": "list", "all_": "all", "with_": "with",
                 "filter_": "filter", "id_": "id", "max_": "max", "min_": "min", "sum_": "sum"}

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
    graphbinary_type = DataType.graph

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


class _EnumIO(_GraphBinaryTypeIO):

    @classmethod
    def dictify(cls, obj, writer):
        ba = bytearray([cls.graphbinary_type.value])
        ba.extend(cls.string_as_bytes(cls.unmangleKeyword(str(obj.name))))
        return ba

    @classmethod
    def objectify(cls, b, reader):
        return cls.python_type[cls.read_string(b)]


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
    def dictify(cls, obj, writer):
        ba = bytearray([cls.graphbinary_type.value])
        ba.extend(cls.string_as_bytes(obj.key))
        ba.extend(writer.writeObject(obj.value))
        return ba
    
    @classmethod
    def objectify(cls, b, reader):
        return Binding(cls.read_string(b), reader.readObject(b))


class BytecodeIO(_GraphBinaryTypeIO):
    python_type = Bytecode
    graphbinary_type = DataType.bytecode
    
    @classmethod
    def dictify(cls, obj, writer):
        ba = bytearray([cls.graphbinary_type.value])
        ba.extend(struct.pack(">i", len(obj.step_instructions)))
        for inst in obj.step_instructions:
            inst_name, inst_args = inst[0], inst[1:] if len(inst) > 1 else []
            ba.extend(cls.string_as_bytes(inst_name))
            ba.extend(struct.pack(">i", len(inst_args)))
            for arg in inst_args:
                ba.extend(writer.writeObject(arg))

        ba.extend(struct.pack(">i", len(obj.source_instructions)))
        for inst in obj.source_instructions:
            inst_name, inst_args = inst[0], inst[1:] if len(inst) > 1 else []
            ba.extend(cls.string_as_bytes(inst_name))
            ba.extend(struct.pack(">i", len(inst_args)))
            for arg in inst_args:
                ba.extend(writer.writeObject(arg))
                
        return ba
    
    @classmethod
    def objectify(cls, b, reader):
        bytecode = Bytecode()
        
        step_count = cls.read_int(b)
        ix = 0
        while ix < step_count:
            inst = [cls.read_string(b)]
            inst_ct = cls.read_int(b)
            iy = 0
            while iy < inst_ct:
                inst.append(reader.readObject(b))
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
                inst.append(reader.readObject(b))
                iy += 1
            bytecode.source_instructions.append(inst)
            ix += 1

        return bytecode


class LambdaIO(_GraphBinaryTypeIO):

    python_type = FunctionType
    graphbinary_type = DataType.lambda_

    @classmethod
    def dictify(cls, obj, writer):
        ba = bytearray([cls.graphbinary_type.value])
        lambda_result = obj()
        script = lambda_result if isinstance(lambda_result, str) else lambda_result[0]
        language = statics.default_lambda_language if isinstance(lambda_result, str) else lambda_result[1]

        ba.extend(cls.string_as_bytes(language))

        script_cleaned = script
        script_args = -1

        if language == "gremlin-jython" or language == "gremlin-python":
            if not script.strip().startswith("lambda"):
                script_cleaned = "lambda " + script
            script_args = six.get_function_code(eval(script_cleaned)).co_argcount

        ba.extend(cls.string_as_bytes(script_cleaned))
        ba.extend(struct.pack(">i", script_args))

        return ba


class PIO(_GraphBinaryTypeIO):
    graphbinary_type = DataType.p
    python_type = P

    @classmethod
    def dictify(cls, obj, writer):
        ba = bytearray([cls.graphbinary_type.value])
        ba.extend(cls.string_as_bytes(obj.operator))
        additional = [writer.writeObject(obj.value), writer.writeObject(obj.other)] \
            if obj.other is not None else [writer.writeObject(obj.value)]
        ba.extend(struct.pack(">i", len(additional)))
        for a in additional:
            ba.extend(a)

        return ba


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
    def dictify(cls, obj, writer):
        ba = bytearray([cls.graphbinary_type.value])
        ba.extend(struct.pack(">i", obj.bulk))
        ba.extend(writer.writeObject(obj.object))

        return ba


class ByteIO(_GraphBinaryTypeIO):
    python_type = SingleByte
    graphbinary_type = DataType.byte

    @classmethod
    def dictify(cls, obj, writer):
        ba = bytearray([cls.graphbinary_type.value])
        ba.extend(struct.pack(">b", obj))
        return ba

    @classmethod
    def objectify(cls, b, reader):
        return int.__new__(SingleByte, struct.unpack_from(">b", b.read(1))[0])


class ByteBufferIO(_GraphBinaryTypeIO):
    python_type = ByteBufferType
    graphbinary_type = DataType.bytebuffer

    @classmethod
    def dictify(cls, obj, writer):
        return cls.as_bytes(cls.graphbinary_type, len(obj), obj)

    @classmethod
    def objectify(cls, b, reader):
        size = cls.read_int(b)
        return ByteBufferType(b.read(size))


