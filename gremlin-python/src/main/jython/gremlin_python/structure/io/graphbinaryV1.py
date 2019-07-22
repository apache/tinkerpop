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
from gremlin_python.structure.graph import Edge, Property, Vertex, VertexProperty, Path

log = logging.getLogger(__name__)

# When we fall back to a superclass's serializer, we iterate over this map.
# We want that iteration order to be consistent, so we use an OrderedDict,
# not a dict.
_serializers = OrderedDict()
_deserializers = {}


class DataType(Enum):
    int = 0x01
    long = 0x02
    string = 0x03
    list = 0x09


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
    def as_bytes(cls, graphbin_type, size=None, *args):
        ba = bytearray([graphbin_type.value])
        if size is not None:
            ba.extend(struct.pack(">i", size))

        for arg in args:
            if isinstance(arg, str):
                ba.extend(arg.encode("utf-8"))
            elif isinstance(arg, (bytes, bytearray)):
                ba.extend(arg)
        return ba

    @classmethod
    def read_int(cls, buff):
        return struct.unpack(">i", buff.read(4))[0]

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
    def dictify(cls, n, writer):
        if n < -9223372036854775808 or n > 9223372036854775807:
            raise Exception("TODO: don't forget bigint")
        else:
            return cls.as_bytes(cls.graphbinary_type, None, struct.pack(cls.byte_format, n))

    @classmethod
    def objectify(cls, buff, reader):
        return struct.unpack(">q", buff.read(8))[0]


class IntIO(LongIO):

    python_type = IntType
    graphbinary_type = DataType.int
    byte_format = ">i"

    @classmethod
    def objectify(cls, buff, reader):
        return cls.read_int(buff)


class StringIO(_GraphBinaryTypeIO):

    python_type = str
    graphbinary_type = DataType.string

    @classmethod
    def dictify(cls, n, writer):
        return cls.as_bytes(cls.graphbinary_type, len(n), n)

    @classmethod
    def objectify(cls, b, reader):
        return b.read(cls.read_int(b)).decode("utf-8")


class ListIO(_GraphBinaryTypeIO):

    python_type = list
    graphbinary_type = DataType.list

    @classmethod
    def dictify(cls, n, writer):
        list_data = bytearray()
        for item in n:
            list_data.extend(writer.writeObject(item))

        return cls.as_bytes(cls.graphbinary_type, len(n), list_data)

    @classmethod
    def objectify(cls, buff, reader):
        size = cls.read_int(buff)
        the_list = []
        while size > 0:
            the_list.append(reader.readObject(buff))
            size = size - 1

        return the_list
