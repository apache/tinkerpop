# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import base64
import calendar
import datetime
import json
import uuid
import math
from collections import OrderedDict
from decimal import *
import logging
from datetime import timedelta

from aenum import Enum
from isodate import parse_duration, duration_isoformat

from gremlin_python import statics
from gremlin_python.statics import FloatType, BigDecimal, ShortType, IntType, LongType, TypeType, DictType, ListType, SetType, SingleByte, SingleChar, to_bigdecimal
from gremlin_python.process.traversal import Direction, P, TextP, Traversal, Traverser, TraversalStrategy, T
from gremlin_python.structure.graph import Edge, Property, Vertex, VertexProperty, Path
from gremlin_python.structure.io.util import HashableDict, SymbolUtil

log = logging.getLogger(__name__)

# When we fall back to a superclass's serializer, we iterate over this map.
# We want that iteration order to be consistent, so we use an OrderedDict,
# not a dict.
_serializers = OrderedDict()
_deserializers = {}


class GraphSONTypeType(type):
    def __new__(mcs, name, bases, dct):
        cls = super(GraphSONTypeType, mcs).__new__(mcs, name, bases, dct)
        if not name.startswith('_'):
            if cls.python_type:
                _serializers[cls.python_type] = cls
            if cls.graphson_type:
                _deserializers[cls.graphson_type] = cls
        return cls


class GraphSONUtil(object):
    TYPE_KEY = "@type"
    VALUE_KEY = "@value"

    @classmethod
    def typed_value(cls, type_name, value, prefix="g"):
        out = {cls.TYPE_KEY: cls.format_type(prefix, type_name)}
        if value is not None:
            out[cls.VALUE_KEY] = value
        return out

    @classmethod
    def format_type(cls, prefix, type_name):
        return "%s:%s" % (prefix, type_name)


# Read/Write classes split to follow precedence of the Java API
class GraphSONWriter(object):
    def __init__(self, serializer_map=None):
        """
        :param serializer_map: map from Python type to serializer instance implementing `dictify`
        """
        self.serializers = _serializers.copy()
        if serializer_map:
            self.serializers.update(serializer_map)

    def write_object(self, objectData):
        # to JSON
        return json.dumps(self.to_dict(objectData), separators=(',', ':'))

    def to_dict(self, obj):
        """
        Encodes python objects in GraphSON type-tagged dict values
        """
        try:
            return self.serializers[type(obj)].dictify(obj, self)
        except KeyError:
            for key, serializer in self.serializers.items():
                if isinstance(obj, key):
                    return serializer.dictify(obj, self)

        if isinstance(obj, dict):
            return dict((self.to_dict(k), self.to_dict(v)) for k, v in obj.items())
        elif isinstance(obj, set):
            return set([self.to_dict(o) for o in obj])
        elif isinstance(obj, list):
            return [self.to_dict(o) for o in obj]
        else:
            return obj


class GraphSONReader(object):
    def __init__(self, deserializer_map=None):
        """
        :param deserializer_map: map from GraphSON type tag to deserializer instance implementing `objectify`
        """
        self.deserializers = _deserializers.copy()
        if deserializer_map:
            self.deserializers.update(deserializer_map)

    def read_object(self, json_data):
        # from JSON
        return self.to_object(json.loads(json_data))

    def to_object(self, obj):
        """
        Unpacks GraphSON type-tagged dict values into objects mapped in self.deserializers
        """
        if isinstance(obj, dict):
            try:
                return self.deserializers[obj[GraphSONUtil.TYPE_KEY]].objectify(obj[GraphSONUtil.VALUE_KEY], self)
            except KeyError:
                pass
            return dict((self.to_object(k), self.to_object(v)) for k, v in obj.items())
        elif isinstance(obj, set):
            return set([self.to_object(o) for o in obj])
        elif isinstance(obj, list):
            return [self.to_object(o) for o in obj]
        else:
            return obj


class _GraphSONTypeIO(object, metaclass=GraphSONTypeType):
    python_type = None
    graphson_type = None

    def dictify(self, obj, writer):
        raise NotImplementedError()

    def objectify(self, d, reader):
        raise NotImplementedError()


class VertexSerializer(_GraphSONTypeIO):
    python_type = Vertex
    graphson_type = "g:Vertex"

    @classmethod
    def dictify(cls, vertex, writer):
        return GraphSONUtil.typed_value("Vertex", {"id": writer.to_dict(vertex.id),
                                                  "label": [writer.to_dict(vertex.label)]})


class EdgeSerializer(_GraphSONTypeIO):
    python_type = Edge
    graphson_type = "g:Edge"

    @classmethod
    def dictify(cls, edge, writer):
        return GraphSONUtil.typed_value("Edge", {"id": writer.to_dict(edge.id),
                                                 "label": [writer.to_dict(edge.label)],
                                                 "inV": {
                                                     "id": writer.to_dict(edge.inV.id),
                                                     "label": [writer.to_dict(edge.inV.label)]
                                                 },
                                                 "outV": {
                                                     "id": writer.to_dict(edge.outV.id),
                                                     "label": [writer.to_dict(edge.outV.label)]
                                                 }})


class VertexPropertySerializer(_GraphSONTypeIO):
    python_type = VertexProperty
    graphson_type = "g:VertexProperty"

    @classmethod
    def dictify(cls, vertex_property, writer):
        return GraphSONUtil.typed_value("VertexProperty", {"id": writer.to_dict(vertex_property.id),
                                                           "label": [writer.to_dict(vertex_property.label)],
                                                           "value": writer.to_dict(vertex_property.value)})


class PropertySerializer(_GraphSONTypeIO):
    python_type = Property
    graphson_type = "g:Property"

    @classmethod
    def dictify(cls, property, writer):
        return GraphSONUtil.typed_value("Property", {"key": writer.to_dict(property.key),
                                                    "value": writer.to_dict(property.value)})


class UUIDIO(_GraphSONTypeIO):
    python_type = uuid.UUID
    graphson_type = "g:UUID"
    graphson_base_type = "UUID"

    @classmethod
    def dictify(cls, obj, writer):
        return GraphSONUtil.typed_value(cls.graphson_base_type, str(obj))

    @classmethod
    def objectify(cls, d, reader):
        return cls.python_type(d)


class DateTimeIO(_GraphSONTypeIO):
    python_type = datetime.datetime
    graphson_type = "g:DateTime"
    graphson_base_type = "DateTime"

    @classmethod
    def dictify(cls, obj, writer):
        if obj.tzinfo is None:
            raise AttributeError("Timezone information is required when constructing datetime")
        return GraphSONUtil.typed_value(cls.graphson_base_type, obj.isoformat())

    @classmethod
    def objectify(cls, dt, reader):
        # specially handling as python isoformat does not support zulu until 3.11
        dt_iso = dt[:-1] + '+00:00' if dt.endswith('Z') else dt
        return datetime.datetime.fromisoformat(dt_iso)


class _NumberIO(_GraphSONTypeIO):
    @classmethod
    def dictify(cls, n, writer):
        if isinstance(n, bool):  # because isinstance(False, int) and isinstance(True, int)
            return n
        return GraphSONUtil.typed_value(cls.graphson_base_type, n)

    @classmethod
    def objectify(cls, v, _):
        return cls.python_type(v)


class ListIO(_GraphSONTypeIO):
    python_type = ListType
    graphson_type = "g:List"

    @classmethod
    def dictify(cls, l, writer):
        new_list = []
        for obj in l:
            new_list.append(writer.to_dict(obj))
        return GraphSONUtil.typed_value("List", new_list)

    @classmethod
    def objectify(cls, l, reader):
        new_list = []
        for obj in l:
            new_list.append(reader.to_object(obj))
        return new_list


class SetIO(_GraphSONTypeIO):
    python_type = SetType
    graphson_type = "g:Set"

    @classmethod
    def dictify(cls, s, writer):
        new_list = []
        for obj in s:
            new_list.append(writer.to_dict(obj))
        return GraphSONUtil.typed_value("Set", new_list)

    @classmethod
    def objectify(cls, s, reader):
        """
        By default, returns a python set

        In case Java returns numeric values of different types which
        python don't recognize, coerce and return a list.
        See comments of TINKERPOP-1844 for more details
        """
        new_list = [reader.to_object(obj) for obj in s]
        new_set = set(new_list)
        if len(new_list) != len(new_set):
            log.warning("Coercing g:Set to list due to java numeric values. "
                        "See TINKERPOP-1844 for more details.")
            return new_list

        return new_set


class MapType(_GraphSONTypeIO):
    python_type = DictType
    graphson_type = "g:Map"

    @classmethod
    def dictify(cls, d, writer):
        l = []
        for key in d:
            l.append(writer.to_dict(key))
            l.append(writer.to_dict(d[key]))
        return GraphSONUtil.typed_value("Map", l)

    @classmethod
    def objectify(cls, l, reader):
        new_dict = {}
        if len(l) > 0:
            x = 0
            while x < len(l):
                new_dict[HashableDict.of(reader.to_object(l[x]))] = reader.to_object(l[x + 1])
                x = x + 2
        return new_dict


class FloatIO(_NumberIO):
    python_type = FloatType
    graphson_type = "g:Float"
    graphson_base_type = "Float"

    @classmethod
    def dictify(cls, n, writer):
        if isinstance(n, bool):  # because isinstance(False, int) and isinstance(True, int)
            return n
        elif math.isnan(n):
            return GraphSONUtil.typed_value(cls.graphson_base_type, "NaN")
        elif math.isinf(n) and n > 0:
            return GraphSONUtil.typed_value(cls.graphson_base_type, "Infinity")
        elif math.isinf(n) and n < 0:
            return GraphSONUtil.typed_value(cls.graphson_base_type, "-Infinity")
        else:
            return GraphSONUtil.typed_value(cls.graphson_base_type, n)

    @classmethod
    def objectify(cls, v, _):
        if isinstance(v, str):
            if v == 'NaN':
                return float('nan')
            elif v == "Infinity":
                return float('inf')
            elif v == "-Infinity":
                return float('-inf')

        return cls.python_type(v)


class BigDecimalIO(_GraphSONTypeIO):
    python_type = BigDecimal
    graphson_type = "g:BigDecimal"
    graphson_base_type = "BigDecimal"

    @classmethod
    def dictify(cls, n, writer):
        return GraphSONUtil.typed_value(cls.graphson_base_type, str(n.value), "g")

    @classmethod
    def objectify(cls, v, _):
        return to_bigdecimal(v)


class DoubleIO(FloatIO):
    graphson_type = "g:Double"
    graphson_base_type = "Double"


class Int64IO(_NumberIO):
    python_type = LongType
    graphson_type = "g:Int64"
    graphson_base_type = "Int64"

    @classmethod
    def dictify(cls, n, writer):
        # if we exceed Java long range then we need a BigInteger
        if isinstance(n, bool):
            return n
        elif n < -9223372036854775808 or n > 9223372036854775807:
            return GraphSONUtil.typed_value("BigInteger", str(n), "g")
        else:
            return GraphSONUtil.typed_value(cls.graphson_base_type, n)


class BigIntegerIO(Int64IO):
    graphson_type = "g:BigInteger"


class Int32IO(Int64IO):
    python_type = IntType
    graphson_type = "g:Int32"
    graphson_base_type = "Int32"

    @classmethod
    def dictify(cls, n, writer):
        # if we exceed Java int range then we need a long
        if isinstance(n, bool):
            return n
        elif n < -9223372036854775808 or n > 9223372036854775807:
            return GraphSONUtil.typed_value("BigInteger", str(n), "g")
        elif n < -2147483648 or n > 2147483647:
            return GraphSONUtil.typed_value("Int64", n)
        else:
            return GraphSONUtil.typed_value(cls.graphson_base_type, n)


class Int16IO(Int64IO):
    python_type = ShortType
    graphson_type = "g:Int16"
    graphson_base_type = "Int16"

    @classmethod
    def dictify(cls, n, writer):
        # if we exceed Java int range then we need a long
        if isinstance(n, bool):
            return n
        elif n < -9223372036854775808 or n > 9223372036854775807:
            return GraphSONUtil.typed_value("BigInteger", str(n), "g")
        elif n < -2147483648 or n > 2147483647:
            return GraphSONUtil.typed_value("Int64", n)
        elif n < -32768 or n > 32767:
            return GraphSONUtil.typed_value("Int32", n)
        else:
            return GraphSONUtil.typed_value(cls.graphson_base_type, n, "g")

    @classmethod
    def objectify(cls, v, _):
        return int.__new__(ShortType, v)


class ByteIO(_NumberIO):
    python_type = SingleByte
    graphson_type = "g:Byte"
    graphson_base_type = "Byte"

    @classmethod
    def dictify(cls, n, writer):
        if isinstance(n, bool):  # because isinstance(False, int) and isinstance(True, int)
            return n
        return GraphSONUtil.typed_value(cls.graphson_base_type, n, "g")

    @classmethod
    def objectify(cls, v, _):
        return int.__new__(SingleByte, v)


class BinaryIO(_GraphSONTypeIO):
    python_type = bytes
    graphson_type = "g:Binary"
    graphson_base_type = "Binary"

    @classmethod
    def dictify(cls, n, writer):
        # writes a JSON String containing base64-encoded bytes
        n_encoded = "".join(chr(x) for x in base64.b64encode(n))
        return GraphSONUtil.typed_value(cls.graphson_base_type, n_encoded, "g")

    @classmethod
    def objectify(cls, v, _):
        return base64.b64decode(v)


class CharIO(_GraphSONTypeIO):
    python_type = SingleChar
    graphson_type = "g:Char"
    graphson_base_type = "Char"

    @classmethod
    def dictify(cls, n, writer):
        return GraphSONUtil.typed_value(cls.graphson_base_type, n, "g")

    @classmethod
    def objectify(cls, v, _):
        return str.__new__(SingleChar, v)


class DurationIO(_GraphSONTypeIO):
    python_type = timedelta
    graphson_type = "g:Duration"
    graphson_base_type = "Duration"

    @classmethod
    def dictify(cls, n, writer):
        n_iso = duration_isoformat(n)
        return GraphSONUtil.typed_value(cls.graphson_base_type, n_iso[:-2] + 'T0S' if n_iso.endswith('0D') else n_iso, "g")

    @classmethod
    def objectify(cls, v, _):
        return parse_duration(v)


class VertexDeserializer(_GraphSONTypeIO):
    graphson_type = "g:Vertex"

    @classmethod
    def objectify(cls, d, reader):
        properties = None
        if "properties" in d:
            properties = reader.to_object(d["properties"])
            if properties is not None:
                properties = [item for sublist in properties.values() for item in sublist]
        label = d.get("label", "vertex")
        return Vertex(reader.to_object(d["id"]), label if isinstance(label, str) else label[0], properties)


class EdgeDeserializer(_GraphSONTypeIO):
    graphson_type = "g:Edge"

    @classmethod
    def objectify(cls, d, reader):
        properties = None
        if "properties" in d:
            properties = reader.to_object(d["properties"])
            if properties is not None:
                properties = [item for sublist in properties.values() for item in sublist]

        return Edge(reader.to_object(d["id"]),
                    Vertex(reader.to_object(d["outV"]).get("id"), reader.to_object(d["outV"]).get("label")[0]),
                    d.get("label", "edge")[0],
                    Vertex(reader.to_object(d["inV"]).get("id"), reader.to_object(d["outV"]).get("label")[0]),
                    properties)


class VertexPropertyDeserializer(_GraphSONTypeIO):
    graphson_type = "g:VertexProperty"

    @classmethod
    def objectify(cls, d, reader):
        properties = None
        if "properties" in d:
            properties = reader.to_object(d["properties"])
            if properties is not None:
                properties = list(map(lambda x: Property(x[0], x[1], None), properties.items()))
        vertex = Vertex(reader.to_object(d.get("vertex"))) if "vertex" in d else None
        return VertexProperty(reader.to_object(d["id"]),
                              d["label"][0],
                              reader.to_object(d["value"]),
                              vertex,
                              properties)


class PropertyDeserializer(_GraphSONTypeIO):
    graphson_type = "g:Property"

    @classmethod
    def objectify(cls, d, reader):
        element = reader.to_object(d["element"]) if "element" in d else None
        return Property(d["key"], reader.to_object(d["value"]), element)


class PathDeserializer(_GraphSONTypeIO):
    graphson_type = "g:Path"

    @classmethod
    def objectify(cls, d, reader):
        return Path(reader.to_object(d["labels"]), reader.to_object(d["objects"]))


class TIO(_GraphSONTypeIO):
    graphson_type = "g:T"
    graphson_base_type = "T"
    python_type = T

    @classmethod
    def dictify(cls, t, writer):
        return GraphSONUtil.typed_value(cls.graphson_base_type, t.name, "g")
    @classmethod
    def objectify(cls, d, reader):
        return T[d]


class DirectionIO(_GraphSONTypeIO):
    graphson_type = "g:Direction"
    graphson_base_type = "Direction"
    python_type = Direction

    @classmethod
    def dictify(cls, d, writer):
        return GraphSONUtil.typed_value(cls.graphson_base_type, d.name, "g")

    @classmethod
    def objectify(cls, d, reader):
        return Direction[d]


class EnumSerializer(_GraphSONTypeIO):
    python_type = Enum

    @classmethod
    def dictify(cls, enum, _):
        return GraphSONUtil.typed_value(SymbolUtil.to_camel_case(type(enum).__name__),
                                        SymbolUtil.to_camel_case(str(enum.name)))
