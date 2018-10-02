'''
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
'''
import datetime
import json
import time
import uuid
import math
from collections import OrderedDict

import six
from aenum import Enum

from gremlin_python import statics
from gremlin_python.statics import FloatType, FunctionType, IntType, LongType, TypeType
from gremlin_python.process.traversal import Binding, Bytecode, P, Traversal, Traverser, TraversalStrategy
from gremlin_python.structure.graph import Edge, Property, Vertex, VertexProperty, Path

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
    def typedValue(cls, type_name, value, prefix="g"):
        out = {cls.TYPE_KEY: cls.formatType(prefix, type_name)}
        if value is not None:
            out[cls.VALUE_KEY] = value
        return out

    @classmethod
    def formatType(cls, prefix, type_name):
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

    def writeObject(self, objectData):
        # to JSON
        return json.dumps(self.toDict(objectData), separators=(',', ':'))

    def toDict(self, obj):
        """
        Encodes python objects in GraphSON type-tagged dict values
        """
        try:
            return self.serializers[type(obj)].dictify(obj, self)
        except KeyError:
            for key, serializer in self.serializers.items():
                if isinstance(obj, key):
                    return serializer.dictify(obj, self)

        # list and map are treated as normal json objs (could be isolated serializers)
        if isinstance(obj, (list, set)):
            return [self.toDict(o) for o in obj]
        elif isinstance(obj, dict):
            return dict((self.toDict(k), self.toDict(v)) for k, v in obj.items())
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

    def readObject(self, jsonData):
        # from JSON
        return self.toObject(json.loads(jsonData))

    def toObject(self, obj):
        """
        Unpacks GraphSON type-tagged dict values into objects mapped in self.deserializers
        """
        if isinstance(obj, dict):
            try:
                return self.deserializers[obj[GraphSONUtil.TYPE_KEY]].objectify(obj[GraphSONUtil.VALUE_KEY], self)
            except KeyError:
                pass
            # list and map are treated as normal json objs (could be isolated deserializers)
            return dict((self.toObject(k), self.toObject(v)) for k, v in obj.items())
        elif isinstance(obj, list):
            return [self.toObject(o) for o in obj]
        else:
            return obj


@six.add_metaclass(GraphSONTypeType)
class _GraphSONTypeIO(object):
    python_type = None
    graphson_type = None

    symbolMap = {"global_": "global", "as_": "as", "in_": "in", "and_": "and",
                 "or_": "or", "is_": "is", "not_": "not", "from_": "from",
                 "set_": "set", "list_": "list", "all_": "all"}

    @classmethod
    def unmangleKeyword(cls, symbol):
        return cls.symbolMap.get(symbol, symbol)

    def dictify(self, obj, writer):
        raise NotImplementedError()

    def objectify(self, d, reader):
        raise NotImplementedError()


class _BytecodeSerializer(_GraphSONTypeIO):
    @classmethod
    def _dictify_instructions(cls, instructions, writer):
        out = []
        for instruction in instructions:
            inst = [instruction[0]]
            inst.extend(writer.toDict(arg) for arg in instruction[1:])
            out.append(inst)
        return out

    @classmethod
    def dictify(cls, bytecode, writer):
        if isinstance(bytecode, Traversal):
            bytecode = bytecode.bytecode
        out = {}
        if bytecode.source_instructions:
            out["source"] = cls._dictify_instructions(bytecode.source_instructions, writer)
        if bytecode.step_instructions:
            out["step"] = cls._dictify_instructions(bytecode.step_instructions, writer)
        return GraphSONUtil.typedValue("Bytecode", out)


class VertexSerializer(_GraphSONTypeIO):
    python_type = Vertex

    @classmethod
    def dictify(cls, vertex, writer):
        v = {"id": writer.toDict(vertex.id),
             "label": vertex.label}
        return GraphSONUtil.typedValue("Vertex", v)


class EdgeSerializer(_GraphSONTypeIO):
    python_type = Edge

    @classmethod
    def dictify(cls, edge, writer):
        e = {"id": edge.id,
             "label": edge.label,
             "inV": writer.toDict(edge.inV.id),
             "outV": writer.toDict(edge.outV.id)}
        return GraphSONUtil.typedValue("Edge", e)


class VertexPropertySerializer(_GraphSONTypeIO):
    python_type = VertexProperty

    @classmethod
    def dictify(cls, vertex_property, writer):
        vp = {"id": vertex_property.id,
              "label": vertex_property.label,
              "value": writer.toDict(vertex_property.value)}
        return GraphSONUtil.typedValue("VertexProperty", vp)


class PropertySerializer(_GraphSONTypeIO):
    python_type = Property

    @classmethod
    def dictify(cls, property, writer):
        p = {"key": writer.toDict(property.key),
             "value": writer.toDict(property.value)}
        return GraphSONUtil.typedValue("Property", p)


class TraversalSerializer(_BytecodeSerializer):
    python_type = Traversal


class BytecodeSerializer(_BytecodeSerializer):
    python_type = Bytecode


class TraversalStrategySerializer(_GraphSONTypeIO):
    python_type = TraversalStrategy

    @classmethod
    def dictify(cls, strategy, writer):
        return GraphSONUtil.typedValue(strategy.strategy_name, writer.toDict(strategy.configuration))


class TraverserIO(_GraphSONTypeIO):
    python_type = Traverser
    graphson_type = "g:Traverser"

    @classmethod
    def dictify(cls, traverser, writer):
        return GraphSONUtil.typedValue("Traverser", {"value": writer.toDict(traverser.object),
                                                     "bulk": writer.toDict(traverser.bulk)})

    @classmethod
    def objectify(cls, d, reader):
        return Traverser(reader.toObject(d["value"]),
                         reader.toObject(d["bulk"]))


class EnumSerializer(_GraphSONTypeIO):
    python_type = Enum

    @classmethod
    def dictify(cls, enum, _):
        return GraphSONUtil.typedValue(cls.unmangleKeyword(type(enum).__name__),
                                       cls.unmangleKeyword(str(enum.name)))


class PSerializer(_GraphSONTypeIO):
    python_type = P

    @classmethod
    def dictify(cls, p, writer):
        out = {"predicate": p.operator,
               "value": [writer.toDict(p.value), writer.toDict(p.other)] if p.other is not None else
               writer.toDict(p.value)}
        return GraphSONUtil.typedValue("P", out)


class BindingSerializer(_GraphSONTypeIO):
    python_type = Binding

    @classmethod
    def dictify(cls, binding, writer):
        out = {"key": binding.key,
               "value": writer.toDict(binding.value)}
        return GraphSONUtil.typedValue("Binding", out)


class LambdaSerializer(_GraphSONTypeIO):
    python_type = FunctionType

    @classmethod
    def dictify(cls, lambda_object, writer):
        lambda_result = lambda_object()
        script = lambda_result if isinstance(lambda_result, str) else lambda_result[0]
        language = statics.default_lambda_language if isinstance(lambda_result, str) else lambda_result[1]
        out = {"script": script,
               "language": language}
        if language == "gremlin-jython" or language == "gremlin-python":
            if not script.strip().startswith("lambda"):
                script = "lambda " + script
                out["script"] = script
            out["arguments"] = six.get_function_code(eval(out["script"])).co_argcount
        else:
            out["arguments"] = -1
        return GraphSONUtil.typedValue("Lambda", out)


class TypeSerializer(_GraphSONTypeIO):
    python_type = TypeType

    @classmethod
    def dictify(cls, typ, writer):
        return writer.toDict(typ())


class UUIDIO(_GraphSONTypeIO):
    python_type = uuid.UUID
    graphson_type = "g:UUID"
    graphson_base_type = "UUID"

    @classmethod
    def dictify(cls, obj, writer):
        return GraphSONUtil.typedValue(cls.graphson_base_type, str(obj))

    @classmethod
    def objectify(cls, d, reader):
        return cls.python_type(d)


class DateIO(_GraphSONTypeIO):
    python_type = datetime.datetime
    graphson_type = "g:Date"
    graphson_base_type = "Date"

    @classmethod
    def dictify(cls, obj, writer):
        # Java timestamp expects miliseconds
        if six.PY3:
            pts = obj.timestamp()
        else:
            # Hack for legacy Python
            # timestamp() in Python 3.3
            pts = time.mktime((obj.year, obj.month, obj.day,
			                   obj.hour, obj.minute, obj.second,
			                   -1, -1, -1)) + obj.microsecond / 1e6

        # Have to use int because of legacy Python
        ts = int(round(pts * 1000))
        return GraphSONUtil.typedValue(cls.graphson_base_type, ts)

    @classmethod
    def objectify(cls, ts, reader):
        # Python timestamp expects seconds
        return datetime.datetime.fromtimestamp(ts / 1000.0)


# Based on current implementation, this class must always be declared before FloatIO.
# Seems pretty fragile for future maintainers. Maybe look into this.
class TimestampIO(_GraphSONTypeIO):
    """A timestamp in Python is type float"""
    python_type = statics.timestamp
    graphson_type = "g:Timestamp"
    graphson_base_type = "Timestamp"

    @classmethod
    def dictify(cls, obj, writer):
        # Java timestamp expects milliseconds integer
        # Have to use int because of legacy Python
        ts = int(round(obj * 1000))
        return GraphSONUtil.typedValue(cls.graphson_base_type, ts)

    @classmethod
    def objectify(cls, ts, reader):
        # Python timestamp expects seconds
        return cls.python_type(ts / 1000.0)


class _NumberIO(_GraphSONTypeIO):
    @classmethod
    def dictify(cls, n, writer):
        if isinstance(n, bool):  # because isinstance(False, int) and isinstance(True, int)
            return n
        return GraphSONUtil.typedValue(cls.graphson_base_type, n)

    @classmethod
    def objectify(cls, v, _):
        return cls.python_type(v)


class FloatIO(_NumberIO):
    python_type = FloatType
    graphson_type = "g:Float"
    graphson_base_type = "Float"

    @classmethod
    def dictify(cls, n, writer):
        if isinstance(n, bool):  # because isinstance(False, int) and isinstance(True, int)
            return n
        elif math.isnan(n):
            return GraphSONUtil.typedValue(cls.graphson_base_type, "NaN")
        elif math.isinf(n) and n > 0:
            return GraphSONUtil.typedValue(cls.graphson_base_type, "Infinity")
        elif math.isinf(n) and n < 0:
            return GraphSONUtil.typedValue(cls.graphson_base_type, "-Infinity")
        else:
            return GraphSONUtil.typedValue(cls.graphson_base_type, n)

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


class DoubleIO(FloatIO):
    graphson_type = "g:Double"
    graphson_base_type = "Double"


class Int64IO(_NumberIO):
    python_type = LongType
    graphson_type = "g:Int64"
    graphson_base_type = "Int64"


class Int32IO(_NumberIO):
    python_type = IntType
    graphson_type = "g:Int32"
    graphson_base_type = "Int32"

    @classmethod
    def dictify(cls, n, writer):
        if isinstance(n, bool):
            return n
        return GraphSONUtil.typedValue(cls.graphson_base_type, n)


class VertexDeserializer(_GraphSONTypeIO):
    graphson_type = "g:Vertex"

    @classmethod
    def objectify(cls, d, reader):
        return Vertex(reader.toObject(d["id"]), d.get("label", ""))


class EdgeDeserializer(_GraphSONTypeIO):
    graphson_type = "g:Edge"

    @classmethod
    def objectify(cls, d, reader):
        return Edge(reader.toObject(d["id"]),
                    Vertex(reader.toObject(d["outV"]), ""),
                    d.get("label", "vertex"),
                    Vertex(reader.toObject(d["inV"]), ""))


class VertexPropertyDeserializer(_GraphSONTypeIO):
    graphson_type = "g:VertexProperty"

    @classmethod
    def objectify(cls, d, reader):
        return VertexProperty(reader.toObject(d["id"]), d["label"],
                              reader.toObject(d["value"]))


class PropertyDeserializer(_GraphSONTypeIO):
    graphson_type = "g:Property"

    @classmethod
    def objectify(cls, d, reader):
        return Property(d["key"], reader.toObject(d["value"]))


class PathDeserializer(_GraphSONTypeIO):
    graphson_type = "g:Path"

    @classmethod
    def objectify(cls, d, reader):
        labels = [set(label) for label in d["labels"]]
        objects = [reader.toObject(o) for o in d["objects"]]
        return Path(labels, objects)
