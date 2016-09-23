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
import json
from abc import abstractmethod
from aenum import Enum

import six

from gremlin_python import statics
from gremlin_python.statics import (
    FloatType, FunctionType, IntType, LongType, long, TypeType)
from gremlin_python.process.traversal import Binding
from gremlin_python.process.traversal import Bytecode
from gremlin_python.process.traversal import P
from gremlin_python.process.traversal import Traversal
from gremlin_python.process.traversal import Traverser
from gremlin_python.process.traversal import TraversalStrategy
from gremlin_python.structure.graph import Edge
from gremlin_python.structure.graph import Property
from gremlin_python.structure.graph import Vertex
from gremlin_python.structure.graph import VertexProperty
from gremlin_python.structure.graph import Path


__author__ = 'Marko A. Rodriguez (http://markorodriguez.com)'


class GraphSONWriter(object):
    @staticmethod
    def _dictify(obj):
        for key in serializers:
            if isinstance(obj, key):
                return serializers[key]._dictify(obj)
        # list and map are treated as normal json objs (could be isolated serializers)
        if isinstance(obj, (list, set)):
            return [GraphSONWriter._dictify(o) for o in obj]
        elif isinstance(obj, dict):
            return dict((GraphSONWriter._dictify(k), GraphSONWriter._dictify(v)) for k, v in obj.items())
        else:
            return obj

    @staticmethod
    def writeObject(objectData):
        return json.dumps(GraphSONWriter._dictify(objectData), separators=(',', ':'))


class GraphSONReader(object):
    @staticmethod
    def _objectify(obj):
        if isinstance(obj, dict):
            if _SymbolHelper._TYPE in obj:
                type = obj[_SymbolHelper._TYPE]
                if type in deserializers:
                    return deserializers[type]._objectify(obj)
            # list and map are treated as normal json objs (could be isolated deserializers)
            return dict((GraphSONReader._objectify(k), GraphSONReader._objectify(v)) for k, v in obj.items())
        elif isinstance(obj, list):
            return [GraphSONReader._objectify(o) for o in obj]
        else:
            return obj

    @staticmethod
    def readObject(jsonData):
        return GraphSONReader._objectify(json.loads(jsonData))


'''
SERIALIZERS
'''


class GraphSONSerializer(object):
    @abstractmethod
    def _dictify(self, obj):
        return obj


class BytecodeSerializer(GraphSONSerializer):
    def _dictify(self, bytecode):
        if isinstance(bytecode, Traversal):
            bytecode = bytecode.bytecode
        out = {}
        sources = []
        for instruction in bytecode.source_instructions:
            inst = []
            inst.append(instruction[0])
            for arg in instruction[1:]:
                inst.append(GraphSONWriter._dictify(arg))
            sources.append(inst)
        steps = []
        for instruction in bytecode.step_instructions:
            inst = []
            inst.append(instruction[0])
            for arg in instruction[1:]:
                inst.append(GraphSONWriter._dictify(arg))
            steps.append(inst)
        if len(sources) > 0:
            out["source"] = sources
        if len(steps) > 0:
            out["step"] = steps
        return _SymbolHelper.objectify("Bytecode", out)


class TraversalStrategySerializer(GraphSONSerializer):
    def _dictify(self, strategy):
        return _SymbolHelper.objectify(strategy.strategy_name, GraphSONWriter._dictify(strategy.configuration))


class TraverserSerializer(GraphSONSerializer):
    def _dictify(self, traverser):
        return _SymbolHelper.objectify("Traverser", {"value": GraphSONWriter._dictify(traverser.object),
                                                     "bulk": GraphSONWriter._dictify(traverser.bulk)})


class EnumSerializer(GraphSONSerializer):
    def _dictify(self, enum):
        return _SymbolHelper.objectify(_SymbolHelper.toGremlin(type(enum).__name__),
                                       _SymbolHelper.toGremlin(str(enum.name)))


class PSerializer(GraphSONSerializer):
    def _dictify(self, p):
        out = {}
        out["predicate"] = p.operator
        if p.other is None:
            out["value"] = GraphSONWriter._dictify(p.value)
        else:
            out["value"] = [GraphSONWriter._dictify(p.value), GraphSONWriter._dictify(p.other)]
        return _SymbolHelper.objectify("P", out)


class BindingSerializer(GraphSONSerializer):
    def _dictify(self, binding):
        out = {}
        out["key"] = binding.key
        out["value"] = GraphSONWriter._dictify(binding.value)
        return _SymbolHelper.objectify("Binding", out)


class LambdaSerializer(GraphSONSerializer):
    def _dictify(self, lambda_object):
        lambda_result = lambda_object()
        out = {}
        script = lambda_result if isinstance(lambda_result, str) else lambda_result[0]
        language = statics.default_lambda_language if isinstance(lambda_result, str) else lambda_result[1]
        out["script"] = script
        out["language"] = language
        if language == "gremlin-jython" or language == "gremlin-python":
            if not script.strip().startswith("lambda"):
                script = "lambda " + script
                out["script"] = script
            out["arguments"] = six.get_function_code(eval(out["script"])).co_argcount
        else:
            out["arguments"] = -1
        return _SymbolHelper.objectify("Lambda", out)


class TypeSerializer(GraphSONSerializer):
    def _dictify(self, clazz):
        return GraphSONWriter._dictify(clazz())


class NumberSerializer(GraphSONSerializer):
    def _dictify(self, number):
        if isinstance(number, bool):  # python thinks that 0/1 integers are booleans
            return number
        elif isinstance(number, long) or (
                    abs(number) > 2147483647):  # in python all numbers are int unless specified otherwise
            return _SymbolHelper.objectify("Int64", number)
        elif isinstance(number, int):
            return _SymbolHelper.objectify("Int32", number)
        elif isinstance(number, float):
            return _SymbolHelper.objectify("Float", number)
        else:
            return number


'''
DESERIALIZERS
'''


class GraphSONDeserializer(object):
    @abstractmethod
    def _objectify(self, d):
        return d


class TraverserDeserializer(GraphSONDeserializer):
    def _objectify(self, d):
        return Traverser(GraphSONReader._objectify(d[_SymbolHelper._VALUE]["value"]),
                         GraphSONReader._objectify(d[_SymbolHelper._VALUE]["bulk"]))


class NumberDeserializer(GraphSONDeserializer):
    def _objectify(self, d):
        type = d[_SymbolHelper._TYPE]
        value = d[_SymbolHelper._VALUE]
        if type == "g:Int32":
            return int(value)
        elif type == "g:Int64":
            return long(value)
        else:
            return float(value)


class VertexDeserializer(GraphSONDeserializer):
    def _objectify(self, d):
        value = d[_SymbolHelper._VALUE]
        return Vertex(GraphSONReader._objectify(value["id"]), value["label"] if "label" in value else "")


class EdgeDeserializer(GraphSONDeserializer):
    def _objectify(self, d):
        value = d[_SymbolHelper._VALUE]
        return Edge(GraphSONReader._objectify(value["id"]),
                    Vertex(GraphSONReader._objectify(value["outV"]), ""),
                    value["label"] if "label" in value else "vertex",
                    Vertex(GraphSONReader._objectify(value["inV"]), ""))


class VertexPropertyDeserializer(GraphSONDeserializer):
    def _objectify(self, d):
        value = d[_SymbolHelper._VALUE]
        return VertexProperty(GraphSONReader._objectify(value["id"]), value["label"],
                              GraphSONReader._objectify(value["value"]))


class PropertyDeserializer(GraphSONDeserializer):
    def _objectify(self, d):
        value = d[_SymbolHelper._VALUE]
        return Property(value["key"], GraphSONReader._objectify(value["value"]))


class PathDeserializer(GraphSONDeserializer):
    def _objectify(self, d):
        value = d[_SymbolHelper._VALUE]
        labels = []
        objects = []
        for label in value["labels"]:
            labels.append(set(label))
        for object in value["objects"]:
            objects.append(GraphSONReader._objectify(object))
        return Path(labels, objects)


class _SymbolHelper(object):
    symbolMap = {"global_": "global", "as_": "as", "in_": "in", "and_": "and",
                 "or_": "or", "is_": "is", "not_": "not", "from_": "from",
                 "set_": "set", "list_": "list", "all_": "all"}

    _TYPE = "@type"
    _VALUE = "@value"

    @staticmethod
    def toGremlin(symbol):
        return _SymbolHelper.symbolMap[symbol] if symbol in _SymbolHelper.symbolMap else symbol

    @staticmethod
    def objectify(type, value=None, prefix="g"):
        object = {_SymbolHelper._TYPE: prefix + ":" + type}
        if value is not None:
            object[_SymbolHelper._VALUE] = value
        return object


serializers = {
    Traversal: BytecodeSerializer(),
    Traverser: TraverserSerializer(),
    Bytecode: BytecodeSerializer(),
    Binding: BindingSerializer(),
    P: PSerializer(),
    Enum: EnumSerializer(),
    FunctionType: LambdaSerializer(),
    LongType: NumberSerializer(),
    IntType: NumberSerializer(),
    FloatType: NumberSerializer(),
    TypeType: TypeSerializer(),
    TraversalStrategy: TraversalStrategySerializer(),
}

deserializers = {
    "g:Traverser": TraverserDeserializer(),
    "g:Int32": NumberDeserializer(),
    "g:Int64": NumberDeserializer(),
    "g:Float": NumberDeserializer(),
    "g:Double": NumberDeserializer(),
    "g:Vertex": VertexDeserializer(),
    "g:Edge": EdgeDeserializer(),
    "g:VertexProperty": VertexPropertyDeserializer(),
    "g:Property": PropertyDeserializer(),
    "g:Path": PathDeserializer()
}
