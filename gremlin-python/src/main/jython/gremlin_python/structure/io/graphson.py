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

__author__ = 'Marko A. Rodriguez (http://markorodriguez.com)'


import json
from abc import abstractmethod
from aenum import Enum
from types import FloatType
from types import FunctionType
from types import IntType
from types import LongType

from gremlin_python import statics
from gremlin_python.process.traversal import Binding
from gremlin_python.process.traversal import Bytecode
from gremlin_python.process.traversal import P
from gremlin_python.process.traversal import Traversal
from gremlin_python.process.traversal import Traverser
from gremlin_python.structure.graph import Edge
from gremlin_python.structure.graph import Property
from gremlin_python.structure.graph import Vertex
from gremlin_python.structure.graph import VertexProperty


class GraphSONWriter(object):
    @staticmethod
    def _dictify(object):
        for key in serializers:
            if isinstance(object, key):
                return serializers[key]._dictify(object)
        # list and map are treated as normal json objects (could be isolated serializers)
        if isinstance(object, list):
            newList = []
            for item in object:
                newList.append(GraphSONWriter._dictify(item))
            return newList
        elif isinstance(object, dict):
            newDict = {}
            for key in object:
                newDict[GraphSONWriter._dictify(key)] = GraphSONWriter._dictify(object[key])
            return newDict
        else:
            return object

    @staticmethod
    def writeObject(objectData):
        return json.dumps(GraphSONWriter._dictify(objectData))


class GraphSONReader(object):
    @staticmethod
    def _objectify(object):
        if isinstance(object, dict):
            if _SymbolHelper._TYPE in object:
                type = object[_SymbolHelper._TYPE]
                if type in deserializers:
                    return deserializers[type]._objectify(object)
                    # list and map are treated as normal json objects (could be isolated deserializers)
            newDict = {}
            for key in object:
                newDict[GraphSONReader._objectify(key)] = GraphSONReader._objectify(object[key])
            return newDict
        elif isinstance(object, list):
            newList = []
            for item in object:
                newList.append(GraphSONReader._objectify(item))
            return newList
        else:
            return object

    @staticmethod
    def readObject(jsonData):
        return GraphSONReader._objectify(json.loads(jsonData))


'''
SERIALIZERS
'''


class GraphSONSerializer(object):
    @abstractmethod
    def _dictify(self, object):
        return object


class BytecodeSerializer(GraphSONSerializer):
    def _dictify(self, bytecode):
        if isinstance(bytecode, Traversal):
            bytecode = bytecode.bytecode
        dict = {}
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
            dict["source"] = sources
        if len(steps) > 0:
            dict["step"] = steps
        return _SymbolHelper.objectify("Bytecode", dict)


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
        dict = {}
        dict["predicate"] = p.operator
        if p.other is None:
            dict["value"] = GraphSONWriter._dictify(p.value)
        else:
            dict["value"] = [GraphSONWriter._dictify(p.value), GraphSONWriter._dictify(p.other)]
        return _SymbolHelper.objectify("P", dict)


class BindingSerializer(GraphSONSerializer):
    def _dictify(self, binding):
        dict = {}
        dict["key"] = binding.key
        dict["value"] = GraphSONWriter._dictify(binding.value)
        return _SymbolHelper.objectify("Binding", dict)


class LambdaSerializer(GraphSONSerializer):
    def _dictify(self, lambdaObject):
        lambdaResult = lambdaObject()
        dict = {}
        script = lambdaResult if isinstance(lambdaResult, str) else lambdaResult[0]
        language = statics.default_lambda_language if isinstance(lambdaResult, str) else lambdaResult[1]
        dict["script"] = script
        dict["language"] = language
        if language == "gremlin-jython" or language == "gremlin-python":
            if not script.strip().startswith("lambda"):
                script = "lambda " + script
                dict["script"] = script
            dict["arguments"] = eval(dict["script"]).func_code.co_argcount
        else:
            dict["arguments"] = -1
        return _SymbolHelper.objectify("Lambda", dict)


class NumberSerializer(GraphSONSerializer):
    def _dictify(self, number):
        if isinstance(number, bool):  # python thinks that 0/1 integers are booleans
            return number
        elif isinstance(number, long):
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
    def _objectify(self, dict):
        return dict


class TraverserDeserializer(GraphSONDeserializer):
    def _objectify(self, dict):
        return Traverser(GraphSONReader._objectify(dict[_SymbolHelper._VALUE]["value"]),
                         GraphSONReader._objectify(dict[_SymbolHelper._VALUE]["bulk"]))


class NumberDeserializer(GraphSONDeserializer):
    def _objectify(self, dict):
        type = dict[_SymbolHelper._TYPE]
        value = dict[_SymbolHelper._VALUE]
        if type == "g:Int32":
            return int(value)
        elif type == "g:Int64":
            return long(value)
        else:
            return float(value)


class VertexDeserializer(GraphSONDeserializer):
    def _objectify(self, dict):
        value = dict[_SymbolHelper._VALUE]
        return Vertex(GraphSONReader._objectify(value["id"]), value["label"] if "label" in value else "")


class EdgeDeserializer(GraphSONDeserializer):
    def _objectify(self, dict):
        value = dict[_SymbolHelper._VALUE]
        return Edge(GraphSONReader._objectify(value["id"]),
                    Vertex(GraphSONReader._objectify(value["outV"]), ""),
                    value["label"] if "label" in value else "vertex",
                    Vertex(GraphSONReader._objectify(value["inV"]), ""))


class VertexPropertyDeserializer(GraphSONDeserializer):
    def _objectify(self, dict):
        value = dict[_SymbolHelper._VALUE]
        return VertexProperty(GraphSONReader._objectify(value["id"]), value["label"],
                              GraphSONReader._objectify(value["value"]))


class PropertyDeserializer(GraphSONDeserializer):
    def _objectify(self, dict):
        value = dict[_SymbolHelper._VALUE]
        return Property(value["key"], GraphSONReader._objectify(value["value"]))


class _SymbolHelper(object):
    symbolMap = {"_global": "global", "_as": "as", "_in": "in", "_and": "and",
                 "_or": "or", "_is": "is", "_not": "not", "_from": "from",
                 "_set": "set", "_list": "list", "_all": "all"}

    _TYPE = "@type"
    _VALUE = "@value"

    @staticmethod
    def toGremlin(symbol):
        return _SymbolHelper.symbolMap[symbol] if symbol in _SymbolHelper.symbolMap else symbol

    @staticmethod
    def objectify(type, value, prefix="g"):
        return {_SymbolHelper._TYPE: prefix + ":" + type, _SymbolHelper._VALUE: value}


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
    FloatType: NumberSerializer()
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
    "g:Property": PropertyDeserializer()
}
