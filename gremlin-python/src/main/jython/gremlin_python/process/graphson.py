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
from types import FunctionType
from types import IntType
from types import LongType

from .traversal import Binding
from .traversal import Bytecode
from .traversal import P
from .traversal import Traversal
from .. import statics


class GraphSONWriter(object):
    @staticmethod
    def _dictify(object):
        for key in serializers:
            if isinstance(object, key):
                return serializers[key]._dictify(object)
        return object

    @staticmethod
    def writeObject(object):
        return json.dumps(GraphSONWriter._dictify(object))


'''
Serializers
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
        return _SymbolHelper.objectify("bytecode", dict)


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
        return _SymbolHelper.objectify("binding", dict)


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
                dict["value"] = script
            dict["arguments"] = eval(dict["value"]).func_code.co_argcount
        else:
            dict["arguments"] = -1
        return _SymbolHelper.objectify("lambda", dict)


class NumberSerializer(GraphSONSerializer):
    def _dictify(self, number):
        if isinstance(number, bool): # python thinks that 0/1 integers are booleans
            return number
        elif isinstance(number, long):
            return _SymbolHelper.objectify("int64", number)
        elif isinstance(number, int):
            return _SymbolHelper.objectify("int32", number)
        else:
            return number


class _SymbolHelper(object):
    symbolMap = {"_global": "global", "_as": "as", "_in": "in", "_and": "and",
                 "_or": "or", "_is": "is", "_not": "not", "_from": "from"}

    _TYPE = "@type"
    _VALUE = "@value"

    @staticmethod
    def toGremlin(symbol):
        return _SymbolHelper.symbolMap[symbol] if symbol in _SymbolHelper.symbolMap else symbol

    @staticmethod
    def objectify(type, value, prefix="gremlin"):
        return {_SymbolHelper._TYPE: prefix + ":" + type, _SymbolHelper._VALUE: value}


serializers = {
    Traversal: BytecodeSerializer(),
    Bytecode: BytecodeSerializer(),
    Binding: BindingSerializer(),
    P: PSerializer(),
    Enum: EnumSerializer(),
    FunctionType: LambdaSerializer(),
    LongType: NumberSerializer(),
    IntType: NumberSerializer()
}
