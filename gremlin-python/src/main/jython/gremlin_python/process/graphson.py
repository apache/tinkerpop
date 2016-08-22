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
        dict = {}
        dict["@type"] = "Bytecode"
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
        return dict


class EnumSerializer(GraphSONSerializer):
    def _dictify(self, enum):
        dict = {}
        dict["@type"] = _SymbolHelper.toGremlin(type(enum).__name__)
        dict["value"] = _SymbolHelper.toGremlin(str(enum.name))
        return dict


class PSerializer(GraphSONSerializer):
    def _dictify(self, p):
        dict = {}
        dict["@type"] = "P"
        dict["predicate"] = p.operator
        if p.other is None:
            dict["value"] = GraphSONWriter._dictify(p.value)
        else:
            dict["value"] = [GraphSONWriter._dictify(p.value), GraphSONWriter._dictify(p.other)]
        return dict


class BindingSerializer(GraphSONSerializer):
    def _dictify(self, binding):
        dict = {}
        dict["@type"] = "Binding"
        dict["variable"] = binding.variable
        dict["value"] = GraphSONWriter._dictify(binding.value)
        return dict


class LambdaSerializer(GraphSONSerializer):
    def _dictify(self, lambdaObject):
        lambdaResult = lambdaObject()
        dict = {}
        dict["@type"] = "Lambda"
        script = lambdaResult if isinstance(lambdaResult, str) else lambdaResult[0]
        language = statics.default_lambda_language if isinstance(lambdaResult, str) else lambdaResult[1]
        dict["value"] = script
        dict["language"] = language
        if language == "gremlin-jython" or language == "gremlin-python":
            if not script.strip().startswith("lambda"):
                script = "lambda " + script
                dict["value"] = script
            dict["arguments"] = eval(dict["value"]).func_code.co_argcount
        else:
            dict["arguments"] = -1
        return dict


class TraversalSerializer(BytecodeSerializer):
    def _dictify(self, traversal):
        return BytecodeSerializer._dictify(self, traversal.bytecode)


class _SymbolHelper(object):
    symbolMap = {"_global": "global", "_as": "as", "_in": "in", "_and": "and",
                 "_or": "or", "_is": "is", "_not": "not", "_from": "from"}

    @staticmethod
    def toGremlin(symbol):
        return _SymbolHelper.symbolMap[symbol] if symbol in _SymbolHelper.symbolMap else symbol


serializers = {
    Bytecode: BytecodeSerializer(),
    Binding: BindingSerializer(),
    P: PSerializer(),
    Enum: EnumSerializer(),
    FunctionType: LambdaSerializer(),
    Traversal: TraversalSerializer()
}
