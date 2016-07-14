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
from aenum import Enum

from traversal import Bytecode
from traversal import P
from traversal import SymbolHelper
from traversal import Traversal


class GraphSONSerializer(object):
    @staticmethod
    def dictify(thing):
        if isinstance(thing, Traversal):
            return GraphSONSerializer.dictify(thing.bytecode)
        elif isinstance(thing, Bytecode):
            dict = {}
            dict["@type"] = "Bytecode"
            sources = []
            for instruction in thing.source_instructions:
                inst = []
                inst.append(instruction[0])
                for arg in instruction[1]:
                    inst.append(GraphSONSerializer.dictify(arg))
                sources.append(inst)
            steps = []
            for instruction in thing.step_instructions:
                inst = []
                inst.append(instruction[0])
                for arg in instruction[1]:
                    inst.append(GraphSONSerializer.dictify(arg))
                steps.append(inst)
            if len(sources) > 0:
                dict["source"] = sources
            if len(steps) > 0:
                dict["step"] = steps
            return dict
        elif isinstance(thing, Enum):
            dict = {}
            dict["@type"] = SymbolHelper.toJava(type(thing).__name__)
            dict["value"] = SymbolHelper.toJava(str(thing.name))
            return dict
        elif isinstance(thing, P):
            dict = {}
            dict["@type"] = "P"
            dict["predicate"] = SymbolHelper.toJava(thing.operator)
            if thing.other is None:
                dict["value"] = GraphSONSerializer.dictify(thing.value)
            else:
                dict["value"] = [GraphSONSerializer.dictify(thing.value), GraphSONSerializer.dictify(thing.other)]
            return dict
        else:
            return thing

    @staticmethod
    def serialize(thing):
        return json.dumps(GraphSONSerializer.dictify(thing))
