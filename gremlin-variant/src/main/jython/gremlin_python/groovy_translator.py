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

import sys
from aenum import Enum

from gremlin_python import P
from gremlin_python import Raw
from gremlin_python import RawExpression
from translator import Translator

if sys.version_info.major > 2:
    long = int

__author__ = 'Marko A. Rodriguez (http://markorodriguez.com)'

methodMap = {"_global": "global", "_as": "as", "_in": "in", "_and": "and", "_or": "or", "_is": "is", "_not": "not",
             "_from": "from"}

enumMap = {"Cardinality": "VertexProperty.Cardinality", "Barrier": "SackFunctions.Barrier"}


class GroovyTranslator(Translator):
    def __init__(self, alias, script_engine="gremlin-groovy"):
        Translator.__init__(self, alias, script_engine)

    def addStep(self, traversal, step_name, *args):
        self.traversal_script = self.traversal_script + "." + GroovyTranslator.mapMethod(
            step_name) + "(" + GroovyTranslator.stringify(*args) + ")"

    def addSpawnStep(self, traversal, step_name, *args):
        newTranslator = GroovyTranslator(self.alias, self.script_engine)
        newTranslator.traversal_script = self.traversal_script
        newTranslator.traversal_script = newTranslator.traversal_script + "." + GroovyTranslator.mapMethod(
            step_name) + "(" + GroovyTranslator.stringify(*args) + ")"
        traversal.translator = newTranslator

    def addSource(self, traversal_source, source_name, *args):
        newTranslator = GroovyTranslator(self.alias, self.script_engine)
        newTranslator.traversal_script = self.traversal_script
        newTranslator.traversal_script = newTranslator.traversal_script + "." + GroovyTranslator.mapMethod(
            source_name) + "(" + GroovyTranslator.stringify(*args) + ")"
        traversal_source.translator = newTranslator

    def getAnonymousTraversalTranslator(self):
        return GroovyTranslator("__", self.script_engine)

    ### HELPER METHODS ###

    @staticmethod
    def mapMethod(method):
        if (method in methodMap):
            return methodMap[method]
        else:
            return method

    @staticmethod
    def mapEnum(enum):
        if (enum in enumMap):
            return enumMap[enum]
        else:
            return enum

    @staticmethod
    def stringOrObject(arg):
        if (isinstance(arg, str) and
                not (arg.startswith("Computer.")) and
                not (arg.startswith("ReadOnlyStrategy."))):
            return "\"" + arg + "\""
        elif isinstance(arg, bool):
            return str(arg).lower()
        elif isinstance(arg, long):
            return str(arg) + "L"
        elif isinstance(arg, float):
            return str(arg) + "f"
        elif isinstance(arg, Enum):  # Column, Order, Direction, Scope, T, etc.
            return GroovyTranslator.mapEnum(type(arg).__name__) + "." + GroovyTranslator.mapMethod(str(arg.name))
        elif isinstance(arg, P):
            if arg.other is None:
                return "P." + GroovyTranslator.mapMethod(arg.operator) + "(" + GroovyTranslator.stringOrObject(
                    arg.value) + ")"
            else:
                return GroovyTranslator.stringOrObject(arg.other) + "." + GroovyTranslator.mapMethod(
                    arg.operator) + "(" + GroovyTranslator.stringOrObject(arg.value) + ")"
        elif callable(arg):  # closures
            lambdaString = arg().strip()
            if lambdaString.startswith("{"):
                return lambdaString
            else:
                return "{" + lambdaString + "}"
        elif isinstance(arg, dict) and 1 == len(arg) and isinstance(arg.keys()[0], str):  # bindings
            return arg.keys()[0]
        elif isinstance(arg, RawExpression):
            return "".join(GroovyTranslator.stringOrObject(i) for i in arg.parts)
        elif isinstance(arg, Raw):
            return str(arg)
        else:
            return str(arg)

    @staticmethod
    def stringify(*args):
        if len(args) == 0:
            return ""
        elif len(args) == 1:
            return GroovyTranslator.stringOrObject(args[0])
        else:
            return ", ".join(GroovyTranslator.stringOrObject(i) for i in args)
