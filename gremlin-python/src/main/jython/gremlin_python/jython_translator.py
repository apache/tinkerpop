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

from gremlin_python import Barrier
from gremlin_python import Cardinality
from gremlin_python import Column
from gremlin_python import P
from gremlin_python import Raw
from gremlin_python import RawExpression
from translator import SymbolHelper
from translator import Translator

if sys.version_info.major > 2:
    long = int

__author__ = 'Marko A. Rodriguez (http://markorodriguez.com)'


class JythonTranslator(Translator):
    def __init__(self, alias, source_language="python", target_language="gremlin-jython"):
        Translator.__init__(self, alias, source_language, target_language)

    def addStep(self, traversal, step_name, *args):
        self.traversal_script = self.traversal_script + "." + SymbolHelper.toJava(
            step_name) + "(" + JythonTranslator.stringify(*args) + ")"

    def addSpawnStep(self, traversal, step_name, *args):
        newTranslator = JythonTranslator(self.alias, self.source_language)
        newTranslator.traversal_script = self.traversal_script
        newTranslator.traversal_script = newTranslator.traversal_script + "." + SymbolHelper.toJava(
            step_name) + "(" + JythonTranslator.stringify(*args) + ")"
        traversal.translator = newTranslator

    def addSource(self, traversal_source, source_name, *args):
        newTranslator = JythonTranslator(self.alias, self.source_language)
        newTranslator.traversal_script = self.traversal_script
        newTranslator.traversal_script = newTranslator.traversal_script + "." + SymbolHelper.toJava(
            source_name) + "(" + JythonTranslator.stringify(*args) + ")"
        traversal_source.translator = newTranslator

    def getAnonymousTraversalTranslator(self):
        return JythonTranslator("__", self.source_language)

    @staticmethod
    def stringOrObject(arg):
        if isinstance(arg, str):
            return "\"" + arg + "\""
        elif isinstance(arg, long):
            return str(arg) + "L"
        elif isinstance(arg, Barrier):
            return "Barrier" + "." + SymbolHelper.toJava(str(arg.name))
        elif isinstance(arg, Column):
            return "Column.valueOf('" + SymbolHelper.toJava(str(arg.name)) + "')"
        elif isinstance(arg, Cardinality):
            return "Cardinality" + "." + SymbolHelper.toJava(str(arg.name))
        elif isinstance(arg, Enum):  # Order, Direction, Scope, T, etc.
            return SymbolHelper.toJava(type(arg).__name__) + "." + SymbolHelper.toJava(str(arg.name))
        elif isinstance(arg, P):
            if arg.other is None:
                return "P." + SymbolHelper.toJava(arg.operator) + "(" + JythonTranslator.stringOrObject(
                    arg.value) + ")"
            else:
                return JythonTranslator.stringOrObject(arg.other) + "." + SymbolHelper.toJava(
                    arg.operator) + "(" + JythonTranslator.stringOrObject(arg.value) + ")"
        elif callable(arg):  # lambdas
            lambdaString = arg().strip()
            if lambdaString.startswith("lambda"):
                return lambdaString
            else:
                return "lambda: " + lambdaString
        elif isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):  # bindings
            return arg[0]
        elif isinstance(arg, RawExpression):
            return "".join(JythonTranslator.stringOrObject(i) for i in arg.parts)
        elif isinstance(arg, Raw):
            return str(arg)
        else:
            return str(arg)

    @staticmethod
    def stringify(*args):
        if len(args) == 0:
            return ""
        elif len(args) == 1:
            return JythonTranslator.stringOrObject(args[0])
        else:
            return ", ".join(JythonTranslator.stringOrObject(i) for i in args)
