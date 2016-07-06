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

import inspect
import sys
from aenum import Enum

from translator import SymbolHelper
from translator import Translator
from traversal import Barrier
from traversal import Cardinality
from traversal import Column
from traversal import P
from traversal import PythonTraversal
from traversal import RawExpression

if sys.version_info.major > 2:
    long = int

__author__ = 'Marko A. Rodriguez (http://markorodriguez.com)'


class JythonTranslator(Translator):
    def __init__(self, traversal_source, anonymous_traversal="__", target_language="gremlin-jython"):
        Translator.__init__(self, traversal_source, anonymous_traversal, target_language)

    def translate(self, bytecode):
        traversal_script = self.anonymous_traversal if self.traversal_source is None else self.traversal_source
        for instruction in bytecode.source_instructions:
            traversal_script = traversal_script + "." + SymbolHelper.toJava(
                instruction[0]) + "(" + JythonTranslator.stringify(*instruction[1]) + ")"
        for instruction in bytecode.step_instructions:
            traversal_script = traversal_script + "." + SymbolHelper.toJava(
                instruction[0]) + "(" + JythonTranslator.stringify(*instruction[1]) + ")"
        return traversal_script

    @staticmethod
    def stringOrObject(arg):
        if isinstance(arg, str):
            return "\"" + arg + "\""
        elif isinstance(arg, long):
            return str(arg) + "L" if arg > 9223372036854775807L else "Long(" + str(arg) + ")"
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
        elif isinstance(arg, PythonTraversal):
            return JythonTranslator(None, "__").translate(arg.bytecode)
        elif callable(arg):  # lambda that produces a string that is a lambda
            argLambdaString = arg().strip()
            argLength = len(inspect.getargspec(eval(argLambdaString)).args)
            if argLength == 0:
                return "JythonZeroArgLambda(" + argLambdaString + ")"
            elif argLength == 1:
                return "JythonOneArgLambda(" + argLambdaString + ")"
            elif argLength == 2:
                return "JythonTwoArgLambda(" + argLambdaString + ")"
            else:
                raise
        elif isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):  # bindings
            return arg[0]
        elif isinstance(arg, RawExpression):
            return "".join(JythonTranslator.stringOrObject(i) for i in arg.parts)
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
