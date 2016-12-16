/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.python

import org.apache.tinkerpop.gremlin.jsr223.CoreImports
import org.apache.tinkerpop.gremlin.process.traversal.P
import org.apache.tinkerpop.gremlin.python.jsr223.SymbolHelper

import java.lang.reflect.Modifier

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class TraversalSourceGenerator {

    public static void create(final String traversalSourceFile) {

        final StringBuilder pythonClass = new StringBuilder()

        pythonClass.append("""'''
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
""")
        pythonClass.append("from aenum import Enum\n")
        pythonClass.append("from .. import statics\n")
        pythonClass.append("from ..statics import long\n\n")

        pythonClass.append("""
class Traversal(object):
    def __init__(self, graph, traversal_strategies, bytecode):
        self.graph = graph
        self.traversal_strategies = traversal_strategies
        self.bytecode = bytecode
        self.side_effects = TraversalSideEffects()
        self.traversers = None
        self.last_traverser = None
    def __repr__(self):
        return str(self.bytecode)
    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.bytecode == other.bytecode
        else:
            return False
    def __iter__(self):
        return self
    def __next__(self):
        if self.traversers is None:
            self.traversal_strategies.apply_strategies(self)
        if self.last_traverser is None:
            self.last_traverser = next(self.traversers)
        object = self.last_traverser.object
        self.last_traverser.bulk = self.last_traverser.bulk - 1
        if self.last_traverser.bulk <= 0:
            self.last_traverser = None
        return object
    def toList(self):
        return list(iter(self))
    def toSet(self):
        return set(iter(self))
    def iterate(self):
        while True:
            try: self.nextTraverser()
            except StopIteration: return self
    def nextTraverser(self):
        if self.traversers is None:
            self.traversal_strategies.apply_strategies(self)
        if self.last_traverser is None:
            return next(self.traversers)
        else:
            temp = self.last_traverser
            self.last_traverser = None
            return temp
    def next(self, amount=None):
        if amount is None:
            return self.__next__()
        else:
            count = 0
            tempList = []
            while count < amount:
                count = count + 1
                try: temp = self.__next__()
                except StopIteration: return tempList
                tempList.append(temp)
            return tempList
    def promise(self, cb=None):
        self.traversal_strategies.apply_async_strategies(self)
        future_traversers = self.traversers
        future = type(future_traversers)()
        def process(f):
            try:
                traversers = f.result()
            except Exception as e:
                future.set_exception(e)
            else:
                self.traversers = iter(traversers)
                if cb:
                    try:
                        result = cb(self)
                    except Exception as e:
                        future.set_exception(e)
                    else:
                        future.set_result(result)
                else:
                    future.set_result(self)
        future_traversers.add_done_callback(process)
        return future


""")

///////////
// Enums //
///////////
        for (final Class<? extends Enum> enumClass : CoreImports.getClassImports()
                .findAll { Enum.class.isAssignableFrom(it) }
                .sort { a, b -> a.getSimpleName() <=> b.getSimpleName() }
                .collect()) {
            pythonClass.append("${enumClass.getSimpleName()} = Enum('${enumClass.getSimpleName()}', '");
            enumClass.getEnumConstants()
                    .sort { a, b -> a.name() <=> b.name() }
                    .each { value -> pythonClass.append("${SymbolHelper.toPython(value.name())} "); }
            pythonClass.deleteCharAt(pythonClass.length() - 1).append("')\n")
            enumClass.getEnumConstants().each { value ->
                pythonClass.append("statics.add_static('${SymbolHelper.toPython(value.name())}', ${value.getDeclaringClass().getSimpleName()}.${SymbolHelper.toPython(value.name())})\n");
            }
            pythonClass.append("\n");
        }
        //////////////

        pythonClass.append("""class P(object):
   def __init__(self, operator, value, other=None):
      self.operator = operator
      self.value = value
      self.other = other
""")
        P.class.getMethods().
                findAll { Modifier.isStatic(it.getModifiers()) }.
                findAll { P.class.isAssignableFrom(it.returnType) }.
                collect { SymbolHelper.toPython(it.name) }.
                unique().
                sort { a, b -> a <=> b }.
                each { method ->
                    pythonClass.append(
                            """   @staticmethod
   def ${method}(*args):
      return P("${SymbolHelper.toJava(method)}", *args)
""")
                };
        pythonClass.append("""   def and_(self, arg):
      return P("and", self, arg)
   def or_(self, arg):
      return P("or", self, arg)
   def __eq__(self, other):
        return isinstance(other, self.__class__) and self.operator == other.operator and self.value == other.value and self.other == other.other
   def __repr__(self):
      return self.operator + "(" + str(self.value) + ")" if self.other is None else self.operator + "(" + str(self.value) + "," + str(self.other) + ")"
""")
        pythonClass.append("\n")
        P.class.getMethods().
                findAll { Modifier.isStatic(it.getModifiers()) }.
                findAll { !it.name.equals("clone") }.
                findAll { P.class.isAssignableFrom(it.getReturnType()) }.
                collect { SymbolHelper.toPython(it.name) }.
                unique().
                sort { a, b -> a <=> b }.
                forEach {
                    pythonClass.append("def ${it}(*args):\n").append("      return P.${it}(*args)\n")
                    pythonClass.append("statics.add_static('${it}',${it})\n\n")
                }
        pythonClass.append("\n")
        //////////////

        pythonClass.append("""
'''
TRAVERSER
'''

class Traverser(object):
    def __init__(self, object, bulk=None):
        if bulk is None:
            bulk = long(1)
        self.object = object
        self.bulk = bulk
    def __repr__(self):
        return str(self.object)
    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.object == other.object

'''
TRAVERSAL SIDE-EFFECTS
'''

class TraversalSideEffects(object):
    def keys(self):
        return set()
    def get(self, key):
        raise KeyError(key)
    def __getitem__(self, key):
        return self.get(key)
    def __repr__(self):
        return "sideEffects[size:" + str(len(self.keys())) + "]"

'''
TRAVERSAL STRATEGIES
'''

class TraversalStrategies(object):
    global_cache = {}
    def __init__(self, traversal_strategies=None):
        self.traversal_strategies = traversal_strategies.traversal_strategies if traversal_strategies is not None else []
    def add_strategies(self, traversal_strategies):
        self.traversal_strategies = self.traversal_strategies + traversal_strategies
    def apply_strategies(self, traversal):
        for traversal_strategy in self.traversal_strategies:
            traversal_strategy.apply(traversal)
    def apply_async_strategies(self, traversal):
        for traversal_strategy in self.traversal_strategies:
            traversal_strategy.apply_async(traversal)
    def __repr__(self):
        return str(self.traversal_strategies)


class TraversalStrategy(object):
    def __init__(self, strategy_name=None, configuration=None):
        self.strategy_name = type(self).__name__ if strategy_name is None else strategy_name
        self.configuration = {} if configuration is None else configuration
    def apply(self, traversal):
        return
    def apply_async(self, traversal):
        return
    def __eq__(self, other):
        return isinstance(other, self.__class__)
    def __hash__(self):
        return hash(self.strategy_name)
    def __repr__(self):
        return self.strategy_name

'''
BYTECODE
'''

class Bytecode(object):
    def __init__(self, bytecode=None):
        self.source_instructions = []
        self.step_instructions = []
        self.bindings = {}
        if bytecode is not None:
            self.source_instructions = list(bytecode.source_instructions)
            self.step_instructions = list(bytecode.step_instructions)
    def add_source(self, source_name, *args):
        instruction = [source_name]
        for arg in args:
            instruction.append(self.__convertArgument(arg))
        self.source_instructions.append(instruction)
    def add_step(self, step_name, *args):
        instruction = [step_name]
        for arg in args:
            instruction.append(self.__convertArgument(arg))
        self.step_instructions.append(instruction)
    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.source_instructions == other.source_instructions and self.step_instructions == other.step_instructions
        else:
            return False
    def __convertArgument(self,arg):
        if isinstance(arg, Traversal):
            self.bindings.update(arg.bytecode.bindings)
            return arg.bytecode
        elif isinstance(arg, dict):
            newDict = {}
            for key in arg:
                newDict[self.__convertArgument(key)] = self.__convertArgument(arg[key])
            return newDict
        elif isinstance(arg, list):
            newList = []
            for item in arg:
                newList.append(self.__convertArgument(item))
            return newList
        elif isinstance(arg, set):
            newSet = set()
            for item in arg:
                newSet.add(self.__convertArgument(item))
            return newSet
        elif isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
            self.bindings[arg[0]] = arg[1]
            return Binding(arg[0],self.__convertArgument(arg[1]))
        else:
            return arg
    def __repr__(self):
        return (str(self.source_instructions) if len(self.source_instructions) > 0 else "") + \\
               (str(self.step_instructions) if len(self.step_instructions) > 0 else "")


'''
BINDINGS
'''

class Bindings(object):
    def of(self,key,value):
        if not isinstance(key, str):
            raise TypeError("Key must be str")
        return (key,value)

class Binding(object):
    def __init__(self,key,value):
        self.key = key
        self.value = value
    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.key == other.key and self.value == other.value
    def __hash__(self):
        return hash(self.key) + hash(self.value)
    def __repr__(self):
        return "binding[" + self.key + "=" + str(self.value) + "]"

""")
        //////////////

        // save to a python file
        final File file = new File(traversalSourceFile);
        file.delete()
        pythonClass.eachLine { file.append(it + "\n") }
    }
}
