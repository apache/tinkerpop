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

import org.apache.tinkerpop.gremlin.process.traversal.P
import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__
import org.apache.tinkerpop.gremlin.python.util.SymbolHelper
import org.apache.tinkerpop.gremlin.util.CoreImports

import java.lang.reflect.Modifier

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GremlinPythonSourceGenerator {

    public static void create(final String gremlinPythonFile) {

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
        pythonClass.append("from collections import OrderedDict\n")
        pythonClass.append("from aenum import Enum\n")
        pythonClass.append("statics = OrderedDict()\n\n")
        pythonClass.append("""
globalTranslator = None
""").append("\n\n");

//////////////////////////
// GraphTraversalSource //
//////////////////////////
        pythonClass.append(
                """class PythonGraphTraversalSource(object):
  def __init__(self, translator, remote_connection=None):
    global globalTranslator
    self.translator = translator
    globalTranslator = translator
    self.remote_connection = remote_connection
  def __repr__(self):
    return "graphtraversalsource[" + str(self.remote_connection) + ", " + self.translator.traversal_script + "]"
""")
        GraphTraversalSource.getMethods()
                .findAll { !it.name.equals("clone") }
                .collect { it.name }
                .unique()
                .sort { a, b -> a <=> b }
                .each { method ->
            final Class<?> returnType = (GraphTraversalSource.getMethods() as Set).findAll {
                it.name.equals(method)
            }.collect {
                it.returnType
            }[0]
            if (null != returnType) {
                if (Traversal.isAssignableFrom(returnType)) {
                    pythonClass.append(
                            """  def ${method}(self, *args):
    traversal = PythonGraphTraversal(self.translator, self.remote_connection)
    traversal.translator.addSpawnStep(traversal, "${method}", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return traversal
""")
                } else if (TraversalSource.isAssignableFrom(returnType)) {
                    pythonClass.append(
                            """  def ${method}(self, *args):
    source = PythonGraphTraversalSource(self.translator, self.remote_connection)
    source.translator.addSource(source, "${method}", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return source
""")
                }
            }
        }
        pythonClass.append("\n\n")

////////////////////
// GraphTraversal //
////////////////////
        pythonClass.append(
                """class PythonGraphTraversal(object):
  def __init__(self, translator, remote_connection=None):
    self.translator = translator
    self.remote_connection = remote_connection
    self.results = None
    self.last_traverser = None
    self.bindings = {}
  def __repr__(self):
    return self.translator.traversal_script
  def __getitem__(self,index):
    if isinstance(index,int):
      return self.range(index,index+1)
    elif isinstance(index,slice):
      return self.range(index.start,index.stop)
    else:
      raise TypeError("Index must be int or slice")
  def __getattr__(self,key):
    return self.values(key)
  def __iter__(self):
        return self
  def __next__(self):
     return self.next()
  def toList(self):
    return list(iter(self))
  def toSet(self):
    return set(iter(self))
  def next(self,amount):
    count = 0
    tempList = []
    while count < amount:
      count = count + 1
      temp = next(self,None)
      if None == temp:
        break
      tempList.append(temp)
    return tempList
  def next(self):
     if self.results is None:
        self.results = self.remote_connection.submit(self.translator.target_language, self.translator.traversal_script, self.bindings)
     if self.last_traverser is None:
         self.last_traverser = next(self.results)
     object = self.last_traverser.object
     self.last_traverser.bulk = self.last_traverser.bulk - 1
     if self.last_traverser.bulk <= 0:
         self.last_traverser = None
     return object
""")
        GraphTraversal.getMethods()
                .findAll { !it.name.equals("clone") }
                .collect { SymbolHelper.toPython(it.name) }
                .unique()
                .sort { a, b -> a <=> b }
                .each { method ->
            final Class<?> returnType = (GraphTraversal.getMethods() as Set).findAll {
                it.name.equals(SymbolHelper.toJava(method))
            }.collect { it.returnType }[0]
            if (null != returnType && Traversal.isAssignableFrom(returnType)) {
                pythonClass.append(
                        """  def ${method}(self, *args):
    self.translator.addStep(self, "${method}", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
      elif isinstance(arg, RawExpression):
        self.bindings.update(arg.bindings)
    return self
""")
            }
        };
        pythonClass.append("\n\n")

////////////////////////
// AnonymousTraversal //
////////////////////////
        pythonClass.append("class __(object):\n");
        __.getMethods()
                .findAll { Traversal.isAssignableFrom(it.returnType) }
                .collect { SymbolHelper.toPython(it.name) }
                .unique()
                .sort { a, b -> a <=> b }
                .each { method ->
            pythonClass.append(
                    """  @staticmethod
  def ${method}(*args):
    return PythonGraphTraversal(globalTranslator.getAnonymousTraversalTranslator()).${method}(*args)
""")
        };
        pythonClass.append("\n\n")

        __.class.getMethods()
                .findAll { Traversal.class.isAssignableFrom(it.getReturnType()) }
                .findAll { Modifier.isStatic(it.getModifiers()) }
                .findAll { !it.name.equals("__") }
                .collect { SymbolHelper.toPython(it.name) }
                .unique()
                .sort { a, b -> a <=> b }
                .forEach {
            pythonClass.append("def ${it}(*args):\n").append("      return __.${it}(*args)\n\n")
            pythonClass.append("statics['${it}'] = ${it}\n")
        }
        pythonClass.append("\n\n")

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
            pythonClass.deleteCharAt(pythonClass.length() - 1).append("')\n\n")
            enumClass.getEnumConstants().each { value ->
                pythonClass.append("statics['${SymbolHelper.toPython(value.name())}'] = ${value.getDeclaringClass().getSimpleName()}.${SymbolHelper.toPython(value.name())}\n");
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
        P.class.getMethods()
                .findAll { Modifier.isStatic(it.getModifiers()) }
                .findAll { P.class.isAssignableFrom(it.returnType) }
                .collect { SymbolHelper.toPython(it.name) }
                .unique()
                .sort { a, b -> a <=> b }
                .each { method ->
            pythonClass.append(
                    """   @staticmethod
   def ${method}(*args):
      return P("${SymbolHelper.toJava(method)}", *args)
""")
        };
        pythonClass.append("""   def _and(self, arg):
      return P("_and", arg, self)
   def _or(self, arg):
      return P("_or", arg, self)
""")
        pythonClass.append("\n")
        P.class.getMethods()
                .findAll { Modifier.isStatic(it.getModifiers()) }
                .findAll { !it.name.equals("clone") }
                .findAll { P.class.isAssignableFrom(it.getReturnType()) }
                .collect { SymbolHelper.toPython(it.name) }
                .unique()
                .sort { a, b -> a <=> b }
                .forEach {
            pythonClass.append("def ${it}(*args):\n").append("      return P.${it}(*args)\n\n")
            pythonClass.append("statics['${it}'] = ${it}\n")
        }
        pythonClass.append("\n")
        //////////////

        pythonClass.append("""class RawExpression(object):
   def __init__(self, *args):
      self.bindings = dict()
      self.parts = [self._process_arg(arg) for arg in args]

   def _process_arg(self, arg):
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
         self.bindings[arg[0]] = arg[1]
         return Raw(arg[0])
      else:
         return Raw(arg)

class Raw(object):
   def __init__(self, value):
      self.value = value

   def __str__(self):
      return str(self.value)

""")
        //////////////

        pythonClass.append("statics = OrderedDict(reversed(list(statics.items())))\n")

// save to a python file
        final File file = new File(gremlinPythonFile);
        file.delete()
        pythonClass.eachLine { file.append(it + "\n") }
    }
}
