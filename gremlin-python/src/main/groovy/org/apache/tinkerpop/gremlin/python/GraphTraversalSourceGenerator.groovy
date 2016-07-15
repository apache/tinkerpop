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

import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__
import org.apache.tinkerpop.gremlin.python.jsr223.SymbolHelper

import java.lang.reflect.Modifier

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GraphTraversalSourceGenerator {

    public static void create(final String graphTraversalSourceFile) {

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
        pythonClass.append("from traversal import Traversal\n")
        pythonClass.append("from traversal import Bytecode\n")
        pythonClass.append("from gremlin_python import statics\n\n")

//////////////////////////
// GraphTraversalSource //
//////////////////////////
        pythonClass.append(
                """class GraphTraversalSource(object):
  def __init__(self, graph, traversal_strategies, bytecode=None):
    self.graph = graph
    self.traversal_strategies = traversal_strategies
    if bytecode is None:
      bytecode = Bytecode()
    self.bytecode = bytecode
  def __repr__(self):
    return "graphtraversalsource[" + str(self.graph) + "]"
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
    traversal = GraphTraversal(self.graph, self.traversal_strategies, Bytecode(self.bytecode))
    traversal.bytecode.add_step("${method}", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
    return traversal
""")
                } else if (TraversalSource.isAssignableFrom(returnType)) {
                    pythonClass.append(
                            """  def ${method}(self, *args):
    source = GraphTraversalSource(self.graph, self.traversal_strategies, Bytecode(self.bytecode))
    source.bytecode.add_source("${method}", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
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
                """class GraphTraversal(Traversal):
  def __init__(self, graph, traversal_strategies, bytecode):
    Traversal.__init__(self, graph, traversal_strategies, bytecode)
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
    self.bytecode.add_step("${method}", *args)
    for arg in args:
      if isinstance(arg, tuple) and 2 == len(arg) and isinstance(arg[0], str):
        self.bindings[arg[0]] = arg[1]
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
    return GraphTraversal(None, None, Bytecode()).${method}(*args)
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
            pythonClass.append("statics.add_static('${it}', ${it})\n\n")
        }
        pythonClass.append("\n\n")

// save to a python file
        final File file = new File(graphTraversalSourceFile);
        file.delete()
        pythonClass.eachLine { file.append(it + "\n") }
    }
}
