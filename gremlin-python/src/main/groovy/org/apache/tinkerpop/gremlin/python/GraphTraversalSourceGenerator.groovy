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
        pythonClass.append("from .traversal import Traversal\n")
        pythonClass.append("from .traversal import TraversalStrategies\n")
        pythonClass.append("from .traversal import Bytecode\n")
        pythonClass.append("from ..driver.remote_connection import RemoteStrategy\n")
        pythonClass.append("from .. import statics\n\n")

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
        GraphTraversalSource.getMethods(). // SOURCE STEPS
                findAll { GraphTraversalSource.class.equals(it.returnType) }.
                findAll {
                    !it.name.equals("clone") &&
                            !it.name.equals(TraversalSource.Symbols.withBindings) &&
                            !it.name.equals(TraversalSource.Symbols.withRemote)
                }.
                collect { SymbolHelper.toPython(it.name) }.
                unique().
                sort { a, b -> a <=> b }.
                forEach { method ->
                    pythonClass.append(
                            """  def ${method}(self, *args):
    source = GraphTraversalSource(self.graph, TraversalStrategies(self.traversal_strategies), Bytecode(self.bytecode))
    source.bytecode.add_source("${SymbolHelper.toJava(method)}", *args)
    return source
""")
                }
        pythonClass.append(
                """  def withRemote(self, remote_connection):
    source = GraphTraversalSource(self.graph, TraversalStrategies(self.traversal_strategies), Bytecode(self.bytecode))
    source.traversal_strategies.add_strategies([RemoteStrategy(remote_connection)])
    return source
  def withBindings(self, bindings):
    return self
""")
        GraphTraversalSource.getMethods(). // SPAWN STEPS
                findAll { GraphTraversal.class.equals(it.returnType) }.
                collect { SymbolHelper.toPython(it.name) }.
                unique().
                sort { a, b -> a <=> b }.
                forEach { method ->
                    pythonClass.append(
                            """  def ${method}(self, *args):
    traversal = GraphTraversal(self.graph, self.traversal_strategies, Bytecode(self.bytecode))
    traversal.bytecode.add_step("${SymbolHelper.toJava(method)}", *args)
    return traversal
""")
                }
        pythonClass.append("\n\n")

////////////////////
// GraphTraversal //
////////////////////
        pythonClass.append(
                """class GraphTraversal(Traversal):
  def __init__(self, graph, traversal_strategies, bytecode):
    Traversal.__init__(self, graph, traversal_strategies, bytecode)
  def __getitem__(self, index):
    if isinstance(index, int):
        return self.range(index, index + 1)
    elif isinstance(index, slice):
        return self.range(index.start, index.stop)
    else:
        raise TypeError("Index must be int or slice")
  def __getattr__(self, key):
    return self.values(key)
""")
        GraphTraversal.getMethods().
                findAll { GraphTraversal.class.equals(it.returnType) }.
                findAll { !it.name.equals("clone") && !it.name.equals("iterate") }.
                collect { SymbolHelper.toPython(it.name) }.
                unique().
                sort { a, b -> a <=> b }.
                forEach { method ->
                    pythonClass.append(
                            """  def ${method}(self, *args):
    self.bytecode.add_step("${SymbolHelper.toJava(method)}", *args)
    return self
""")
                };
        pythonClass.append("\n\n")

////////////////////////
// AnonymousTraversal //
////////////////////////
        pythonClass.append("class __(object):\n");
        __.class.getMethods().
                findAll { GraphTraversal.class.equals(it.returnType) }.
                findAll { Modifier.isStatic(it.getModifiers()) }.
                collect { SymbolHelper.toPython(it.name) }.
                unique().
                sort { a, b -> a <=> b }.
                forEach { method ->
                    pythonClass.append(
                            """  @staticmethod
  def ${method}(*args):
    return GraphTraversal(None, None, Bytecode()).${method}(*args)
""")
                };
        pythonClass.append("\n\n")
        // add to gremlin.python.statics
        __.class.getMethods().
                findAll { GraphTraversal.class.equals(it.returnType) }.
                findAll { Modifier.isStatic(it.getModifiers()) }.
                findAll { !it.name.equals("__") }.
                collect { SymbolHelper.toPython(it.name) }.
                unique().
                sort { a, b -> a <=> b }.
                forEach {
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
