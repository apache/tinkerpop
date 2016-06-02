/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tinkerpop.gremlin.process.variant.python

import org.apache.tinkerpop.gremlin.process.computer.Computer
import org.apache.tinkerpop.gremlin.process.traversal.Operator
import org.apache.tinkerpop.gremlin.process.traversal.Order
import org.apache.tinkerpop.gremlin.process.traversal.P
import org.apache.tinkerpop.gremlin.process.traversal.Pop
import org.apache.tinkerpop.gremlin.process.traversal.SackFunctions
import org.apache.tinkerpop.gremlin.process.traversal.Scope
import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy
import org.apache.tinkerpop.gremlin.structure.Column
import org.apache.tinkerpop.gremlin.structure.Direction
import org.apache.tinkerpop.gremlin.structure.T
import org.apache.tinkerpop.gremlin.structure.VertexProperty

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GremlinPythonGenerator {

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

        final Map<String, String> methodMap = [global: "_global", as: "_as", in: "_in", and: "_and", or: "_or", is: "_is", not: "_not", from: "_from"]
                .withDefault { it }
        final Map<String, String> invertedMethodMap = [:].withDefault { it };
        methodMap.entrySet().forEach { invertedMethodMap.put(it.value, it.key) }

        final Set<String> NO_QUOTE = [VertexProperty.Cardinality, Column, Direction, Operator, Order, P, Pop, Scope, SackFunctions.Barrier, T,
                                      ReadOnlyStrategy, Computer]
                .collect { it.getSimpleName() }.toSet();
        final Map<String, String> enumMap = [Cardinality: "VertexProperty.Cardinality", Barrier: "SackFunctions.Barrier"]
                .withDefault { it }

        pythonClass.append("""
class Helper(object):
  @staticmethod
  def stringOrObject(arg):
    if (type(arg) is str and
""")
        NO_QUOTE.forEach { pythonClass.append("      not(arg.startswith(\"${enumMap[it]}.\")) and\n") }
        pythonClass.append("      not(len(arg)==0)):\n")
        pythonClass.append("""         return "\\"" + arg + "\\""
    elif type(arg) is bool:
      return str(arg).lower()
    elif type(arg) is long:
      return str(arg) + "L"
    elif type(arg) is float:
      return str(arg) + "f"
    else:
      return str(arg)

  @staticmethod
  def stringify(*args):
    if len(args) == 0:
      return ""
    elif len(args) == 1:
      return Helper.stringOrObject(args[0])
    else:
      return ", ".join(Helper.stringOrObject(i) for i in args)
""").append("\n\n");

///////////
// Enums //
///////////

        pythonClass.append("class Cardinality(object):\n");
        VertexProperty.Cardinality.values().each { value ->
            pythonClass.append("   ${value} = \"VertexProperty.Cardinality.${value}\"\n");
        }
        pythonClass.append("\n\n");

        pythonClass.append("class Column(object):\n");
        Column.values().each { value ->
            pythonClass.append("   ${value} = \"${value.getDeclaringClass().getSimpleName()}.${value}\"\n");
        }
        pythonClass.append("\n\n");

        pythonClass.append("class Direction(object):\n");
        Direction.values().each { value ->
            pythonClass.append("   ${value} = \"${value.getDeclaringClass().getSimpleName()}.${value}\"\n");
        }
        pythonClass.append("\n\n");

        pythonClass.append("class Operator(object):\n");
        Operator.values().each { value ->
            pythonClass.append("   ${methodMap[value.name()]} = \"${value.getDeclaringClass().getSimpleName()}.${value}\"\n");
        }
        pythonClass.append("\n\n");

        pythonClass.append("class Order(object):\n");
        Order.values().each { value ->
            pythonClass.append("   ${value} = \"${value.getDeclaringClass().getSimpleName()}.${value}\"\n");
        }
        pythonClass.append("\n\n");

        pythonClass.append("""class P(object):
   def __init__(self, pString):
      self.pString = pString
   def __repr__(self):
      return self.pString
""")
        P.getMethods()
                .findAll { P.class.isAssignableFrom(it.returnType) }
                .findAll { !it.name.equals("or") && !it.name.equals("and") }
                .collect { methodMap[it.name] }
                .toSet()
                .each { method ->
            pythonClass.append(
                    """   @staticmethod
   def ${method}(*args):
      return P("P.${invertedMethodMap[method]}(" + Helper.stringify(*args) + ")")
""")
        };
        pythonClass.append("""   def _and(self, arg):
      return P(self.pString + ".and(" + Helper.stringify(arg) + ")")
   def _or(self, arg):
      return P(self.pString + ".or(" + Helper.stringify(arg) + ")")
""")
        pythonClass.append("\n\n")

        pythonClass.append("class Pop(object):\n");
        Pop.values().each { value ->
            pythonClass.append("   ${value} = \"${value.getDeclaringClass().getSimpleName()}.${value}\"\n");
        }
        pythonClass.append("\n\n");

        pythonClass.append("""class Barrier(object):
   normSack = "SackFunctions.Barrier.normSack"
""").append("\n\n");

        pythonClass.append("class Scope(object):\n");
        Scope.values().each { value ->
            pythonClass.append("   _${value} = \"${value.getDeclaringClass().getSimpleName()}.${value}\"\n");
        }
        pythonClass.append("\n\n");

        pythonClass.append("class T(object):\n");
        T.values().each { value ->
            pythonClass.append("   ${value} = \"${value.getDeclaringClass().getSimpleName()}.${value}\"\n");
        }
        pythonClass.append("\n\n");

//////////////////////////
// GraphTraversalSource //
//////////////////////////
        pythonClass.append(
                """class PythonGraphTraversalSource(object):
  def __init__(self, traversalSourceString):
    self.traversalSourceString = traversalSourceString
  def __repr__(self):
    return "graphtraversalsource[" + self.traversalSourceString + "]"
""")
        GraphTraversalSource.getMethods().collect { it.name }.toSet().each { method ->
            final Class<?> returnType = (GraphTraversalSource.getMethods() as Set).findAll {
                it.name.equals(method)
            }.collect {
                it.returnType
            }[0]
            if (null != returnType) {
                if (Traversal.isAssignableFrom(returnType)) {
                    pythonClass.append(
                            """  def ${method}(self, *args):
    return PythonGraphTraversal(self.traversalSourceString + ".${method}(" + Helper.stringify(*args) + ")")
""")
                } else if (TraversalSource.isAssignableFrom(returnType)) {
                    pythonClass.append(
                            """  def ${method}(self, *args):
    return PythonGraphTraversalSource(self.traversalSourceString + ".${method}(" + Helper.stringify(*args) + ")")
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
  def __init__(self, traversalString):
    self.traversalString = traversalString
  def __repr__(self):
    return self.traversalString;
  def __getitem__(self,index):
    if type(index) is int:
      return self.range(index,index+1)
    elif type(index) is slice:
      return self.range(index.start,index.stop)
    else:
      raise TypeError("Index must be int or slice")
  def __getattr__(self,key):
    return self.values(key)
""")
        GraphTraversal.getMethods()
                .collect { methodMap[it.name] }
                .toSet()
                .each { method ->
            final Class<?> returnType = (GraphTraversal.getMethods() as Set).findAll {
                it.name.equals(invertedMethodMap[method])
            }.collect { it.returnType }[0]
            if (null != returnType && Traversal.isAssignableFrom(returnType)) {
                pythonClass.append(
                        """  def ${method}(self, *args):
    self.traversalString = self.traversalString + ".${invertedMethodMap[method]}(" + Helper.stringify(*args) + ")"
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
                .collect { methodMap[it.name] }
                .toSet()
                .each { method ->
            pythonClass.append(
                    """  @staticmethod
  def ${method}(*args):
    return PythonGraphTraversal("__").${method}(*args)
""")
        };
        pythonClass.append("\n\n")

// save to a python file
        final File file = new File(gremlinPythonFile);
        file.delete()
        pythonClass.eachLine { file.append(it + "\n") }
    }
}
