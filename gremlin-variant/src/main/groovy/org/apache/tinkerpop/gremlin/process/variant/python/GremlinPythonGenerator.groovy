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

import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__
import org.apache.tinkerpop.gremlin.util.Gremlin

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class GremlinPythonGenerator {

    public static void create(final String outputFileLocation) {
        final StringBuilder pythonClass = new StringBuilder()
        pythonClass.append("""
class Helper(object):
  @staticmethod
  def stringOrObject(arg):
    if (type(arg) is str and
       not(arg.startswith("P.")) and
       not(arg.startswith("Order.")) and
       not(arg.startswith("Scope.")) and
       not(arg.startswith("Pop.")) and
       not(arg.startswith("Column.")) and
       not(arg.startswith("T.")) and
       not(arg.startswith("Operator.")) and
       not(arg.startswith("SackFunctions.Barrier.")) and
       not(arg.startswith("Direction."))):
      return "\\"" + arg + "\\""
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
""");

///////////
// Enums //
///////////

        pythonClass.append("""class Column(object):
  keys = "Column.keys"
  values = "Column.values"
""").append("\n\n");

        pythonClass.append("""class Direction(object):
  IN = "Direction.IN"
  OUT = "Direction.OUT"
  BOTH = "Direction.BOTH"
""").append("\n\n");

        pythonClass.append("""class Order(object):
  incr = "Order.incr"
  decr = "Order.decr"
  shuffle = "Order.shuffle"
  keyIncr = "Order.keyIncr"
  keyDecr = "Order.keyDecr"
  valueIncr = "Order.valueIncr"
  valueDecr = "Order.valueDecr"
""").append("\n\n");

        pythonClass.append("""class Pop(object):
  first = "Pop.first"
  last = "Pop.last"
  all = "Pop.all"
""").append("\n\n");

        pythonClass.append("""class Scope(object):
  _local = "Scope.local"
  _global = "Scope.global"
""").append("\n\n");

        pythonClass.append("""class T(object):
  label = "T.label"
  id = "T.id"
  key = "T.key"
  value = "T.value"
""").append("\n\n");

//////////////////////////
// GraphTraversalSource //
//////////////////////////
        Set<String> methods = GraphTraversalSource.getMethods().collect { it.name } as Set;
        pythonClass.append(
                """class PythonGraphTraversalSource(object):
  def __init__(self, traversalSourceString):
    self.traversalSourceString = traversalSourceString
  def __repr__(self):
    return "graphtraversalsource[" + self.traversalSourceString + "]"
""")
        methods.each { method ->
            final Class<?> returnType = (GraphTraversalSource.getMethods() as Set).findAll {
                it.name.equals(method)
            }.collect {
                it.returnType
            }[0]
            if (null != returnType && Traversal.isAssignableFrom(returnType)) {
                pythonClass.append(
                        """  def ${method}(self, *args):
    return PythonGraphTraversal(self.traversalSourceString + ".${method}(" + Helper.stringify(*args) + ")")
""")
            } else {
                pythonClass.append(
                        """  def ${method}(self, *args):
    return PythonGraphTraversalSource(self.traversalSourceString + ".${method}(" + Helper.stringify(*args) + ")")
""")
            }
        }; []
        pythonClass.append("\n\n")

////////////////////
// GraphTraversal //
////////////////////
        final Map<String, String> methodMap = [as: "_as", in: "_in", and: "_and", or: "_or", is: "_is", not: "_not", from: "_from"].withDefault {
            it
        }
        final Map<String, String> invertedMethodMap = [_as: "as", _in: "in", _and: "and", _or: "or", _is: "is", _not: "not", _from: "from"].withDefault {
            it
        }
        methods = GraphTraversal.getMethods().collect { methodMap[it.name] } as Set;
        methods.remove("toList")
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
      raise TypeError("index must be int or slice")
  def __getattr__(self,key):
    return self.values(key)
""")
        methods.each { method ->
            final Class<?> returnType = (GraphTraversal.getMethods() as Set).findAll {
                it.name.equals(invertedMethodMap[method])
            }.collect { it.returnType }[0]
            if (null != returnType && Traversal.isAssignableFrom(returnType)) {
                pythonClass.append(
                        """  def ${method}(self, *args):
    self.traversalString = self.traversalString + ".${invertedMethodMap[method]}(" + Helper.stringify(*args) + ")"
    return self
""")
            } else {
                pythonClass.append(
                        """  def ${method}(self, *args):
    self.traversalString = self.traversalString + ".${invertedMethodMap[method]}(" + Helper.stringify(*args) + ")"
    return self.toList()
""")
            }
        };
        pythonClass.append("\n\n")

////////////////////////
// AnonymousTraversal //
////////////////////////
        methods = __.getMethods().collect { methodMap[it.name] } as Set; []
        pythonClass.append("class __(object):\n");
        methods.each { method ->
            pythonClass.append(
                    """  @staticmethod
  def ${method}(*args):
    return PythonGraphTraversal("__").${method}(*args)
""")
        };
        pythonClass.append("\n\n")

// save to a python file
        final File file = new File("${outputFileLocation}/gremlin-python-${Gremlin.version()}.py");
        file.delete()
        pythonClass.eachLine { file.append(it + "\n") }
    }
}
