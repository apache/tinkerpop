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

package org.apache.tinkerpop.gremlin.javascript

import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__
import org.apache.tinkerpop.gremlin.javascript.jsr223.SymbolHelper

import java.lang.reflect.Modifier

/**
 * @author Jorge Bay Gondra
 */
class GraphTraversalSourceGenerator {

    public static void create(final String graphTraversalSourceFile) {

        final StringBuilder moduleOutput = new StringBuilder()

        moduleOutput.append("""/*
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
 """)

//////////////////////////
// GraphTraversalSource //
//////////////////////////
        moduleOutput.append("""
/**
 * @author Jorge Bay Gondra
 */
(function defineGraphTraversalModule() {
  "use strict";

  var t = loadModule.call(this, './traversal.js');
  var remote = loadModule.call(this, '../driver/remote-connection.js');
  var Bytecode = t.Bytecode;
  var inherits = t.inherits;
  var parseArgs = t.parseArgs;

  /**
   *
   * @param {Graph} graph
   * @param {TraversalStrategies} traversalStrategies
   * @param {Bytecode} [bytecode]
   * @constructor
   */
  function GraphTraversalSource(graph, traversalStrategies, bytecode) {
    this._graph = graph;
    this._traversalStrategies = traversalStrategies;
    this._bytecode = bytecode || new Bytecode();
  }

  /**
   * @param remoteConnection
   * @returns {GraphTraversal}
   */
  GraphTraversalSource.prototype.withRemote = function (remoteConnection) {
    var traversalStrategy = new t.TraversalStrategies(this._traversalStrategies);
    traversalStrategy.addStrategy(new remote.RemoteStrategy(remoteConnection));
    return new GraphTraversal(this._graph, traversalStrategy, new Bytecode(this._bytecode));
  };

  /**
   * Returns the string representation of the GraphTraversalSource.
   * @returns {string}
   */
  GraphTraversalSource.prototype.toString = function () {
    return 'graphtraversalsource[' + this._graph.toString() + ']';
  };
""")
        GraphTraversalSource.getMethods(). // SOURCE STEPS
                findAll { GraphTraversalSource.class.equals(it.returnType) }.
                findAll {
                    !it.name.equals("clone") &&
                            !it.name.equals(TraversalSource.Symbols.withBindings) &&
                            !it.name.equals(TraversalSource.Symbols.withRemote)
                }.
                collect { SymbolHelper.toJs(it.name) }.
                unique().
                sort { a, b -> a <=> b }.
                forEach { method ->
                    moduleOutput.append(
                            """
  /**
   * ${method} GraphTraversalSource method.
   * @param {...Object} args
   * @returns {GraphTraversalSource}
   */
  GraphTraversalSource.prototype.${method} = function (args) {
    var b = new Bytecode(this._bytecode).addSource('${SymbolHelper.toJava(method)}', parseArgs.apply(null, arguments));
    return new GraphTraversalSource(this._graph, new t.TraversalStrategies(this._traversalStrategies), b);
  };
""")
                }
        GraphTraversalSource.getMethods(). // SPAWN STEPS
                findAll { GraphTraversal.class.equals(it.returnType) }.
                collect { SymbolHelper.toJs(it.name) }.
                unique().
                sort { a, b -> a <=> b }.
                forEach { method ->
                    moduleOutput.append(
                            """
  /**
   * ${method} GraphTraversalSource step method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  GraphTraversalSource.prototype.${method} = function (args) {
    var b = new Bytecode(this._bytecode).addStep('${SymbolHelper.toJava(method)}', parseArgs.apply(null, arguments));
    return new GraphTraversal(this._graph, new t.TraversalStrategies(this._traversalStrategies), b);
  };
""")
                }
////////////////////
// GraphTraversal //
////////////////////
        moduleOutput.append(
                """
  /**
   * Represents a graph traversal.
   * @constructor
   */
  function GraphTraversal(graph, traversalStrategies, bytecode) {
    t.Traversal.call(this, graph, traversalStrategies, bytecode);
  }

  inherits(GraphTraversal, t.Traversal);
""")
        GraphTraversal.getMethods().
                findAll { GraphTraversal.class.equals(it.returnType) }.
                findAll { !it.name.equals("clone") && !it.name.equals("iterate") }.
                collect { SymbolHelper.toJs(it.name) }.
                unique().
                sort { a, b -> a <=> b }.
                forEach { method ->
                    moduleOutput.append(
                            """
  /**
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  GraphTraversal.prototype.${method} = function (args) {
    this._bytecode.addStep('${SymbolHelper.toJava(method)}', parseArgs.apply(null, arguments));
    return this;
  };
""")
                };

////////////////////////
// AnonymousTraversal //
////////////////////////
        moduleOutput.append("""
  /**
   * Contains the static method definitions
   * @type {Object}
   */
  var statics = {};
""");
        __.class.getMethods().
                findAll { GraphTraversal.class.equals(it.returnType) }.
                findAll { Modifier.isStatic(it.getModifiers()) }.
                collect { SymbolHelper.toJs(it.name) }.
                unique().
                sort { a, b -> a <=> b }.
                forEach { method ->
                    moduleOutput.append(
                            """
  /**
   * ${method}() static method
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  statics.${method} = function (args) {
    var g = new GraphTraversal(null, null, new Bytecode());
    return g.${method}.apply(g, arguments);
  };
""")
                };

        moduleOutput.append("""
  function loadModule(moduleName) {
    if (typeof require !== 'undefined') {
      return require(moduleName);
    }
    if (typeof load !== 'undefined') {
      var path = new java.io.File(__DIR__ + moduleName).getCanonicalPath();
      this.__dependencies = this.__dependencies || {};
      return this.__dependencies[path] = (this.__dependencies[path] || load(path));
    }
    throw new Error('No module loader was found');
  }

  var toExport = {
    GraphTraversal: GraphTraversal,
    GraphTraversalSource: GraphTraversalSource,
    statics: statics
  };
  if (typeof module !== 'undefined') {
    // CommonJS
    module.exports = toExport;
    return;
  }
  // Nashorn and rest
  return toExport;
}).call(this);""")

        // save to file
        final File file = new File(graphTraversalSourceFile);
        file.delete()
        moduleOutput.eachLine { file.append(it + "\n") }
    }
}
