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
import org.apache.tinkerpop.gremlin.javascript.SymbolHelper

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
'use strict';

var t = require('./traversal.js');
var remote = require('../driver/remote-connection');
var utils = require('../utils');
var Bytecode = require('./bytecode');
var TraversalStrategies = require('./traversal-strategy').TraversalStrategies;
var inherits = utils.inherits;
var parseArgs = utils.parseArgs;

/**
 *
 * @param {Graph} graph
 * @param {TraversalStrategies} traversalStrategies
 * @param {Bytecode} [bytecode]
 * @constructor
 */
function GraphTraversalSource(graph, traversalStrategies, bytecode) {
  this.graph = graph;
  this.traversalStrategies = traversalStrategies;
  this.bytecode = bytecode || new Bytecode();
}

/**
 * @param remoteConnection
 * @returns {GraphTraversalSource}
 */
GraphTraversalSource.prototype.withRemote = function (remoteConnection) {
  var traversalStrategy = new TraversalStrategies(this.traversalStrategies);
  traversalStrategy.addStrategy(new remote.RemoteStrategy(remoteConnection));
  return new GraphTraversalSource(this.graph, traversalStrategy, new Bytecode(this.bytecode));
};

/**
 * Returns the string representation of the GraphTraversalSource.
 * @returns {string}
 */
GraphTraversalSource.prototype.toString = function () {
  return 'graphtraversalsource[' + this.graph.toString() + ']';
};
""")
        GraphTraversalSource.getMethods(). // SOURCE STEPS
                findAll { GraphTraversalSource.class.equals(it.returnType) }.
                findAll {
                    !it.name.equals("clone") &&
                            !it.name.equals(TraversalSource.Symbols.withBindings) &&
                            !it.name.equals(TraversalSource.Symbols.withRemote)
                }.
                collect { it.name }.
                unique().
                sort { a, b -> a <=> b }.
                forEach { methodName ->
                    moduleOutput.append(
                            """
/**
 * Graph Traversal Source ${methodName} method.
 * @param {...Object} args
 * @returns {GraphTraversalSource}
 */
GraphTraversalSource.prototype.${SymbolHelper.toJs(methodName)} = function (args) {
  var b = new Bytecode(this.bytecode).addSource('$methodName', parseArgs.apply(null, arguments));
  return new GraphTraversalSource(this.graph, new TraversalStrategies(this.traversalStrategies), b);
};
""")
                }
        GraphTraversalSource.getMethods().
                findAll { GraphTraversal.class.equals(it.returnType) }.
                collect { it.name }.
                unique().
                sort { a, b -> a <=> b }.
                forEach { methodName ->
                    moduleOutput.append(
                            """
/**
 * $methodName GraphTraversalSource step method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversalSource.prototype.${SymbolHelper.toJs(methodName)} = function (args) {
  var b = new Bytecode(this.bytecode).addStep('$methodName', parseArgs.apply(null, arguments));
  return new GraphTraversal(this.graph, new TraversalStrategies(this.traversalStrategies), b);
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
 * @extends Traversal
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
                collect { it.name }.
                unique().
                sort { a, b -> a <=> b }.
                forEach { methodName ->
                    moduleOutput.append(
                            """
/**
 * Graph traversal $methodName method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.${SymbolHelper.toJs(methodName)} = function (args) {
  this.bytecode.addStep('$methodName', parseArgs.apply(null, arguments));
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
                findAll { !it.name.equals("__") }.
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
module.exports = {
  GraphTraversal: GraphTraversal,
  GraphTraversalSource: GraphTraversalSource,
  statics: statics
};""");

        // save to file
        final File file = new File(graphTraversalSourceFile);
        file.delete()
        moduleOutput.eachLine { file.append(it + "\n") }
    }
}
