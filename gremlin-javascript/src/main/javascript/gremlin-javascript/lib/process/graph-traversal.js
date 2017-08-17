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

/**
 * Graph Traversal Source withBulk method.
 * @param {...Object} args
 * @returns {GraphTraversalSource}
 */
GraphTraversalSource.prototype.withBulk = function (args) {
  var b = new Bytecode(this.bytecode).addSource('withBulk', parseArgs.apply(null, arguments));
  return new GraphTraversalSource(this.graph, new TraversalStrategies(this.traversalStrategies), b);
};

/**
 * Graph Traversal Source withPath method.
 * @param {...Object} args
 * @returns {GraphTraversalSource}
 */
GraphTraversalSource.prototype.withPath = function (args) {
  var b = new Bytecode(this.bytecode).addSource('withPath', parseArgs.apply(null, arguments));
  return new GraphTraversalSource(this.graph, new TraversalStrategies(this.traversalStrategies), b);
};

/**
 * Graph Traversal Source withSack method.
 * @param {...Object} args
 * @returns {GraphTraversalSource}
 */
GraphTraversalSource.prototype.withSack = function (args) {
  var b = new Bytecode(this.bytecode).addSource('withSack', parseArgs.apply(null, arguments));
  return new GraphTraversalSource(this.graph, new TraversalStrategies(this.traversalStrategies), b);
};

/**
 * Graph Traversal Source withSideEffect method.
 * @param {...Object} args
 * @returns {GraphTraversalSource}
 */
GraphTraversalSource.prototype.withSideEffect = function (args) {
  var b = new Bytecode(this.bytecode).addSource('withSideEffect', parseArgs.apply(null, arguments));
  return new GraphTraversalSource(this.graph, new TraversalStrategies(this.traversalStrategies), b);
};

/**
 * Graph Traversal Source withStrategies method.
 * @param {...Object} args
 * @returns {GraphTraversalSource}
 */
GraphTraversalSource.prototype.withStrategies = function (args) {
  var b = new Bytecode(this.bytecode).addSource('withStrategies', parseArgs.apply(null, arguments));
  return new GraphTraversalSource(this.graph, new TraversalStrategies(this.traversalStrategies), b);
};

/**
 * Graph Traversal Source withoutStrategies method.
 * @param {...Object} args
 * @returns {GraphTraversalSource}
 */
GraphTraversalSource.prototype.withoutStrategies = function (args) {
  var b = new Bytecode(this.bytecode).addSource('withoutStrategies', parseArgs.apply(null, arguments));
  return new GraphTraversalSource(this.graph, new TraversalStrategies(this.traversalStrategies), b);
};

/**
 * E GraphTraversalSource step method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversalSource.prototype.E = function (args) {
  var b = new Bytecode(this.bytecode).addStep('E', parseArgs.apply(null, arguments));
  return new GraphTraversal(this.graph, new TraversalStrategies(this.traversalStrategies), b);
};

/**
 * V GraphTraversalSource step method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversalSource.prototype.V = function (args) {
  var b = new Bytecode(this.bytecode).addStep('V', parseArgs.apply(null, arguments));
  return new GraphTraversal(this.graph, new TraversalStrategies(this.traversalStrategies), b);
};

/**
 * addV GraphTraversalSource step method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversalSource.prototype.addV = function (args) {
  var b = new Bytecode(this.bytecode).addStep('addV', parseArgs.apply(null, arguments));
  return new GraphTraversal(this.graph, new TraversalStrategies(this.traversalStrategies), b);
};

/**
 * inject GraphTraversalSource step method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversalSource.prototype.inject = function (args) {
  var b = new Bytecode(this.bytecode).addStep('inject', parseArgs.apply(null, arguments));
  return new GraphTraversal(this.graph, new TraversalStrategies(this.traversalStrategies), b);
};

/**
 * Represents a graph traversal.
 * @extends Traversal
 * @constructor
 */
function GraphTraversal(graph, traversalStrategies, bytecode) {
  t.Traversal.call(this, graph, traversalStrategies, bytecode);
}

inherits(GraphTraversal, t.Traversal);

/**
 * Graph traversal V method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.V = function (args) {
  this.bytecode.addStep('V', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal addE method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.addE = function (args) {
  this.bytecode.addStep('addE', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal addInE method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.addInE = function (args) {
  this.bytecode.addStep('addInE', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal addOutE method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.addOutE = function (args) {
  this.bytecode.addStep('addOutE', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal addV method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.addV = function (args) {
  this.bytecode.addStep('addV', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal aggregate method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.aggregate = function (args) {
  this.bytecode.addStep('aggregate', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal and method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.and = function (args) {
  this.bytecode.addStep('and', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal as method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.as = function (args) {
  this.bytecode.addStep('as', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal barrier method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.barrier = function (args) {
  this.bytecode.addStep('barrier', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal both method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.both = function (args) {
  this.bytecode.addStep('both', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal bothE method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.bothE = function (args) {
  this.bytecode.addStep('bothE', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal bothV method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.bothV = function (args) {
  this.bytecode.addStep('bothV', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal branch method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.branch = function (args) {
  this.bytecode.addStep('branch', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal by method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.by = function (args) {
  this.bytecode.addStep('by', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal cap method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.cap = function (args) {
  this.bytecode.addStep('cap', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal choose method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.choose = function (args) {
  this.bytecode.addStep('choose', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal coalesce method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.coalesce = function (args) {
  this.bytecode.addStep('coalesce', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal coin method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.coin = function (args) {
  this.bytecode.addStep('coin', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal constant method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.constant = function (args) {
  this.bytecode.addStep('constant', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal count method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.count = function (args) {
  this.bytecode.addStep('count', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal cyclicPath method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.cyclicPath = function (args) {
  this.bytecode.addStep('cyclicPath', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal dedup method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.dedup = function (args) {
  this.bytecode.addStep('dedup', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal drop method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.drop = function (args) {
  this.bytecode.addStep('drop', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal emit method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.emit = function (args) {
  this.bytecode.addStep('emit', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal filter method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.filter = function (args) {
  this.bytecode.addStep('filter', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal flatMap method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.flatMap = function (args) {
  this.bytecode.addStep('flatMap', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal fold method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.fold = function (args) {
  this.bytecode.addStep('fold', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal from method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.from_ = function (args) {
  this.bytecode.addStep('from', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal group method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.group = function (args) {
  this.bytecode.addStep('group', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal groupCount method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.groupCount = function (args) {
  this.bytecode.addStep('groupCount', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal groupV3d0 method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.groupV3d0 = function (args) {
  this.bytecode.addStep('groupV3d0', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal has method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.has = function (args) {
  this.bytecode.addStep('has', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal hasId method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.hasId = function (args) {
  this.bytecode.addStep('hasId', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal hasKey method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.hasKey = function (args) {
  this.bytecode.addStep('hasKey', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal hasLabel method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.hasLabel = function (args) {
  this.bytecode.addStep('hasLabel', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal hasNot method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.hasNot = function (args) {
  this.bytecode.addStep('hasNot', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal hasValue method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.hasValue = function (args) {
  this.bytecode.addStep('hasValue', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal id method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.id = function (args) {
  this.bytecode.addStep('id', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal identity method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.identity = function (args) {
  this.bytecode.addStep('identity', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal in method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.in_ = function (args) {
  this.bytecode.addStep('in', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal inE method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.inE = function (args) {
  this.bytecode.addStep('inE', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal inV method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.inV = function (args) {
  this.bytecode.addStep('inV', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal inject method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.inject = function (args) {
  this.bytecode.addStep('inject', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal is method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.is = function (args) {
  this.bytecode.addStep('is', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal key method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.key = function (args) {
  this.bytecode.addStep('key', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal label method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.label = function (args) {
  this.bytecode.addStep('label', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal limit method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.limit = function (args) {
  this.bytecode.addStep('limit', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal local method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.local = function (args) {
  this.bytecode.addStep('local', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal loops method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.loops = function (args) {
  this.bytecode.addStep('loops', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal map method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.map = function (args) {
  this.bytecode.addStep('map', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal mapKeys method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.mapKeys = function (args) {
  this.bytecode.addStep('mapKeys', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal mapValues method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.mapValues = function (args) {
  this.bytecode.addStep('mapValues', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal match method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.match = function (args) {
  this.bytecode.addStep('match', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal max method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.max = function (args) {
  this.bytecode.addStep('max', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal mean method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.mean = function (args) {
  this.bytecode.addStep('mean', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal min method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.min = function (args) {
  this.bytecode.addStep('min', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal not method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.not = function (args) {
  this.bytecode.addStep('not', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal option method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.option = function (args) {
  this.bytecode.addStep('option', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal optional method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.optional = function (args) {
  this.bytecode.addStep('optional', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal or method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.or = function (args) {
  this.bytecode.addStep('or', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal order method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.order = function (args) {
  this.bytecode.addStep('order', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal otherV method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.otherV = function (args) {
  this.bytecode.addStep('otherV', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal out method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.out = function (args) {
  this.bytecode.addStep('out', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal outE method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.outE = function (args) {
  this.bytecode.addStep('outE', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal outV method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.outV = function (args) {
  this.bytecode.addStep('outV', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal pageRank method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.pageRank = function (args) {
  this.bytecode.addStep('pageRank', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal path method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.path = function (args) {
  this.bytecode.addStep('path', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal peerPressure method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.peerPressure = function (args) {
  this.bytecode.addStep('peerPressure', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal profile method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.profile = function (args) {
  this.bytecode.addStep('profile', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal program method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.program = function (args) {
  this.bytecode.addStep('program', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal project method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.project = function (args) {
  this.bytecode.addStep('project', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal properties method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.properties = function (args) {
  this.bytecode.addStep('properties', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal property method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.property = function (args) {
  this.bytecode.addStep('property', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal propertyMap method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.propertyMap = function (args) {
  this.bytecode.addStep('propertyMap', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal range method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.range = function (args) {
  this.bytecode.addStep('range', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal repeat method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.repeat = function (args) {
  this.bytecode.addStep('repeat', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal sack method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.sack = function (args) {
  this.bytecode.addStep('sack', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal sample method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.sample = function (args) {
  this.bytecode.addStep('sample', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal select method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.select = function (args) {
  this.bytecode.addStep('select', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal sideEffect method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.sideEffect = function (args) {
  this.bytecode.addStep('sideEffect', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal simplePath method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.simplePath = function (args) {
  this.bytecode.addStep('simplePath', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal store method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.store = function (args) {
  this.bytecode.addStep('store', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal subgraph method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.subgraph = function (args) {
  this.bytecode.addStep('subgraph', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal sum method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.sum = function (args) {
  this.bytecode.addStep('sum', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal tail method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.tail = function (args) {
  this.bytecode.addStep('tail', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal timeLimit method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.timeLimit = function (args) {
  this.bytecode.addStep('timeLimit', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal times method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.times = function (args) {
  this.bytecode.addStep('times', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal to method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.to = function (args) {
  this.bytecode.addStep('to', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal toE method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.toE = function (args) {
  this.bytecode.addStep('toE', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal toV method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.toV = function (args) {
  this.bytecode.addStep('toV', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal tree method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.tree = function (args) {
  this.bytecode.addStep('tree', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal unfold method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.unfold = function (args) {
  this.bytecode.addStep('unfold', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal union method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.union = function (args) {
  this.bytecode.addStep('union', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal until method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.until = function (args) {
  this.bytecode.addStep('until', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal value method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.value = function (args) {
  this.bytecode.addStep('value', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal valueMap method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.valueMap = function (args) {
  this.bytecode.addStep('valueMap', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal values method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.values = function (args) {
  this.bytecode.addStep('values', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Graph traversal where method.
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
GraphTraversal.prototype.where = function (args) {
  this.bytecode.addStep('where', parseArgs.apply(null, arguments));
  return this;
};

/**
 * Contains the static method definitions
 * @type {Object}
 */
var statics = {};

/**
 * V() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.V = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.V.apply(g, arguments);
};

/**
 * addE() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.addE = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.addE.apply(g, arguments);
};

/**
 * addInE() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.addInE = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.addInE.apply(g, arguments);
};

/**
 * addOutE() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.addOutE = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.addOutE.apply(g, arguments);
};

/**
 * addV() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.addV = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.addV.apply(g, arguments);
};

/**
 * aggregate() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.aggregate = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.aggregate.apply(g, arguments);
};

/**
 * and() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.and = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.and.apply(g, arguments);
};

/**
 * as() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.as = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.as.apply(g, arguments);
};

/**
 * barrier() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.barrier = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.barrier.apply(g, arguments);
};

/**
 * both() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.both = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.both.apply(g, arguments);
};

/**
 * bothE() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.bothE = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.bothE.apply(g, arguments);
};

/**
 * bothV() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.bothV = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.bothV.apply(g, arguments);
};

/**
 * branch() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.branch = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.branch.apply(g, arguments);
};

/**
 * cap() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.cap = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.cap.apply(g, arguments);
};

/**
 * choose() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.choose = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.choose.apply(g, arguments);
};

/**
 * coalesce() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.coalesce = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.coalesce.apply(g, arguments);
};

/**
 * coin() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.coin = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.coin.apply(g, arguments);
};

/**
 * constant() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.constant = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.constant.apply(g, arguments);
};

/**
 * count() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.count = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.count.apply(g, arguments);
};

/**
 * cyclicPath() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.cyclicPath = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.cyclicPath.apply(g, arguments);
};

/**
 * dedup() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.dedup = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.dedup.apply(g, arguments);
};

/**
 * drop() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.drop = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.drop.apply(g, arguments);
};

/**
 * emit() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.emit = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.emit.apply(g, arguments);
};

/**
 * filter() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.filter = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.filter.apply(g, arguments);
};

/**
 * flatMap() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.flatMap = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.flatMap.apply(g, arguments);
};

/**
 * fold() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.fold = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.fold.apply(g, arguments);
};

/**
 * group() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.group = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.group.apply(g, arguments);
};

/**
 * groupCount() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.groupCount = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.groupCount.apply(g, arguments);
};

/**
 * groupV3d0() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.groupV3d0 = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.groupV3d0.apply(g, arguments);
};

/**
 * has() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.has = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.has.apply(g, arguments);
};

/**
 * hasId() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.hasId = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.hasId.apply(g, arguments);
};

/**
 * hasKey() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.hasKey = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.hasKey.apply(g, arguments);
};

/**
 * hasLabel() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.hasLabel = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.hasLabel.apply(g, arguments);
};

/**
 * hasNot() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.hasNot = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.hasNot.apply(g, arguments);
};

/**
 * hasValue() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.hasValue = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.hasValue.apply(g, arguments);
};

/**
 * id() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.id = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.id.apply(g, arguments);
};

/**
 * identity() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.identity = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.identity.apply(g, arguments);
};

/**
 * in() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.in_ = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.in_.apply(g, arguments);
};

/**
 * inE() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.inE = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.inE.apply(g, arguments);
};

/**
 * inV() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.inV = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.inV.apply(g, arguments);
};

/**
 * inject() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.inject = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.inject.apply(g, arguments);
};

/**
 * is() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.is = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.is.apply(g, arguments);
};

/**
 * key() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.key = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.key.apply(g, arguments);
};

/**
 * label() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.label = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.label.apply(g, arguments);
};

/**
 * limit() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.limit = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.limit.apply(g, arguments);
};

/**
 * local() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.local = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.local.apply(g, arguments);
};

/**
 * loops() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.loops = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.loops.apply(g, arguments);
};

/**
 * map() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.map = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.map.apply(g, arguments);
};

/**
 * mapKeys() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.mapKeys = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.mapKeys.apply(g, arguments);
};

/**
 * mapValues() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.mapValues = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.mapValues.apply(g, arguments);
};

/**
 * match() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.match = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.match.apply(g, arguments);
};

/**
 * max() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.max = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.max.apply(g, arguments);
};

/**
 * mean() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.mean = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.mean.apply(g, arguments);
};

/**
 * min() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.min = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.min.apply(g, arguments);
};

/**
 * not() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.not = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.not.apply(g, arguments);
};

/**
 * optional() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.optional = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.optional.apply(g, arguments);
};

/**
 * or() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.or = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.or.apply(g, arguments);
};

/**
 * order() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.order = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.order.apply(g, arguments);
};

/**
 * otherV() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.otherV = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.otherV.apply(g, arguments);
};

/**
 * out() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.out = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.out.apply(g, arguments);
};

/**
 * outE() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.outE = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.outE.apply(g, arguments);
};

/**
 * outV() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.outV = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.outV.apply(g, arguments);
};

/**
 * path() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.path = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.path.apply(g, arguments);
};

/**
 * project() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.project = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.project.apply(g, arguments);
};

/**
 * properties() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.properties = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.properties.apply(g, arguments);
};

/**
 * property() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.property = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.property.apply(g, arguments);
};

/**
 * propertyMap() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.propertyMap = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.propertyMap.apply(g, arguments);
};

/**
 * range() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.range = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.range.apply(g, arguments);
};

/**
 * repeat() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.repeat = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.repeat.apply(g, arguments);
};

/**
 * sack() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.sack = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.sack.apply(g, arguments);
};

/**
 * sample() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.sample = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.sample.apply(g, arguments);
};

/**
 * select() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.select = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.select.apply(g, arguments);
};

/**
 * sideEffect() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.sideEffect = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.sideEffect.apply(g, arguments);
};

/**
 * simplePath() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.simplePath = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.simplePath.apply(g, arguments);
};

/**
 * store() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.store = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.store.apply(g, arguments);
};

/**
 * subgraph() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.subgraph = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.subgraph.apply(g, arguments);
};

/**
 * sum() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.sum = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.sum.apply(g, arguments);
};

/**
 * tail() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.tail = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.tail.apply(g, arguments);
};

/**
 * timeLimit() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.timeLimit = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.timeLimit.apply(g, arguments);
};

/**
 * times() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.times = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.times.apply(g, arguments);
};

/**
 * to() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.to = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.to.apply(g, arguments);
};

/**
 * toE() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.toE = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.toE.apply(g, arguments);
};

/**
 * toV() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.toV = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.toV.apply(g, arguments);
};

/**
 * tree() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.tree = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.tree.apply(g, arguments);
};

/**
 * unfold() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.unfold = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.unfold.apply(g, arguments);
};

/**
 * union() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.union = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.union.apply(g, arguments);
};

/**
 * until() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.until = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.until.apply(g, arguments);
};

/**
 * value() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.value = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.value.apply(g, arguments);
};

/**
 * valueMap() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.valueMap = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.valueMap.apply(g, arguments);
};

/**
 * values() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.values = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.values.apply(g, arguments);
};

/**
 * where() static method
 * @param {...Object} args
 * @returns {GraphTraversal}
 */
statics.where = function (args) {
  var g = new GraphTraversal(null, null, new Bytecode());
  return g.where.apply(g, arguments);
};

module.exports = {
  GraphTraversal: GraphTraversal,
  GraphTraversalSource: GraphTraversalSource,
  statics: statics
};