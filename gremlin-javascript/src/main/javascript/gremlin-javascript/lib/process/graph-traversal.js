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

const Traversal = require('./traversal').Traversal;
const remote = require('../driver/remote-connection');
const utils = require('../utils');
const Bytecode = require('./bytecode');
const TraversalStrategies = require('./traversal-strategy').TraversalStrategies;


/**
 * Represents the primary DSL of the Gremlin traversal machine.
 */
class GraphTraversalSource {
  /**
   * @param {Graph} graph
   * @param {TraversalStrategies} traversalStrategies
   * @param {Bytecode} [bytecode]
   */
  constructor(graph, traversalStrategies, bytecode) {
    this.graph = graph;
    this.traversalStrategies = traversalStrategies;
    this.bytecode = bytecode || new Bytecode();
  }

  /**
   * @param remoteConnection
   * @returns {GraphTraversalSource}
   */
  withRemote(remoteConnection) {
    const traversalStrategy = new TraversalStrategies(this.traversalStrategies);
    traversalStrategy.addStrategy(new remote.RemoteStrategy(remoteConnection));
    return new GraphTraversalSource(this.graph, traversalStrategy, new Bytecode(this.bytecode));
  }

  /**
   * Returns the string representation of the GraphTraversalSource.
   * @returns {string}
   */
  toString() {
    return 'graphtraversalsource[' + this.graph.toString() + ']';
  }
  
  /**
   * Graph Traversal Source withBulk method.
   * @param {...Object} args
   * @returns {GraphTraversalSource}
   */
  withBulk(...args) {
    const b = new Bytecode(this.bytecode).addSource('withBulk', args);
    return new GraphTraversalSource(this.graph, new TraversalStrategies(this.traversalStrategies), b);
  }
  
  /**
   * Graph Traversal Source withPath method.
   * @param {...Object} args
   * @returns {GraphTraversalSource}
   */
  withPath(...args) {
    const b = new Bytecode(this.bytecode).addSource('withPath', args);
    return new GraphTraversalSource(this.graph, new TraversalStrategies(this.traversalStrategies), b);
  }
  
  /**
   * Graph Traversal Source withSack method.
   * @param {...Object} args
   * @returns {GraphTraversalSource}
   */
  withSack(...args) {
    const b = new Bytecode(this.bytecode).addSource('withSack', args);
    return new GraphTraversalSource(this.graph, new TraversalStrategies(this.traversalStrategies), b);
  }
  
  /**
   * Graph Traversal Source withSideEffect method.
   * @param {...Object} args
   * @returns {GraphTraversalSource}
   */
  withSideEffect(...args) {
    const b = new Bytecode(this.bytecode).addSource('withSideEffect', args);
    return new GraphTraversalSource(this.graph, new TraversalStrategies(this.traversalStrategies), b);
  }
  
  /**
   * Graph Traversal Source withStrategies method.
   * @param {...Object} args
   * @returns {GraphTraversalSource}
   */
  withStrategies(...args) {
    const b = new Bytecode(this.bytecode).addSource('withStrategies', args);
    return new GraphTraversalSource(this.graph, new TraversalStrategies(this.traversalStrategies), b);
  }
  
  /**
   * Graph Traversal Source withoutStrategies method.
   * @param {...Object} args
   * @returns {GraphTraversalSource}
   */
  withoutStrategies(...args) {
    const b = new Bytecode(this.bytecode).addSource('withoutStrategies', args);
    return new GraphTraversalSource(this.graph, new TraversalStrategies(this.traversalStrategies), b);
  }
  
  /**
   * E GraphTraversalSource step method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  E(...args) {
    const b = new Bytecode(this.bytecode).addStep('E', args);
    return new GraphTraversal(this.graph, new TraversalStrategies(this.traversalStrategies), b);
  }
  
  /**
   * V GraphTraversalSource step method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  V(...args) {
    const b = new Bytecode(this.bytecode).addStep('V', args);
    return new GraphTraversal(this.graph, new TraversalStrategies(this.traversalStrategies), b);
  }
  
  /**
   * addV GraphTraversalSource step method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  addV(...args) {
    const b = new Bytecode(this.bytecode).addStep('addV', args);
    return new GraphTraversal(this.graph, new TraversalStrategies(this.traversalStrategies), b);
  }
  
  /**
   * inject GraphTraversalSource step method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  inject(...args) {
    const b = new Bytecode(this.bytecode).addStep('inject', args);
    return new GraphTraversal(this.graph, new TraversalStrategies(this.traversalStrategies), b);
  }
  
}

/**
 * Represents a graph traversal.
 */
class GraphTraversal extends Traversal {
  constructor(graph, traversalStrategies, bytecode) {
    super(graph, traversalStrategies, bytecode);
  }
  
  /**
   * Graph traversal V method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  V(...args) {
    this.bytecode.addStep('V', args);
    return this;
  }
  
  /**
   * Graph traversal addE method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  addE(...args) {
    this.bytecode.addStep('addE', args);
    return this;
  }
  
  /**
   * Graph traversal addInE method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  addInE(...args) {
    this.bytecode.addStep('addInE', args);
    return this;
  }
  
  /**
   * Graph traversal addOutE method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  addOutE(...args) {
    this.bytecode.addStep('addOutE', args);
    return this;
  }
  
  /**
   * Graph traversal addV method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  addV(...args) {
    this.bytecode.addStep('addV', args);
    return this;
  }
  
  /**
   * Graph traversal aggregate method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  aggregate(...args) {
    this.bytecode.addStep('aggregate', args);
    return this;
  }
  
  /**
   * Graph traversal and method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  and(...args) {
    this.bytecode.addStep('and', args);
    return this;
  }
  
  /**
   * Graph traversal as method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  as(...args) {
    this.bytecode.addStep('as', args);
    return this;
  }
  
  /**
   * Graph traversal barrier method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  barrier(...args) {
    this.bytecode.addStep('barrier', args);
    return this;
  }
  
  /**
   * Graph traversal both method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  both(...args) {
    this.bytecode.addStep('both', args);
    return this;
  }
  
  /**
   * Graph traversal bothE method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  bothE(...args) {
    this.bytecode.addStep('bothE', args);
    return this;
  }
  
  /**
   * Graph traversal bothV method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  bothV(...args) {
    this.bytecode.addStep('bothV', args);
    return this;
  }
  
  /**
   * Graph traversal branch method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  branch(...args) {
    this.bytecode.addStep('branch', args);
    return this;
  }
  
  /**
   * Graph traversal by method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  by(...args) {
    this.bytecode.addStep('by', args);
    return this;
  }
  
  /**
   * Graph traversal cap method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  cap(...args) {
    this.bytecode.addStep('cap', args);
    return this;
  }
  
  /**
   * Graph traversal choose method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  choose(...args) {
    this.bytecode.addStep('choose', args);
    return this;
  }
  
  /**
   * Graph traversal coalesce method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  coalesce(...args) {
    this.bytecode.addStep('coalesce', args);
    return this;
  }
  
  /**
   * Graph traversal coin method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  coin(...args) {
    this.bytecode.addStep('coin', args);
    return this;
  }
  
  /**
   * Graph traversal constant method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  constant(...args) {
    this.bytecode.addStep('constant', args);
    return this;
  }
  
  /**
   * Graph traversal count method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  count(...args) {
    this.bytecode.addStep('count', args);
    return this;
  }
  
  /**
   * Graph traversal cyclicPath method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  cyclicPath(...args) {
    this.bytecode.addStep('cyclicPath', args);
    return this;
  }
  
  /**
   * Graph traversal dedup method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  dedup(...args) {
    this.bytecode.addStep('dedup', args);
    return this;
  }
  
  /**
   * Graph traversal drop method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  drop(...args) {
    this.bytecode.addStep('drop', args);
    return this;
  }
  
  /**
   * Graph traversal emit method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  emit(...args) {
    this.bytecode.addStep('emit', args);
    return this;
  }
  
  /**
   * Graph traversal filter method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  filter(...args) {
    this.bytecode.addStep('filter', args);
    return this;
  }
  
  /**
   * Graph traversal flatMap method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  flatMap(...args) {
    this.bytecode.addStep('flatMap', args);
    return this;
  }
  
  /**
   * Graph traversal fold method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  fold(...args) {
    this.bytecode.addStep('fold', args);
    return this;
  }
  
  /**
   * Graph traversal from method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  from_(...args) {
    this.bytecode.addStep('from', args);
    return this;
  }
  
  /**
   * Graph traversal group method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  group(...args) {
    this.bytecode.addStep('group', args);
    return this;
  }
  
  /**
   * Graph traversal groupCount method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  groupCount(...args) {
    this.bytecode.addStep('groupCount', args);
    return this;
  }
  
  /**
   * Graph traversal groupV3d0 method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  groupV3d0(...args) {
    this.bytecode.addStep('groupV3d0', args);
    return this;
  }
  
  /**
   * Graph traversal has method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  has(...args) {
    this.bytecode.addStep('has', args);
    return this;
  }
  
  /**
   * Graph traversal hasId method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  hasId(...args) {
    this.bytecode.addStep('hasId', args);
    return this;
  }
  
  /**
   * Graph traversal hasKey method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  hasKey(...args) {
    this.bytecode.addStep('hasKey', args);
    return this;
  }
  
  /**
   * Graph traversal hasLabel method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  hasLabel(...args) {
    this.bytecode.addStep('hasLabel', args);
    return this;
  }
  
  /**
   * Graph traversal hasNot method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  hasNot(...args) {
    this.bytecode.addStep('hasNot', args);
    return this;
  }
  
  /**
   * Graph traversal hasValue method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  hasValue(...args) {
    this.bytecode.addStep('hasValue', args);
    return this;
  }
  
  /**
   * Graph traversal id method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  id(...args) {
    this.bytecode.addStep('id', args);
    return this;
  }
  
  /**
   * Graph traversal identity method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  identity(...args) {
    this.bytecode.addStep('identity', args);
    return this;
  }
  
  /**
   * Graph traversal in method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  in_(...args) {
    this.bytecode.addStep('in', args);
    return this;
  }
  
  /**
   * Graph traversal inE method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  inE(...args) {
    this.bytecode.addStep('inE', args);
    return this;
  }
  
  /**
   * Graph traversal inV method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  inV(...args) {
    this.bytecode.addStep('inV', args);
    return this;
  }
  
  /**
   * Graph traversal inject method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  inject(...args) {
    this.bytecode.addStep('inject', args);
    return this;
  }
  
  /**
   * Graph traversal is method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  is(...args) {
    this.bytecode.addStep('is', args);
    return this;
  }
  
  /**
   * Graph traversal key method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  key(...args) {
    this.bytecode.addStep('key', args);
    return this;
  }
  
  /**
   * Graph traversal label method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  label(...args) {
    this.bytecode.addStep('label', args);
    return this;
  }
  
  /**
   * Graph traversal limit method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  limit(...args) {
    this.bytecode.addStep('limit', args);
    return this;
  }
  
  /**
   * Graph traversal local method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  local(...args) {
    this.bytecode.addStep('local', args);
    return this;
  }
  
  /**
   * Graph traversal loops method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  loops(...args) {
    this.bytecode.addStep('loops', args);
    return this;
  }
  
  /**
   * Graph traversal map method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  map(...args) {
    this.bytecode.addStep('map', args);
    return this;
  }
  
  /**
   * Graph traversal mapKeys method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  mapKeys(...args) {
    this.bytecode.addStep('mapKeys', args);
    return this;
  }
  
  /**
   * Graph traversal mapValues method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  mapValues(...args) {
    this.bytecode.addStep('mapValues', args);
    return this;
  }
  
  /**
   * Graph traversal match method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  match(...args) {
    this.bytecode.addStep('match', args);
    return this;
  }
  
  /**
   * Graph traversal max method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  max(...args) {
    this.bytecode.addStep('max', args);
    return this;
  }
  
  /**
   * Graph traversal mean method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  mean(...args) {
    this.bytecode.addStep('mean', args);
    return this;
  }
  
  /**
   * Graph traversal min method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  min(...args) {
    this.bytecode.addStep('min', args);
    return this;
  }
  
  /**
   * Graph traversal not method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  not(...args) {
    this.bytecode.addStep('not', args);
    return this;
  }
  
  /**
   * Graph traversal option method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  option(...args) {
    this.bytecode.addStep('option', args);
    return this;
  }
  
  /**
   * Graph traversal optional method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  optional(...args) {
    this.bytecode.addStep('optional', args);
    return this;
  }
  
  /**
   * Graph traversal or method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  or(...args) {
    this.bytecode.addStep('or', args);
    return this;
  }
  
  /**
   * Graph traversal order method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  order(...args) {
    this.bytecode.addStep('order', args);
    return this;
  }
  
  /**
   * Graph traversal otherV method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  otherV(...args) {
    this.bytecode.addStep('otherV', args);
    return this;
  }
  
  /**
   * Graph traversal out method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  out(...args) {
    this.bytecode.addStep('out', args);
    return this;
  }
  
  /**
   * Graph traversal outE method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  outE(...args) {
    this.bytecode.addStep('outE', args);
    return this;
  }
  
  /**
   * Graph traversal outV method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  outV(...args) {
    this.bytecode.addStep('outV', args);
    return this;
  }
  
  /**
   * Graph traversal pageRank method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  pageRank(...args) {
    this.bytecode.addStep('pageRank', args);
    return this;
  }
  
  /**
   * Graph traversal path method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  path(...args) {
    this.bytecode.addStep('path', args);
    return this;
  }
  
  /**
   * Graph traversal peerPressure method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  peerPressure(...args) {
    this.bytecode.addStep('peerPressure', args);
    return this;
  }
  
  /**
   * Graph traversal profile method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  profile(...args) {
    this.bytecode.addStep('profile', args);
    return this;
  }
  
  /**
   * Graph traversal program method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  program(...args) {
    this.bytecode.addStep('program', args);
    return this;
  }
  
  /**
   * Graph traversal project method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  project(...args) {
    this.bytecode.addStep('project', args);
    return this;
  }
  
  /**
   * Graph traversal properties method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  properties(...args) {
    this.bytecode.addStep('properties', args);
    return this;
  }
  
  /**
   * Graph traversal property method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  property(...args) {
    this.bytecode.addStep('property', args);
    return this;
  }
  
  /**
   * Graph traversal propertyMap method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  propertyMap(...args) {
    this.bytecode.addStep('propertyMap', args);
    return this;
  }
  
  /**
   * Graph traversal range method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  range(...args) {
    this.bytecode.addStep('range', args);
    return this;
  }
  
  /**
   * Graph traversal repeat method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  repeat(...args) {
    this.bytecode.addStep('repeat', args);
    return this;
  }
  
  /**
   * Graph traversal sack method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  sack(...args) {
    this.bytecode.addStep('sack', args);
    return this;
  }
  
  /**
   * Graph traversal sample method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  sample(...args) {
    this.bytecode.addStep('sample', args);
    return this;
  }
  
  /**
   * Graph traversal select method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  select(...args) {
    this.bytecode.addStep('select', args);
    return this;
  }
  
  /**
   * Graph traversal sideEffect method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  sideEffect(...args) {
    this.bytecode.addStep('sideEffect', args);
    return this;
  }
  
  /**
   * Graph traversal simplePath method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  simplePath(...args) {
    this.bytecode.addStep('simplePath', args);
    return this;
  }
  
  /**
   * Graph traversal store method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  store(...args) {
    this.bytecode.addStep('store', args);
    return this;
  }
  
  /**
   * Graph traversal subgraph method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  subgraph(...args) {
    this.bytecode.addStep('subgraph', args);
    return this;
  }
  
  /**
   * Graph traversal sum method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  sum(...args) {
    this.bytecode.addStep('sum', args);
    return this;
  }
  
  /**
   * Graph traversal tail method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  tail(...args) {
    this.bytecode.addStep('tail', args);
    return this;
  }
  
  /**
   * Graph traversal timeLimit method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  timeLimit(...args) {
    this.bytecode.addStep('timeLimit', args);
    return this;
  }
  
  /**
   * Graph traversal times method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  times(...args) {
    this.bytecode.addStep('times', args);
    return this;
  }
  
  /**
   * Graph traversal to method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  to(...args) {
    this.bytecode.addStep('to', args);
    return this;
  }
  
  /**
   * Graph traversal toE method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  toE(...args) {
    this.bytecode.addStep('toE', args);
    return this;
  }
  
  /**
   * Graph traversal toV method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  toV(...args) {
    this.bytecode.addStep('toV', args);
    return this;
  }
  
  /**
   * Graph traversal tree method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  tree(...args) {
    this.bytecode.addStep('tree', args);
    return this;
  }
  
  /**
   * Graph traversal unfold method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  unfold(...args) {
    this.bytecode.addStep('unfold', args);
    return this;
  }
  
  /**
   * Graph traversal union method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  union(...args) {
    this.bytecode.addStep('union', args);
    return this;
  }
  
  /**
   * Graph traversal until method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  until(...args) {
    this.bytecode.addStep('until', args);
    return this;
  }
  
  /**
   * Graph traversal value method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  value(...args) {
    this.bytecode.addStep('value', args);
    return this;
  }
  
  /**
   * Graph traversal valueMap method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  valueMap(...args) {
    this.bytecode.addStep('valueMap', args);
    return this;
  }
  
  /**
   * Graph traversal values method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  values(...args) {
    this.bytecode.addStep('values', args);
    return this;
  }
  
  /**
   * Graph traversal where method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  where(...args) {
    this.bytecode.addStep('where', args);
    return this;
  }
  
}

function callOnEmptyTraversal(fnName, args) {
  const g = new GraphTraversal(null, null, new Bytecode());
  return g[fnName].apply(g, args);
}

/**
 * Contains the static method definitions
 * @type {Object}
 */
const statics = {
  V: (...args) => callOnEmptyTraversal('V', args),
  addE: (...args) => callOnEmptyTraversal('addE', args),
  addInE: (...args) => callOnEmptyTraversal('addInE', args),
  addOutE: (...args) => callOnEmptyTraversal('addOutE', args),
  addV: (...args) => callOnEmptyTraversal('addV', args),
  aggregate: (...args) => callOnEmptyTraversal('aggregate', args),
  and: (...args) => callOnEmptyTraversal('and', args),
  as: (...args) => callOnEmptyTraversal('as', args),
  barrier: (...args) => callOnEmptyTraversal('barrier', args),
  both: (...args) => callOnEmptyTraversal('both', args),
  bothE: (...args) => callOnEmptyTraversal('bothE', args),
  bothV: (...args) => callOnEmptyTraversal('bothV', args),
  branch: (...args) => callOnEmptyTraversal('branch', args),
  cap: (...args) => callOnEmptyTraversal('cap', args),
  choose: (...args) => callOnEmptyTraversal('choose', args),
  coalesce: (...args) => callOnEmptyTraversal('coalesce', args),
  coin: (...args) => callOnEmptyTraversal('coin', args),
  constant: (...args) => callOnEmptyTraversal('constant', args),
  count: (...args) => callOnEmptyTraversal('count', args),
  cyclicPath: (...args) => callOnEmptyTraversal('cyclicPath', args),
  dedup: (...args) => callOnEmptyTraversal('dedup', args),
  drop: (...args) => callOnEmptyTraversal('drop', args),
  emit: (...args) => callOnEmptyTraversal('emit', args),
  filter: (...args) => callOnEmptyTraversal('filter', args),
  flatMap: (...args) => callOnEmptyTraversal('flatMap', args),
  fold: (...args) => callOnEmptyTraversal('fold', args),
  group: (...args) => callOnEmptyTraversal('group', args),
  groupCount: (...args) => callOnEmptyTraversal('groupCount', args),
  groupV3d0: (...args) => callOnEmptyTraversal('groupV3d0', args),
  has: (...args) => callOnEmptyTraversal('has', args),
  hasId: (...args) => callOnEmptyTraversal('hasId', args),
  hasKey: (...args) => callOnEmptyTraversal('hasKey', args),
  hasLabel: (...args) => callOnEmptyTraversal('hasLabel', args),
  hasNot: (...args) => callOnEmptyTraversal('hasNot', args),
  hasValue: (...args) => callOnEmptyTraversal('hasValue', args),
  id: (...args) => callOnEmptyTraversal('id', args),
  identity: (...args) => callOnEmptyTraversal('identity', args),
  in_: (...args) => callOnEmptyTraversal('in_', args),
  inE: (...args) => callOnEmptyTraversal('inE', args),
  inV: (...args) => callOnEmptyTraversal('inV', args),
  inject: (...args) => callOnEmptyTraversal('inject', args),
  is: (...args) => callOnEmptyTraversal('is', args),
  key: (...args) => callOnEmptyTraversal('key', args),
  label: (...args) => callOnEmptyTraversal('label', args),
  limit: (...args) => callOnEmptyTraversal('limit', args),
  local: (...args) => callOnEmptyTraversal('local', args),
  loops: (...args) => callOnEmptyTraversal('loops', args),
  map: (...args) => callOnEmptyTraversal('map', args),
  mapKeys: (...args) => callOnEmptyTraversal('mapKeys', args),
  mapValues: (...args) => callOnEmptyTraversal('mapValues', args),
  match: (...args) => callOnEmptyTraversal('match', args),
  max: (...args) => callOnEmptyTraversal('max', args),
  mean: (...args) => callOnEmptyTraversal('mean', args),
  min: (...args) => callOnEmptyTraversal('min', args),
  not: (...args) => callOnEmptyTraversal('not', args),
  optional: (...args) => callOnEmptyTraversal('optional', args),
  or: (...args) => callOnEmptyTraversal('or', args),
  order: (...args) => callOnEmptyTraversal('order', args),
  otherV: (...args) => callOnEmptyTraversal('otherV', args),
  out: (...args) => callOnEmptyTraversal('out', args),
  outE: (...args) => callOnEmptyTraversal('outE', args),
  outV: (...args) => callOnEmptyTraversal('outV', args),
  path: (...args) => callOnEmptyTraversal('path', args),
  project: (...args) => callOnEmptyTraversal('project', args),
  properties: (...args) => callOnEmptyTraversal('properties', args),
  property: (...args) => callOnEmptyTraversal('property', args),
  propertyMap: (...args) => callOnEmptyTraversal('propertyMap', args),
  range: (...args) => callOnEmptyTraversal('range', args),
  repeat: (...args) => callOnEmptyTraversal('repeat', args),
  sack: (...args) => callOnEmptyTraversal('sack', args),
  sample: (...args) => callOnEmptyTraversal('sample', args),
  select: (...args) => callOnEmptyTraversal('select', args),
  sideEffect: (...args) => callOnEmptyTraversal('sideEffect', args),
  simplePath: (...args) => callOnEmptyTraversal('simplePath', args),
  store: (...args) => callOnEmptyTraversal('store', args),
  subgraph: (...args) => callOnEmptyTraversal('subgraph', args),
  sum: (...args) => callOnEmptyTraversal('sum', args),
  tail: (...args) => callOnEmptyTraversal('tail', args),
  timeLimit: (...args) => callOnEmptyTraversal('timeLimit', args),
  times: (...args) => callOnEmptyTraversal('times', args),
  to: (...args) => callOnEmptyTraversal('to', args),
  toE: (...args) => callOnEmptyTraversal('toE', args),
  toV: (...args) => callOnEmptyTraversal('toV', args),
  tree: (...args) => callOnEmptyTraversal('tree', args),
  unfold: (...args) => callOnEmptyTraversal('unfold', args),
  union: (...args) => callOnEmptyTraversal('union', args),
  until: (...args) => callOnEmptyTraversal('until', args),
  value: (...args) => callOnEmptyTraversal('value', args),
  valueMap: (...args) => callOnEmptyTraversal('valueMap', args),
  values: (...args) => callOnEmptyTraversal('values', args),
  where: (...args) => callOnEmptyTraversal('where', args)
};

module.exports = {
  GraphTraversal,
  GraphTraversalSource,
  statics
};