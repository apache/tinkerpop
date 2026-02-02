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

import { EnumValue, Traversal, cardinality, direction } from './traversal.js';
import { Transaction } from './transaction.js';
import { TraversalStrategies, VertexProgramStrategy, OptionsStrategy } from './traversal-strategy.js';
import {Graph, Vertex} from '../structure/graph.js';
import { RemoteConnection, RemoteStrategy } from '../driver/remote-connection.js';
import GremlinLang from './gremlin-lang.js';

/**
 * Represents the primary DSL of the Gremlin traversal machine.
 */
export class GraphTraversalSource {
  remoteConnection?: RemoteConnection;

  /**
   * Creates a new instance of {@link GraphTraversalSource}.
   * @param {Graph} graph
   * @param {TraversalStrategies} traversalStrategies
   * @param {GremlinLang} [gremlinLang] Optional GremlinLang instance.
   * @param {Function} [graphTraversalSourceClass] Optional {@link GraphTraversalSource} constructor.
   * @param {Function} [graphTraversalClass] Optional {@link GraphTraversal} constructor.
   */
  constructor(
    public graph: Graph,
    public traversalStrategies: TraversalStrategies,
    public gremlinLang: GremlinLang = new GremlinLang(),
    public graphTraversalSourceClass: typeof GraphTraversalSource = GraphTraversalSource,
    public graphTraversalClass: typeof GraphTraversal = GraphTraversal,
  ) {
    const strat = traversalStrategies.strategies.find((ts) => ts.fqcn === 'js:RemoteStrategy');
    this.remoteConnection = strat !== undefined ? strat.connection : undefined;
  }

  /**
   * Spawn a new <code>Transaction</code> object that can then start and stop a transaction.
   * @returns {Transaction}
   */
  tx(): Transaction {
    // you can't do g.tx().begin().tx() - no child transactions
    if (this.remoteConnection && this.remoteConnection.isSessionBound) {
      throw new Error('This TraversalSource is already bound to a transaction - child transactions are not supported');
    }

    return new Transaction(this);
  }

  /**
   * @param graphComputer
   * @param workers
   * @param result
   * @param persist
   * @param vertices
   * @param edges
   * @param configuration
   * @returns {GraphTraversalSource}
   */
  withComputer(
    graphComputer: any,
    workers: any,
    result: any,
    persist: any,
    vertices: any[],
    edges: any[],
    configuration: any,
  ): GraphTraversalSource {
    const m: any = {};
    if (graphComputer !== undefined) {
      m.graphComputer = graphComputer;
    }
    if (workers !== undefined) {
      m.workers = workers;
    }
    if (result !== undefined) {
      m.result = result;
    }
    if (persist !== undefined) {
      m.graphComputer = persist;
    }
    if (vertices !== undefined) {
      m.vertices = vertices;
    }
    if (edges !== undefined) {
      m.edges = edges;
    }
    if (configuration !== undefined) {
      m.configuration = configuration;
    }
    return this.withStrategies(new VertexProgramStrategy(m));
  }

  /**
   * Graph Traversal Source with method.
   * @param {String} key
   * @param {Object} value if not specified, the value with default to {@code true}
   * @returns {GraphTraversalSource}
   */
  with_(key: string, value: object | undefined = undefined): GraphTraversalSource {
    const val = value === undefined ? true : value;
    const glStrategies = this.gremlinLang.getOptionsStrategies();
    if (glStrategies.length === 0) {
      return this.withStrategies(new OptionsStrategy({ [key]: val }));
    }
    glStrategies[glStrategies.length - 1].configuration[key] = val;
    const gl = new GremlinLang(this.gremlinLang);
    return new this.graphTraversalSourceClass(
      this.graph,
      new TraversalStrategies(this.traversalStrategies),
      gl,
      this.graphTraversalSourceClass,
      this.graphTraversalClass,
    );
  }

  /**
   * Returns the string representation of the GraphTraversalSource.
   * @returns {string}
   */
  toString(): string {
    return 'graphtraversalsource[' + this.graph.toString() + ']';
  }

  /**
   * Graph Traversal Source withBulk method.
   * @param {...Object} args
   * @returns {GraphTraversalSource}
   */
  withBulk(...args: any[]): GraphTraversalSource {
    const gl = new GremlinLang(this.gremlinLang).addSource('withBulk', args);
    return new this.graphTraversalSourceClass(
      this.graph,
      new TraversalStrategies(this.traversalStrategies),
      gl,
      this.graphTraversalSourceClass,
      this.graphTraversalClass,
    );
  }

  /**
   * Graph Traversal Source withPath method.
   * @param {...Object} args
   * @returns {GraphTraversalSource}
   */
  withPath(...args: any[]): GraphTraversalSource {
    const gl = new GremlinLang(this.gremlinLang).addSource('withPath', args);
    return new this.graphTraversalSourceClass(
      this.graph,
      new TraversalStrategies(this.traversalStrategies),
      gl,
      this.graphTraversalSourceClass,
      this.graphTraversalClass,
    );
  }

  /**
   * Graph Traversal Source withSack method.
   * @param {...Object} args
   * @returns {GraphTraversalSource}
   */
  withSack(...args: any[]): GraphTraversalSource {
    const gl = new GremlinLang(this.gremlinLang).addSource('withSack', args);
    return new this.graphTraversalSourceClass(
      this.graph,
      new TraversalStrategies(this.traversalStrategies),
      gl,
      this.graphTraversalSourceClass,
      this.graphTraversalClass,
    );
  }

  /**
   * Graph Traversal Source withSideEffect method.
   * @param {...Object} args
   * @returns {GraphTraversalSource}
   */
  withSideEffect(...args: any[]): GraphTraversalSource {
    const gl = new GremlinLang(this.gremlinLang).addSource('withSideEffect', args);
    return new this.graphTraversalSourceClass(
      this.graph,
      new TraversalStrategies(this.traversalStrategies),
      gl,
      this.graphTraversalSourceClass,
      this.graphTraversalClass,
    );
  }

  /**
   * Graph Traversal Source withStrategies method.
   * @param {...Object} args
   * @returns {GraphTraversalSource}
   */
  withStrategies(...args: any[]): GraphTraversalSource {
    const gl = new GremlinLang(this.gremlinLang).addSource('withStrategies', args);
    return new this.graphTraversalSourceClass(
      this.graph,
      new TraversalStrategies(this.traversalStrategies),
      gl,
      this.graphTraversalSourceClass,
      this.graphTraversalClass,
    );
  }

  /**
   * Graph Traversal Source withoutStrategies method.
   * @param {...Object} args
   * @returns {GraphTraversalSource}
   */
  withoutStrategies(...args: any[]): GraphTraversalSource {
    const gl = new GremlinLang(this.gremlinLang).addSource('withoutStrategies', args);
    return new this.graphTraversalSourceClass(
      this.graph,
      new TraversalStrategies(this.traversalStrategies),
      gl,
      this.graphTraversalSourceClass,
      this.graphTraversalClass,
    );
  }

  /**
   * E GraphTraversalSource step method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  E(...args: any[]): GraphTraversal {
    const gl = new GremlinLang(this.gremlinLang).addStep('E', args);
    return new this.graphTraversalClass(this.graph, new TraversalStrategies(this.traversalStrategies), gl);
  }

  /**
   * V GraphTraversalSource step method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  V(...args: any[]): GraphTraversal {
    for (let i = 0; i < args.length; i++) {
      if (args[i] instanceof Vertex) {
        args[i] = args[i].id
      }
    }
    const gl = new GremlinLang(this.gremlinLang).addStep('V', args);
    return new this.graphTraversalClass(this.graph, new TraversalStrategies(this.traversalStrategies), gl);
  }

  /**
   * addE GraphTraversalSource step method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  addE(...args: any[]): GraphTraversal {
    const gl = new GremlinLang(this.gremlinLang).addStep('addE', args);
    return new this.graphTraversalClass(this.graph, new TraversalStrategies(this.traversalStrategies), gl);
  }

  /**
   * mergeV GraphTraversalSource step method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  mergeE(...args: any[]): GraphTraversal {
    if (args && args[0]) {
      if (args[0].get(direction.out) instanceof Vertex) {
        args[0].set(direction.out, args[0].get(direction.out).id);
      }
      if (args[0].get(direction.in) instanceof Vertex) {
        args[0].set(direction.in, args[0].get(direction.in).id);
      }
    }
    const gl = new GremlinLang(this.gremlinLang).addStep('mergeE', args);
    return new this.graphTraversalClass(this.graph, new TraversalStrategies(this.traversalStrategies), gl);
  }

  /**
   * addV GraphTraversalSource step method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  addV(...args: any[]): GraphTraversal {
    const gl = new GremlinLang(this.gremlinLang).addStep('addV', args);
    return new this.graphTraversalClass(this.graph, new TraversalStrategies(this.traversalStrategies), gl);
  }

  /**
   * mergeV GraphTraversalSource step method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  mergeV(...args: any[]): GraphTraversal {
    const gl = new GremlinLang(this.gremlinLang).addStep('mergeV', args);
    return new this.graphTraversalClass(this.graph, new TraversalStrategies(this.traversalStrategies), gl);
  }

  /**
   * inject GraphTraversalSource step method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  inject(...args: any[]): GraphTraversal {
    const gl = new GremlinLang(this.gremlinLang).addStep('inject', args);
    return new this.graphTraversalClass(this.graph, new TraversalStrategies(this.traversalStrategies), gl);
  }

  /**
   * io GraphTraversalSource step method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  io(...args: any[]): GraphTraversal {
    const gl = new GremlinLang(this.gremlinLang).addStep('io', args);
    return new this.graphTraversalClass(this.graph, new TraversalStrategies(this.traversalStrategies), gl);
  }

  /**
   * call GraphTraversalSource step method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  call(...args: any[]): GraphTraversal {
    const gl = new GremlinLang(this.gremlinLang).addStep('call', args);
    return new this.graphTraversalClass(this.graph, new TraversalStrategies(this.traversalStrategies), gl);
  }

  /**
   * union GraphTraversalSource method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  union(...args: any[]): GraphTraversal {
    const gl = new GremlinLang(this.gremlinLang).addStep('union', args);
    return new this.graphTraversalClass(this.graph, new TraversalStrategies(this.traversalStrategies), gl);
  }
}

/**
 * Represents a graph traversal.
 */
export class GraphTraversal extends Traversal {
  constructor(graph: Graph | null, traversalStrategies: TraversalStrategies | null, gremlinLang?: GremlinLang) {
    super(graph, traversalStrategies, gremlinLang);
  }

  /**
   * Copy a traversal so as to reset and re-use it.
   */
  clone() {
    return new GraphTraversal(this.graph, this.traversalStrategies, new GremlinLang(this.gremlinLang));
  }

  /**
   * Graph traversal V method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  V(...args: any[]): this {
    for (let i = 0; i < args.length; i++) {
      if (args[i] instanceof Vertex) {
        args[i] = args[i].id
      }
    }
    this.gremlinLang.addStep('V', args);
    return this;
  }

  /**
   * Graph traversal E method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  E(...args: any[]): this {
    this.gremlinLang.addStep('E', args);
    return this;
  }

  /**
   * Graph traversal addE method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  addE(...args: any[]): this {
    this.gremlinLang.addStep('addE', args);
    return this;
  }

  /**
   * Graph traversal addV method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  addV(...args: any[]): this {
    this.gremlinLang.addStep('addV', args);
    return this;
  }

  /**
   * Graph traversal aggregate method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  aggregate(...args: any[]): this {
    this.gremlinLang.addStep('aggregate', args);
    return this;
  }

  /**
   * Graph traversal all method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  all(...args: any[]): this {
    this.gremlinLang.addStep('all', args);
    return this;
  }

  /**
   * Graph traversal and method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  and(...args: any[]): this {
    this.gremlinLang.addStep('and', args);
    return this;
  }

  /**
   * Graph traversal any method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  any(...args: any[]): this {
    this.gremlinLang.addStep('any', args);
    return this;
  }

  /**
   * Graph traversal as method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  as(...args: any[]): this {
    this.gremlinLang.addStep('as', args);
    return this;
  }

  /**
   * Graph traversal asDate method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  asDate(...args: any[]): this {
    this.gremlinLang.addStep('asDate', args);
    return this;
  }

  /**
   * Graph traversal asNumber method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  asNumber(...args: any[]): this {
    this.gremlinLang.addStep('asNumber', args);
    return this;
  }

  /**
   * Graph traversal asString method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  asString(...args: any[]): this {
    this.gremlinLang.addStep('asString', args);
    return this;
  }

  /**
   * Graph traversal barrier method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  barrier(...args: any[]): this {
    this.gremlinLang.addStep('barrier', args);
    return this;
  }

  /**
   * Graph traversal both method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  both(...args: any[]): this {
    this.gremlinLang.addStep('both', args);
    return this;
  }

  /**
   * Graph traversal bothE method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  bothE(...args: any[]): this {
    this.gremlinLang.addStep('bothE', args);
    return this;
  }

  /**
   * Graph traversal bothV method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  bothV(...args: any[]): this {
    this.gremlinLang.addStep('bothV', args);
    return this;
  }

  /**
   * Graph traversal branch method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  branch(...args: any[]): this {
    this.gremlinLang.addStep('branch', args);
    return this;
  }

  /**
   * Graph traversal by method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  by(...args: any[]): this {
    this.gremlinLang.addStep('by', args);
    return this;
  }

  /**
   * Graph traversal call method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  call(...args: any[]): this {
    this.gremlinLang.addStep('call', args);
    return this;
  }
  /**
   * Graph traversal cap method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  cap(...args: any[]): this {
    this.gremlinLang.addStep('cap', args);
    return this;
  }

  /**
   * Graph traversal choose method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  choose(...args: any[]): this {
    this.gremlinLang.addStep('choose', args);
    return this;
  }

  /**
   * Graph traversal coalesce method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  coalesce(...args: any[]): this {
    this.gremlinLang.addStep('coalesce', args);
    return this;
  }

  /**
   * Graph traversal coin method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  coin(...args: any[]): this {
    this.gremlinLang.addStep('coin', args);
    return this;
  }

  /**
   * Graph traversal combine method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  combine(...args: any[]): this {
    this.gremlinLang.addStep('combine', args);
    return this;
  }

  /**
   * Graph traversal concat method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  concat(...args: any[]): this {
    this.gremlinLang.addStep('concat', args);
    return this;
  }

  /**
   * Graph traversal conjoin method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  conjoin(...args: any[]): this {
    this.gremlinLang.addStep('conjoin', args);
    return this;
  }

  /**
   * Graph traversal connectedComponent method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  connectedComponent(...args: any[]): this {
    this.gremlinLang.addStep('connectedComponent', args);
    return this;
  }

  /**
   * Graph traversal constant method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  constant(...args: any[]): this {
    this.gremlinLang.addStep('constant', args);
    return this;
  }

  /**
   * Graph traversal count method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  count(...args: any[]): this {
    this.gremlinLang.addStep('count', args);
    return this;
  }

  /**
   * Graph traversal cyclicPath method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  cyclicPath(...args: any[]): this {
    this.gremlinLang.addStep('cyclicPath', args);
    return this;
  }

  /**
   * Graph traversal dateAdd method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  dateAdd(...args: any[]): this {
    this.gremlinLang.addStep('dateAdd', args);
    return this;
  }

  /**
   * Graph traversal dateDiff method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  dateDiff(...args: any[]): this {
    this.gremlinLang.addStep('dateDiff', args);
    return this;
  }

  /**
   * Graph traversal dedup method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  dedup(...args: any[]): this {
    this.gremlinLang.addStep('dedup', args);
    return this;
  }

  /**
   * Graph traversal difference method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  difference(...args: any[]): this {
    this.gremlinLang.addStep('difference', args);
    return this;
  }

  /**
   * Graph traversal discard method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  discard(...args: any[]): this {
    this.gremlinLang.addStep('discard', args);
    return this;
  }

  /**
   * Graph traversal disjunct method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  disjunct(...args: any[]): this {
    this.gremlinLang.addStep('disjunct', args);
    return this;
  }

  /**
   * Graph traversal drop method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  drop(...args: any[]): this {
    this.gremlinLang.addStep('drop', args);
    return this;
  }

  /**
   * Graph traversal element method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  element(...args: any[]): this {
    this.gremlinLang.addStep('element', args);
    return this;
  }
  /**
   * Graph traversal elementMap method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  elementMap(...args: any[]): this {
    this.gremlinLang.addStep('elementMap', args);
    return this;
  }

  /**
   * Graph traversal emit method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  emit(...args: any[]): this {
    this.gremlinLang.addStep('emit', args);
    return this;
  }

  /**
   * Graph traversal fa method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  fail(...args: any[]): this {
    this.gremlinLang.addStep('fail', args);
    return this;
  }

  /**
   * Graph traversal filter method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  filter(...args: any[]): this {
    this.gremlinLang.addStep('filter', args);
    return this;
  }

  /**
   * Graph traversal flatMap method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  flatMap(...args: any[]): this {
    this.gremlinLang.addStep('flatMap', args);
    return this;
  }

  /**
   * Graph traversal fold method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  fold(...args: any[]): this {
    this.gremlinLang.addStep('fold', args);
    return this;
  }

  /**
   * Graph traversal format method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  format(...args: any[]): this {
    this.gremlinLang.addStep('format', args);
    return this;
  }

  /**
   * Graph traversal from method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  from_(...args: any[]): this {
    for (let i = 0; i < args.length; i++) {
      if (args[i] instanceof Vertex) {
        args[i] = args[i].id
      }
    }
    this.gremlinLang.addStep('from', args);
    return this;
  }

  /**
   * Graph traversal group method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  group(...args: any[]): this {
    this.gremlinLang.addStep('group', args);
    return this;
  }

  /**
   * Graph traversal groupCount method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  groupCount(...args: any[]): this {
    this.gremlinLang.addStep('groupCount', args);
    return this;
  }

  /**
   * Graph traversal has method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  has(...args: any[]): this {
    this.gremlinLang.addStep('has', args);
    return this;
  }

  /**
   * Graph traversal hasId method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  hasId(...args: any[]): this {
    this.gremlinLang.addStep('hasId', args);
    return this;
  }

  /**
   * Graph traversal hasKey method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  hasKey(...args: any[]): this {
    this.gremlinLang.addStep('hasKey', args);
    return this;
  }

  /**
   * Graph traversal hasLabel method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  hasLabel(...args: any[]): this {
    this.gremlinLang.addStep('hasLabel', args);
    return this;
  }

  /**
   * Graph traversal hasNot method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  hasNot(...args: any[]): this {
    this.gremlinLang.addStep('hasNot', args);
    return this;
  }

  /**
   * Graph traversal hasValue method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  hasValue(...args: any[]): this {
    this.gremlinLang.addStep('hasValue', args);
    return this;
  }

  /**
   * Graph traversal id method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  id(...args: any[]): this {
    this.gremlinLang.addStep('id', args);
    return this;
  }

  /**
   * Graph traversal identity method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  identity(...args: any[]): this {
    this.gremlinLang.addStep('identity', args);
    return this;
  }

  /**
   * Graph traversal in method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  in_(...args: any[]): this {
    this.gremlinLang.addStep('in', args);
    return this;
  }

  /**
   * Graph traversal inE method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  inE(...args: any[]): this {
    this.gremlinLang.addStep('inE', args);
    return this;
  }

  /**
   * Graph traversal inV method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  inV(...args: any[]): this {
    this.gremlinLang.addStep('inV', args);
    return this;
  }

  /**
   * Graph traversal index method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  index(...args: any[]): this {
    this.gremlinLang.addStep('index', args);
    return this;
  }

  /**
   * Graph traversal inject method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  inject(...args: any[]): this {
    this.gremlinLang.addStep('inject', args);
    return this;
  }

  /**
   * Graph traversal intersect method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  intersect(...args: any[]): this {
    this.gremlinLang.addStep('intersect', args);
    return this;
  }

  /**
   * Graph traversal is method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  is(...args: any[]): this {
    this.gremlinLang.addStep('is', args);
    return this;
  }

  /**
   * Graph traversal key method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  key(...args: any[]): this {
    this.gremlinLang.addStep('key', args);
    return this;
  }

  /**
   * Graph traversal label method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  label(...args: any[]): this {
    this.gremlinLang.addStep('label', args);
    return this;
  }

  /**
   * Graph traversal length method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  length(...args: any[]): this {
    this.gremlinLang.addStep('length', args);
    return this;
  }

  /**
   * Graph traversal limit method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  limit(...args: any[]): this {
    this.gremlinLang.addStep('limit', args);
    return this;
  }

  /**
   * Graph traversal local method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  local(...args: any[]): this {
    this.gremlinLang.addStep('local', args);
    return this;
  }

  /**
   * Graph traversal loops method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  loops(...args: any[]): this {
    this.gremlinLang.addStep('loops', args);
    return this;
  }

  /**
   * Graph traversal lTrim method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  lTrim(...args: any[]): this {
    this.gremlinLang.addStep('lTrim', args);
    return this;
  }

  /**
   * Graph traversal map method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  map(...args: any[]): this {
    this.gremlinLang.addStep('map', args);
    return this;
  }

  /**
   * Graph traversal match method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  match(...args: any[]): this {
    this.gremlinLang.addStep('match', args);
    return this;
  }

  /**
   * Graph traversal math method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  math(...args: any[]): this {
    this.gremlinLang.addStep('math', args);
    return this;
  }

  /**
   * Graph traversal max method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  max(...args: any[]): this {
    this.gremlinLang.addStep('max', args);
    return this;
  }

  /**
   * Graph traversal mean method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  mean(...args: any[]): this {
    this.gremlinLang.addStep('mean', args);
    return this;
  }

  /**
   * Graph traversal merge method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  merge(...args: any[]): this {
    this.gremlinLang.addStep('merge', args);
    return this;
  }

  /**
   * Graph traversal mergeE method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  mergeE(...args: any[]): this {
    if (args && args[0]) {
      if (args[0].get(direction.out) instanceof Vertex) {
        args[0].set(direction.out, args[0].get(direction.out).id);
      }
      if (args[0].get(direction.in) instanceof Vertex) {
        args[0].set(direction.in, args[0].get(direction.in).id);
      }
    }
    this.gremlinLang.addStep('mergeE', args);
    return this;
  }

  /**
   * Graph traversal mergeV method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  mergeV(...args: any[]): this {
    this.gremlinLang.addStep('mergeV', args);
    return this;
  }

  /**
   * Graph traversal min method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  min(...args: any[]): this {
    this.gremlinLang.addStep('min', args);
    return this;
  }

  /**
   * Graph traversal none method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  none(...args: any[]): this {
    this.gremlinLang.addStep('none', args);
    return this;
  }

  /**
   * Graph traversal not method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  not(...args: any[]): this {
    this.gremlinLang.addStep('not', args);
    return this;
  }

  /**
   * Graph traversal option method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  option(...args: any[]): this {
    this.gremlinLang.addStep('option', args);
    return this;
  }

  /**
   * Graph traversal optional method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  optional(...args: any[]): this {
    this.gremlinLang.addStep('optional', args);
    return this;
  }

  /**
   * Graph traversal or method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  or(...args: any[]): this {
    this.gremlinLang.addStep('or', args);
    return this;
  }

  /**
   * Graph traversal order method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  order(...args: any[]): this {
    this.gremlinLang.addStep('order', args);
    return this;
  }

  /**
   * Graph traversal otherV method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  otherV(...args: any[]): this {
    this.gremlinLang.addStep('otherV', args);
    return this;
  }

  /**
   * Graph traversal out method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  out(...args: any[]): this {
    this.gremlinLang.addStep('out', args);
    return this;
  }

  /**
   * Graph traversal outE method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  outE(...args: any[]): this {
    this.gremlinLang.addStep('outE', args);
    return this;
  }

  /**
   * Graph traversal outV method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  outV(...args: any[]): this {
    this.gremlinLang.addStep('outV', args);
    return this;
  }

  /**
   * Graph traversal pageRank method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  pageRank(...args: any[]): this {
    this.gremlinLang.addStep('pageRank', args);
    return this;
  }

  /**
   * Graph traversal path method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  path(...args: any[]): this {
    this.gremlinLang.addStep('path', args);
    return this;
  }

  /**
   * Graph traversal peerPressure method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  peerPressure(...args: any[]): this {
    this.gremlinLang.addStep('peerPressure', args);
    return this;
  }

  /**
   * Graph traversal product method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  product(...args: any[]): this {
    this.gremlinLang.addStep('product', args);
    return this;
  }

  /**
   * Graph traversal profile method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  profile(...args: any[]): this {
    this.gremlinLang.addStep('profile', args);
    return this;
  }

  /**
   * Graph traversal program method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  program(...args: any[]): this {
    this.gremlinLang.addStep('program', args);
    return this;
  }

  /**
   * Graph traversal project method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  project(...args: any[]): this {
    this.gremlinLang.addStep('project', args);
    return this;
  }

  /**
   * Graph traversal properties method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  properties(...args: any[]): this {
    this.gremlinLang.addStep('properties', args);
    return this;
  }

  /**
   * Graph traversal property method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  property(...args: any[]): this {
    this.gremlinLang.addStep('property', args);
    return this;
  }

  /**
   * Graph traversal propertyMap method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  propertyMap(...args: any[]): this {
    this.gremlinLang.addStep('propertyMap', args);
    return this;
  }

  /**
   * Graph traversal range method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  range(...args: any[]): this {
    this.gremlinLang.addStep('range', args);
    return this;
  }

  /**
   * Graph traversal read method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  read(...args: any[]): this {
    this.gremlinLang.addStep('read', args);
    return this;
  }

  /**
   * Graph traversal repeat method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  repeat(...args: any[]): this {
    this.gremlinLang.addStep('repeat', args);
    return this;
  }

  /**
   * Graph traversal replace method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  replace(...args: any[]): this {
    this.gremlinLang.addStep('replace', args);
    return this;
  }

  /**
   * Graph traversal reverse method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  reverse(...args: any[]): this {
    this.gremlinLang.addStep('reverse', args);
    return this;
  }

  /**
   * Graph traversal rTrim method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  rTrim(...args: any[]): this {
    this.gremlinLang.addStep('rTrim', args);
    return this;
  }

  /**
   * Graph traversal sack method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  sack(...args: any[]): this {
    this.gremlinLang.addStep('sack', args);
    return this;
  }

  /**
   * Graph traversal sample method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  sample(...args: any[]): this {
    this.gremlinLang.addStep('sample', args);
    return this;
  }

  /**
   * Graph traversal select method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  select(...args: any[]): this {
    this.gremlinLang.addStep('select', args);
    return this;
  }

  /**
   * Graph traversal shortestPath method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  shortestPath(...args: any[]): this {
    this.gremlinLang.addStep('shortestPath', args);
    return this;
  }

  /**
   * Graph traversal sideEffect method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  sideEffect(...args: any[]): this {
    this.gremlinLang.addStep('sideEffect', args);
    return this;
  }

  /**
   * Graph traversal simplePath method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  simplePath(...args: any[]): this {
    this.gremlinLang.addStep('simplePath', args);
    return this;
  }

  /**
   * Graph traversal skip method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  skip(...args: any[]): this {
    this.gremlinLang.addStep('skip', args);
    return this;
  }

  /**
   * Graph traversal split method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  split(...args: any[]): this {
    this.gremlinLang.addStep('split', args);
    return this;
  }

  /**
   * Graph traversal subgraph method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  subgraph(...args: any[]): this {
    this.gremlinLang.addStep('subgraph', args);
    return this;
  }

  /**
   * Graph traversal subgraph method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  substring(...args: any[]): this {
    this.gremlinLang.addStep('substring', args);
    return this;
  }

  /**
   * Graph traversal sum method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  sum(...args: any[]): this {
    this.gremlinLang.addStep('sum', args);
    return this;
  }

  /**
   * Graph traversal tail method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  tail(...args: any[]): this {
    this.gremlinLang.addStep('tail', args);
    return this;
  }

  /**
   * Graph traversal timeLimit method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  timeLimit(...args: any[]): this {
    this.gremlinLang.addStep('timeLimit', args);
    return this;
  }

  /**
   * Graph traversal times method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  times(...args: any[]): this {
    this.gremlinLang.addStep('times', args);
    return this;
  }

  /**
   * Graph traversal to method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  to(...args: any[]): this {
    for (let i = 0; i < args.length; i++) {
      if (args[i] instanceof Vertex) {
        args[i] = args[i].id
      }
    }
    this.gremlinLang.addStep('to', args);
    return this;
  }

  /**
   * Graph traversal toE method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  toE(...args: any[]): this {
    this.gremlinLang.addStep('toE', args);
    return this;
  }

  /**
   * Graph traversal toLower method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  toLower(...args: any[]): this {
    this.gremlinLang.addStep('toLower', args);
    return this;
  }

  /**
   * Graph traversal toUpper method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  toUpper(...args: any[]): this {
    this.gremlinLang.addStep('toUpper', args);
    return this;
  }

  /**
   * Graph traversal toV method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  toV(...args: any[]): this {
    this.gremlinLang.addStep('toV', args);
    return this;
  }

  /**
   * Graph traversal tree method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  tree(...args: any[]): this {
    this.gremlinLang.addStep('tree', args);
    return this;
  }

  /**
   * Graph traversal trim method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  trim(...args: any[]): this {
    this.gremlinLang.addStep('trim', args);
    return this;
  }

  /**
   * Graph traversal unfold method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  unfold(...args: any[]): this {
    this.gremlinLang.addStep('unfold', args);
    return this;
  }

  /**
   * Graph traversal union method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  union(...args: any[]): this {
    this.gremlinLang.addStep('union', args);
    return this;
  }

  /**
   * Graph traversal until method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  until(...args: any[]): this {
    this.gremlinLang.addStep('until', args);
    return this;
  }

  /**
   * Graph traversal value method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  value(...args: any[]): this {
    this.gremlinLang.addStep('value', args);
    return this;
  }

  /**
   * Graph traversal valueMap method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  valueMap(...args: any[]): this {
    this.gremlinLang.addStep('valueMap', args);
    return this;
  }

  /**
   * Graph traversal values method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  values(...args: any[]): this {
    this.gremlinLang.addStep('values', args);
    return this;
  }

  /**
   * Graph traversal where method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  where(...args: any[]): this {
    this.gremlinLang.addStep('where', args);
    return this;
  }

  /**
   * Graph traversal with method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  with_(...args: any[]): this {
    this.gremlinLang.addStep('with', args);
    return this;
  }

  /**
   * Graph traversal write method.
   * @param {...Object} args
   * @returns {GraphTraversal}
   */
  write(...args: any[]): this {
    this.gremlinLang.addStep('write', args);
    return this;
  }
}

export class CardinalityValue extends GremlinLang {
  /**
   * Creates a new instance of {@link CardinalityValue}.
   * @param {String} card
   * @param {Object} value
   */
  constructor(card: string | EnumValue, value: any) {
    super();
    this.addSource('CardinalityValueTraversal', [card, value]);
  }

  /**
   * Create a value with single cardinality.
   * @param {Array} value
   * @returns {CardinalityValue}
   */
  static single(value: any[]): CardinalityValue {
    return new CardinalityValue(cardinality.single, value);
  }

  /**
   * Create a value with list cardinality.
   * @param {Array} value
   * @returns {CardinalityValue}
   */
  static list(value: any[]): CardinalityValue {
    return new CardinalityValue(cardinality.list, value);
  }

  /**
   * Create a value with set cardinality.
   * @param {Array} value
   * @returns {CardinalityValue}
   */
  static set(value: any[]): CardinalityValue {
    return new CardinalityValue(cardinality.set, value);
  }
}

function callOnEmptyTraversal(fnName: string, args: any[]) {
  const g = new GraphTraversal(null, null);
  return g[fnName as keyof typeof g].apply(g, args);
}

/**
 * Contains the static method definitions
 */
export const statics = {
  E: (...args: any[]) => callOnEmptyTraversal('E', args),
  V: (...args: any[]) => callOnEmptyTraversal('V', args),
  addE: (...args: any[]) => callOnEmptyTraversal('addE', args),
  addV: (...args: any[]) => callOnEmptyTraversal('addV', args),
  aggregate: (...args: any[]) => callOnEmptyTraversal('aggregate', args),
  all: (...args: any[]) => callOnEmptyTraversal('all', args),
  and: (...args: any[]) => callOnEmptyTraversal('and', args),
  any: (...args: any[]) => callOnEmptyTraversal('any', args),
  as: (...args: any[]) => callOnEmptyTraversal('as', args),
  asDate: (...args: any[]) => callOnEmptyTraversal('asDate', args),
  asBool: (...args: any[]) => callOnEmptyTraversal('asBool', args),
  asNumber: (...args: any[]) => callOnEmptyTraversal('asNumber', args),
  asString: (...args: any[]) => callOnEmptyTraversal('asString', args),
  barrier: (...args: any[]) => callOnEmptyTraversal('barrier', args),
  both: (...args: any[]) => callOnEmptyTraversal('both', args),
  bothE: (...args: any[]) => callOnEmptyTraversal('bothE', args),
  bothV: (...args: any[]) => callOnEmptyTraversal('bothV', args),
  branch: (...args: any[]) => callOnEmptyTraversal('branch', args),
  call: (...args: any[]) => callOnEmptyTraversal('call', args),
  cap: (...args: any[]) => callOnEmptyTraversal('cap', args),
  choose: (...args: any[]) => callOnEmptyTraversal('choose', args),
  coalesce: (...args: any[]) => callOnEmptyTraversal('coalesce', args),
  coin: (...args: any[]) => callOnEmptyTraversal('coin', args),
  combine: (...args: any[]) => callOnEmptyTraversal('combine', args),
  concat: (...args: any[]) => callOnEmptyTraversal('concat', args),
  conjoin: (...args: any[]) => callOnEmptyTraversal('conjoin', args),
  constant: (...args: any[]) => callOnEmptyTraversal('constant', args),
  count: (...args: any[]) => callOnEmptyTraversal('count', args),
  cyclicPath: (...args: any[]) => callOnEmptyTraversal('cyclicPath', args),
  dateAdd: (...args: any[]) => callOnEmptyTraversal('dateAdd', args),
  dateDiff: (...args: any[]) => callOnEmptyTraversal('dateDiff', args),
  dedup: (...args: any[]) => callOnEmptyTraversal('dedup', args),
  difference: (...args: any[]) => callOnEmptyTraversal('difference', args),
  discard: (...args: any[]) => callOnEmptyTraversal('discard', args),
  disjunct: (...args: any[]) => callOnEmptyTraversal('disjunct', args),
  drop: (...args: any[]) => callOnEmptyTraversal('drop', args),
  element: (...args: any[]) => callOnEmptyTraversal('element', args),
  elementMap: (...args: any[]) => callOnEmptyTraversal('elementMap', args),
  emit: (...args: any[]) => callOnEmptyTraversal('emit', args),
  fail: (...args: any[]) => callOnEmptyTraversal('fail', args),
  filter: (...args: any[]) => callOnEmptyTraversal('filter', args),
  flatMap: (...args: any[]) => callOnEmptyTraversal('flatMap', args),
  fold: (...args: any[]) => callOnEmptyTraversal('fold', args),
  format: (...args: any[]) => callOnEmptyTraversal('format', args),
  group: (...args: any[]) => callOnEmptyTraversal('group', args),
  groupCount: (...args: any[]) => callOnEmptyTraversal('groupCount', args),
  has: (...args: any[]) => callOnEmptyTraversal('has', args),
  hasId: (...args: any[]) => callOnEmptyTraversal('hasId', args),
  hasKey: (...args: any[]) => callOnEmptyTraversal('hasKey', args),
  hasLabel: (...args: any[]) => callOnEmptyTraversal('hasLabel', args),
  hasNot: (...args: any[]) => callOnEmptyTraversal('hasNot', args),
  hasValue: (...args: any[]) => callOnEmptyTraversal('hasValue', args),
  id: (...args: any[]) => callOnEmptyTraversal('id', args),
  identity: (...args: any[]) => callOnEmptyTraversal('identity', args),
  in_: (...args: any[]) => callOnEmptyTraversal('in_', args),
  inE: (...args: any[]) => callOnEmptyTraversal('inE', args),
  inV: (...args: any[]) => callOnEmptyTraversal('inV', args),
  index: (...args: any[]) => callOnEmptyTraversal('index', args),
  inject: (...args: any[]) => callOnEmptyTraversal('inject', args),
  intersect: (...args: any[]) => callOnEmptyTraversal('intersect', args),
  is: (...args: any[]) => callOnEmptyTraversal('is', args),
  key: (...args: any[]) => callOnEmptyTraversal('key', args),
  label: (...args: any[]) => callOnEmptyTraversal('label', args),
  length: (...args: any[]) => callOnEmptyTraversal('length', args),
  limit: (...args: any[]) => callOnEmptyTraversal('limit', args),
  local: (...args: any[]) => callOnEmptyTraversal('local', args),
  loops: (...args: any[]) => callOnEmptyTraversal('loops', args),
  lTrim: (...args: any[]) => callOnEmptyTraversal('lTrim', args),
  map: (...args: any[]) => callOnEmptyTraversal('map', args),
  match: (...args: any[]) => callOnEmptyTraversal('match', args),
  math: (...args: any[]) => callOnEmptyTraversal('math', args),
  max: (...args: any[]) => callOnEmptyTraversal('max', args),
  mean: (...args: any[]) => callOnEmptyTraversal('mean', args),
  merge: (...args: any[]) => callOnEmptyTraversal('merge', args),
  mergeE: (...args: any[]) => callOnEmptyTraversal('mergeE', args),
  mergeV: (...args: any[]) => callOnEmptyTraversal('mergeV', args),
  min: (...args: any[]) => callOnEmptyTraversal('min', args),
  none: (...args: any[]) => callOnEmptyTraversal('none', args),
  not: (...args: any[]) => callOnEmptyTraversal('not', args),
  optional: (...args: any[]) => callOnEmptyTraversal('optional', args),
  or: (...args: any[]) => callOnEmptyTraversal('or', args),
  order: (...args: any[]) => callOnEmptyTraversal('order', args),
  otherV: (...args: any[]) => callOnEmptyTraversal('otherV', args),
  out: (...args: any[]) => callOnEmptyTraversal('out', args),
  outE: (...args: any[]) => callOnEmptyTraversal('outE', args),
  outV: (...args: any[]) => callOnEmptyTraversal('outV', args),
  path: (...args: any[]) => callOnEmptyTraversal('path', args),
  product: (...args: any[]) => callOnEmptyTraversal('product', args),
  project: (...args: any[]) => callOnEmptyTraversal('project', args),
  properties: (...args: any[]) => callOnEmptyTraversal('properties', args),
  property: (...args: any[]) => callOnEmptyTraversal('property', args),
  propertyMap: (...args: any[]) => callOnEmptyTraversal('propertyMap', args),
  range: (...args: any[]) => callOnEmptyTraversal('range', args),
  repeat: (...args: any[]) => callOnEmptyTraversal('repeat', args),
  replace: (...args: any[]) => callOnEmptyTraversal('replace', args),
  reverse: (...args: any[]) => callOnEmptyTraversal('reverse', args),
  rTrim: (...args: any[]) => callOnEmptyTraversal('rTrim', args),
  sack: (...args: any[]) => callOnEmptyTraversal('sack', args),
  sample: (...args: any[]) => callOnEmptyTraversal('sample', args),
  select: (...args: any[]) => callOnEmptyTraversal('select', args),
  sideEffect: (...args: any[]) => callOnEmptyTraversal('sideEffect', args),
  simplePath: (...args: any[]) => callOnEmptyTraversal('simplePath', args),
  skip: (...args: any[]) => callOnEmptyTraversal('skip', args),
  split: (...args: any[]) => callOnEmptyTraversal('split', args),
  subgraph: (...args: any[]) => callOnEmptyTraversal('subgraph', args),
  substring: (...args: any[]) => callOnEmptyTraversal('substring', args),
  sum: (...args: any[]) => callOnEmptyTraversal('sum', args),
  tail: (...args: any[]) => callOnEmptyTraversal('tail', args),
  timeLimit: (...args: any[]) => callOnEmptyTraversal('timeLimit', args),
  times: (...args: any[]) => callOnEmptyTraversal('times', args),
  to: (...args: any[]) => callOnEmptyTraversal('to', args),
  toE: (...args: any[]) => callOnEmptyTraversal('toE', args),
  toLower: (...args: any[]) => callOnEmptyTraversal('toLower', args),
  toUpper: (...args: any[]) => callOnEmptyTraversal('toUpper', args),
  toV: (...args: any[]) => callOnEmptyTraversal('toV', args),
  tree: (...args: any[]) => callOnEmptyTraversal('tree', args),
  trim: (...args: any[]) => callOnEmptyTraversal('trim', args),
  unfold: (...args: any[]) => callOnEmptyTraversal('unfold', args),
  union: (...args: any[]) => callOnEmptyTraversal('union', args),
  until: (...args: any[]) => callOnEmptyTraversal('until', args),
  value: (...args: any[]) => callOnEmptyTraversal('value', args),
  valueMap: (...args: any[]) => callOnEmptyTraversal('valueMap', args),
  values: (...args: any[]) => callOnEmptyTraversal('values', args),
  where: (...args: any[]) => callOnEmptyTraversal('where', args),
};
