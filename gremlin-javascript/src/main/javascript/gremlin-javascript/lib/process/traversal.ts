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

import { Graph } from '../structure/graph.js';
import Bytecode from './bytecode.js';
import { TraversalStrategies } from './traversal-strategy.js';

const itemDone = Object.freeze({ value: null, done: true });
const asyncIteratorSymbol = Symbol.asyncIterator || Symbol('@@asyncIterator');

export class Traversal {
  traversers: Traverser<any>[] | null = null;
  sideEffects?: any = null;
  private _traversalStrategiesPromise: Promise<void> | null = null;
  private _traversersIteratorIndex = 0;

  constructor(
    public graph: Graph | null,
    public traversalStrategies: TraversalStrategies | null,
    public bytecode: Bytecode,
  ) {}

  /**
   * Async iterable method implementation.
   */
  [asyncIteratorSymbol]() {
    return this;
  }

  /** @returns {Bytecode} */
  getBytecode() {
    return this.bytecode;
  }

  /**
   * Returns an Array containing the traverser objects.
   * @returns {Promise.<Array>}
   */
  toList<T>(): Promise<T[]> {
    return this._applyStrategies().then(() => {
      const result: T[] = [];
      let it;
      while ((it = this._getNext()) && !it.done) {
        result.push(it.value as T);
      }
      return result;
    });
  }

  /**
   * Determines if there are any more items to iterate from the traversal.
   * @returns {Promise.<boolean>}
   */
  hasNext() {
    return this._applyStrategies().then(
      () =>
        this.traversers &&
        this.traversers.length > 0 &&
        this._traversersIteratorIndex < this.traversers.length &&
        this.traversers[this._traversersIteratorIndex].bulk > 0,
    );
  }

  /**
   * Iterates all Traverser instances in the traversal.
   * @returns {Promise}
   */
  iterate() {
    this.bytecode.addStep('discard');
    return this._applyStrategies().then(() => {
      let it;
      while ((it = this._getNext()) && !it.done) {
        //
      }
    });
  }

  /**
   * Async iterator method implementation.
   * Returns a promise containing an iterator item.
   * @returns {Promise.<{value, done}>}
   */
  next<T>(): Promise<IteratorResult<T, null>> {
    return this._applyStrategies().then(() => this._getNext<T>());
  }

  /**
   * Synchronous iterator of traversers including
   * @private
   */
  _getNext<T>(): IteratorResult<T, null> {
    while (this.traversers && this._traversersIteratorIndex < this.traversers.length) {
      const traverser = this.traversers[this._traversersIteratorIndex];
      if (traverser.bulk > 0) {
        traverser.bulk--;
        return { value: traverser.object, done: false };
      }
      this._traversersIteratorIndex++;
    }
    return itemDone;
  }

  _applyStrategies() {
    if (this._traversalStrategiesPromise) {
      // Apply strategies only once
      return this._traversalStrategiesPromise;
    }
    return (this._traversalStrategiesPromise = this.traversalStrategies?.applyStrategies(this) ?? Promise.resolve());
  }

  /**
   * Returns step instructions during JSON serialization
   * @returns {Array}
   */
  toJSON() {
    return this.bytecode.stepInstructions;
  }

  /**
   * Returns the Bytecode JSON representation of the traversal
   * @returns {String}
   */
  toString() {
    return this.bytecode.toString();
  }
}

export class IO {
  static get graphml() {
    return 'graphml';
  }

  static get graphson() {
    return 'graphson';
  }

  static get gryo() {
    return 'gryo';
  }

  static get reader() {
    return '~tinkerpop.io.reader';
  }

  static get registry() {
    return '~tinkerpop.io.registry';
  }

  static get writer() {
    return '~tinkerpop.io.writer';
  }
}

// eslint-disable-next-line no-unused-vars
class ConnectedComponent {
  static get component() {
    return 'gremlin.connectedComponentVertexProgram.component';
  }

  static get edges() {
    return '~tinkerpop.connectedComponent.edges';
  }

  static get propertyName() {
    return '~tinkerpop.connectedComponent.propertyName';
  }
}

// eslint-disable-next-line no-unused-vars
class ShortestPath {
  static get distance() {
    return '~tinkerpop.shortestPath.distance';
  }

  static get edges() {
    return '~tinkerpop.shortestPath.edges';
  }

  static get includeEdges() {
    return '~tinkerpop.shortestPath.includeEdges';
  }

  static get maxDistance() {
    return '~tinkerpop.shortestPath.maxDistance';
  }

  static get target() {
    return '~tinkerpop.shortestPath.target';
  }
}

// eslint-disable-next-line no-unused-vars
class PageRank {
  static get edges() {
    return '~tinkerpop.pageRank.edges';
  }

  static get propertyName() {
    return '~tinkerpop.pageRank.propertyName';
  }

  static get times() {
    return '~tinkerpop.pageRank.times';
  }
}

// eslint-disable-next-line no-unused-vars
class PeerPressure {
  static get edges() {
    return '~tinkerpop.peerPressure.edges';
  }

  static get propertyName() {
    return '~tinkerpop.peerPressure.propertyName';
  }

  static get times() {
    return '~tinkerpop.peerPressure.times';
  }
}

export class P<T1 = any, T2 = any> {
  /**
   * Represents an operation.
   * @constructor
   */
  constructor(
    public operator: string,
    public value: T1,
    public other: T2,
  ) {}

  /**
   * Returns the string representation of the instance.
   * @returns {string}
   */
  toString() {
    function formatValue(value: T1 | T2): any {
      if (Array.isArray(value)) {
        const acc = [];
        for (const item of value) {
          acc.push(formatValue(item));
        }
        return acc;
      }
      if (value && typeof value === 'string') {
        return `'${value}'`;
      }
      return value;
    }

    if (this.other === undefined || this.other === null) {
      return this.operator + '(' + formatValue(this.value) + ')';
    }
    return this.operator + '(' + formatValue(this.value) + ', ' + formatValue(this.other) + ')';
  }

  and(arg?: any) {
    return new P('and', this, arg);
  }

  or(arg?: any) {
    return new P('or', this, arg);
  }

  static within(...args: any[]) {
    if (args.length === 1 && Array.isArray(args[0])) {
      return new P('within', args[0], null);
    }
    return new P('within', args, null);
  }

  static without(...args: any[]) {
    if (args.length === 1 && Array.isArray(args[0])) {
      return new P('without', args[0], null);
    }
    return new P('without', args, null);
  }

  /** @param {...Object} args */
  static between(...args: any[]) {
    return createP('between', args);
  }

  /** @param {...Object} args */
  static eq(...args: any[]) {
    return createP('eq', args);
  }

  /** @param {...Object} args */
  static gt(...args: any[]) {
    return createP('gt', args);
  }

  /** @param {...Object} args */
  static gte(...args: any[]) {
    return createP('gte', args);
  }

  /** @param {...Object} args */
  static inside(...args: any[]) {
    return createP('inside', args);
  }

  /** @param {...Object} args */
  static lt(...args: any[]) {
    return createP('lt', args);
  }

  /** @param {...Object} args */
  static lte(...args: any[]) {
    return createP('lte', args);
  }

  /** @param {...Object} args */
  static neq(...args: any[]) {
    return createP('neq', args);
  }

  /** @param {...Object} args */
  static not(...args: any[]) {
    return createP('not', args);
  }

  /** @param {...Object} args */
  static outside(...args: any[]) {
    return createP('outside', args);
  }

  /** @param {...Object} args */
  static test(...args: any[]) {
    return createP('test', args);
  }
}

function createP(operator: string, args: any) {
  args.unshift(null, operator);
  return new (Function.prototype.bind.apply(P, args))();
}

export class TextP<T1 = any, T2 = any> {
  /**
   * Represents an operation.
   * @constructor
   */
  constructor(
    public operator: string,
    public value: T1,
    public other: T2,
  ) {}

  /**
   * Returns the string representation of the instance.
   * @returns {string}
   */
  toString() {
    function formatValue(value: any) {
      if (value && typeof value === 'string') {
        return `'${value}'`;
      }
      return value;
    }

    if (this.other === undefined) {
      return this.operator + '(' + formatValue(this.value) + ')';
    }
    return this.operator + '(' + formatValue(this.value) + ', ' + formatValue(this.other) + ')';
  }

  and(arg: any) {
    return new P('and', this, arg);
  }

  or(arg: any) {
    return new P('or', this, arg);
  }

  /** @param {...Object} args */
  static containing(...args: any[]) {
    return createTextP('containing', args);
  }

  /** @param {...Object} args */
  static endingWith(...args: any[]) {
    return createTextP('endingWith', args);
  }

  /** @param {...Object} args */
  static notContaining(...args: any[]) {
    return createTextP('notContaining', args);
  }

  /** @param {...Object} args */
  static notEndingWith(...args: any[]) {
    return createTextP('notEndingWith', args);
  }

  /** @param {...Object} args */
  static notStartingWith(...args: any[]) {
    return createTextP('notStartingWith', args);
  }

  /** @param {...Object} args */
  static startingWith(...args: any[]) {
    return createTextP('startingWith', args);
  }

  /** @param {...Object} args */
  static regex(...args: any[]) {
    return createTextP('regex', args);
  }

  /** @param {...Object} args */
  static notRegex(...args: any[]) {
    return createTextP('notRegex', args);
  }
}

function createTextP(operator: string, args: any) {
  args.unshift(null, operator);
  return new (Function.prototype.bind.apply(TextP, args))();
}

export class Traverser<T = any> {
  constructor(
    public object: T,
    public bulk: number,
  ) {
    this.bulk = bulk || 1;
  }
}

export class TraversalSideEffects {}

export const withOptions = {
  tokens: '~tinkerpop.valueMap.tokens',
  none: 0,
  ids: 1,
  labels: 2,
  keys: 4,
  values: 8,
  all: 15,
  indexer: '~tinkerpop.index.indexer',
  list: 0,
  map: 1,
};

function toEnum(typeName: string, keys: string) {
  const result: Record<string, EnumValue> = {};
  keys.split(' ').forEach((k) => {
    let jsKey = k;
    if (jsKey === jsKey.toUpperCase()) {
      jsKey = jsKey.toLowerCase();
    }
    result[jsKey] = new EnumValue(typeName, k);
  });
  return result;
}

const directionAlias = {
  from_: 'out',
  to: 'in',
} as const;

// for direction enums, maps the same EnumValue object to the enum aliases after creating them
function toDirectionEnum(typeName: string, keys: string) {
  const result = toEnum(typeName, keys);
  Object.keys(directionAlias).forEach((k) => {
    result[k] = result[directionAlias[k as keyof typeof directionAlias]];
  });
  return result;
}

export class EnumValue {
  constructor(
    public typeName: string,
    public elementName: string,
  ) {}

  toString() {
    return this.elementName;
  }
}

export const barrier = toEnum('Barrier', 'normSack');
export const cardinality = toEnum('Cardinality', 'list set single');
export const column = toEnum('Column', 'keys values');
export const direction = toDirectionEnum('Direction', 'BOTH IN OUT from_ to');
export const dt = toEnum('DT', 'second minute hour day');
export const graphSONVersion = toEnum('GraphSONVersion', 'V1_0 V2_0 V3_0');
export const gryoVersion = toEnum('GryoVersion', 'V1_0 V3_0');
export const merge = toEnum('Merge', 'onCreate onMatch outV inV');
export const operator = toEnum('Operator', 'addAll and assign div max min minus mult or sum sumLong');
export const order = toEnum('Order', 'asc desc shuffle');
export const pick = toEnum('Pick', 'any none');
export const pop = toEnum('Pop', 'all first last mixed');
export const scope = toEnum('Scope', 'global local');
export const t = toEnum('T', 'id key label value');
