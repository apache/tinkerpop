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

const utils = require('../utils');
const itemDone = Object.freeze({ value: null, done: true });

class Traversal {
  constructor(graph, traversalStrategies, bytecode) {
    this.graph = graph;
    this.traversalStrategies = traversalStrategies;
    this.bytecode = bytecode;
    /** @type {Array<Traverser>} */
    this.traversers = null;
    this.sideEffects = null;
    this._traversalStrategiesPromise = null;
    this._traversersIteratorIndex = 0;
  }

  /** @returns {Bytecode} */
  getBytecode() {
    return this.bytecode;
  }

  /**
   * Returns an Array containing the traverser objects.
   * @returns {Promise.<Array>}
   */
  toList() {
    return this._applyStrategies().then(() => {
      const result = [];
      let it;
      while ((it = this._getNext()) && !it.done) {
        result.push(it.value);
      }
      return result;
    });
  };

  /**
   * Iterates all Traverser instances in the traversal.
   * @returns {Promise}
   */
  iterate() {
    return this._applyStrategies().then(() => {
      let it;
      while ((it = this._getNext()) && !it.done) {
      }
    });
  }

  /**
   * Async iterator method implementation.
   * Returns a promise containing an iterator item.
   * @returns {Promise.<{value, done}>}
   */
  next() {
    return this._applyStrategies().then(() => this._getNext());
  }

  /**
   * Synchronous iterator of traversers including
   * @private
   */
  _getNext() {
    while (this.traversers && this._traversersIteratorIndex < this.traversers.length) {
      let traverser = this.traversers[this._traversersIteratorIndex];
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
    return this._traversalStrategiesPromise = this.traversalStrategies.applyStrategies(this);
  }

  /**
   * Returns the Bytecode JSON representation of the traversal
   * @returns {String}
   */
  toString() {
    return this.bytecode.toString();
  };
}

class P {
  /**
   * Represents an operation.
   * @constructor
   */
  constructor(operator, value, other) {
    this.operator = operator;
    this.value = value;
    this.other = other;
  }

  /**
   * Returns the string representation of the instance.
   * @returns {string}
   */
  toString() {
    if (this.other === undefined) {
      return this.operator + '(' + this.value + ')';
    }
    return this.operator + '(' + this.value + ', ' + this.other + ')';
  }

  and(arg) {
    return new P('and', this, arg);
  }

  or(arg) {
    return new P('or', this, arg);
  }

  /** @param {...Object} args */
  static between(...args) {
    return createP('between', args);
  }

  /** @param {...Object} args */
  static eq(...args) {
    return createP('eq', args);
  }

  /** @param {...Object} args */
  static gt(...args) {
    return createP('gt', args);
  }

  /** @param {...Object} args */
  static gte(...args) {
    return createP('gte', args);
  }

  /** @param {...Object} args */
  static inside(...args) {
    return createP('inside', args);
  }

  /** @param {...Object} args */
  static lt(...args) {
    return createP('lt', args);
  }

  /** @param {...Object} args */
  static lte(...args) {
    return createP('lte', args);
  }

  /** @param {...Object} args */
  static neq(...args) {
    return createP('neq', args);
  }

  /** @param {...Object} args */
  static not(...args) {
    return createP('not', args);
  }

  /** @param {...Object} args */
  static outside(...args) {
    return createP('outside', args);
  }

  /** @param {...Object} args */
  static test(...args) {
    return createP('test', args);
  }

  /** @param {...Object} args */
  static within(...args) {
    return createP('within', args);
  }

  /** @param {...Object} args */
  static without(...args) {
    return createP('without', args);
  }

}

function createP(operator, args) {
  args.unshift(null, operator);
  return new (Function.prototype.bind.apply(P, args));
}

class Traverser {
  constructor(object, bulk) {
    this.object = object;
    this.bulk = bulk || 1;
  }
}

class TraversalSideEffects {

}

function toEnum(typeName, keys) {
  const result = {};
  keys.split(' ').forEach(k => {
    let jsKey = k;
    if (jsKey === jsKey.toUpperCase()) {
      jsKey = jsKey.toLowerCase();
    }
    result[jsKey] = new EnumValue(typeName, k);
  });
  return result;
}

class EnumValue {
  constructor(typeName, elementName) {
    this.typeName = typeName;
    this.elementName = elementName;
  }

  toString() {
    return this.elementName;
  }
}

module.exports = {
  EnumValue,
  P,
  Traversal,
  TraversalSideEffects,
  Traverser,
  barrier: toEnum('Barrier', 'normSack'),
  cardinality: toEnum('Cardinality', 'list set single'),
  column: toEnum('Column', 'keys values'),
  direction: toEnum('Direction', 'BOTH IN OUT'),
  graphSONVersion: toEnum('GraphSONVersion', 'V1_0 V2_0'),
  gryoVersion: toEnum('GryoVersion', 'V1_0'),
  operator: toEnum('Operator', 'addAll and assign div max min minus mult or sum sumLong'),
  order: toEnum('Order', 'decr incr keyDecr keyIncr shuffle valueDecr valueIncr'),
  pick: toEnum('Pick', 'any none'),
  pop: toEnum('Pop', 'all first last'),
  scope: toEnum('Scope', 'global local'),
  t: toEnum('T', 'id key label value')
};