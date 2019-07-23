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
const asyncIteratorSymbol = Symbol.asyncIterator || Symbol('@@asyncIterator');

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
   * Determines if there are any more items to iterate from the traversal.
   * @returns {Promise.<boolean>}
   */
   hasNext() {
     return this._applyStrategies().then(() => {
       return this.traversers && this.traversers.length > 0 &&
              this._traversersIteratorIndex < this.traversers.length &&
              this.traversers[this._traversersIteratorIndex].bulk > 0;
     });
   }

  /**
   * Iterates all Traverser instances in the traversal.
   * @returns {Promise}
   */
  iterate() {
    this.bytecode.addStep('none');
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


class IO {

 static get graphml() {
   return "graphml"
 }

 static get graphson() {
   return "graphson"
 }

 static get gryo() {
   return "gryo"
 }

 static get reader() {
   return "~tinkerpop.io.reader"
 }

 static get registry() {
   return "~tinkerpop.io.registry"
 }

 static get writer() {
   return "~tinkerpop.io.writer"
 }
}

class ConnectedComponent {

 static get component() {
   return "gremlin.connectedComponentVertexProgram.component"
 }

 static get edges() {
   return "~tinkerpop.connectedComponent.edges"
 }

 static get propertyName() {
   return "~tinkerpop.connectedComponent.propertyName"
 }
}

class ShortestPath {

 static get distance() {
   return "~tinkerpop.shortestPath.distance"
 }

 static get edges() {
   return "~tinkerpop.shortestPath.edges"
 }

 static get includeEdges() {
   return "~tinkerpop.shortestPath.includeEdges"
 }

 static get maxDistance() {
   return "~tinkerpop.shortestPath.maxDistance"
 }

 static get target() {
   return "~tinkerpop.shortestPath.target"
 }
}

class PageRank {

 static get edges() {
   return "~tinkerpop.pageRank.edges"
 }

 static get propertyName() {
   return "~tinkerpop.pageRank.propertyName"
 }

 static get times() {
   return "~tinkerpop.pageRank.times"
 }
}

class PeerPressure {

 static get edges() {
   return "~tinkerpop.peerPressure.edges"
 }

 static get propertyName() {
   return "~tinkerpop.peerPressure.propertyName"
 }

 static get times() {
   return "~tinkerpop.peerPressure.times"
 }
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

  static within(...args) {
    if (args.length === 1 && Array.isArray(args[0])) {
      return new P("within", args[0], null);
    } else {
      return new P("within", args, null);
    }
  }

  static without(...args) {
    if (args.length === 1 && Array.isArray(args[0])) {
      return new P("without", args[0], null);
    } else {
      return new P("without", args, null);
    }
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

}

function createP(operator, args) {
  args.unshift(null, operator);
  return new (Function.prototype.bind.apply(P, args));
}

class TextP {
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
  static containing(...args) {
    return createTextP('containing', args);
  }

  /** @param {...Object} args */
  static endingWith(...args) {
    return createTextP('endingWith', args);
  }

  /** @param {...Object} args */
  static notContaining(...args) {
    return createTextP('notContaining', args);
  }

  /** @param {...Object} args */
  static notEndingWith(...args) {
    return createTextP('notEndingWith', args);
  }

  /** @param {...Object} args */
  static notStartingWith(...args) {
    return createTextP('notStartingWith', args);
  }

  /** @param {...Object} args */
  static startingWith(...args) {
    return createTextP('startingWith', args);
  }

}

function createTextP(operator, args) {
  args.unshift(null, operator);
  return new (Function.prototype.bind.apply(TextP, args));
}

class Traverser {
  constructor(object, bulk) {
    this.object = object;
    this.bulk = bulk || 1;
  }
}

class TraversalSideEffects {

}

const withOptions = {
  tokens: "~tinkerpop.valueMap.tokens",
  none: 0,
  ids: 1,
  labels: 2,
  keys: 4,
  values: 8,
  all: 15,
  indexer: "~tinkerpop.index.indexer",
  list: 0,
  map: 1
};

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
  TextP,
  withOptions,
  IO,
  Traversal,
  TraversalSideEffects,
  Traverser,
  barrier: toEnum('Barrier', 'normSack'),
  cardinality: toEnum('Cardinality', 'list set single'),
  column: toEnum('Column', 'keys values'),
  direction: toEnum('Direction', 'BOTH IN OUT'),
  graphSONVersion: toEnum('GraphSONVersion', 'V1_0 V2_0 V3_0'),
  gryoVersion: toEnum('GryoVersion', 'V1_0 V3_0'),
  operator: toEnum('Operator', 'addAll and assign div max min minus mult or sum sumLong'),
  order: toEnum('Order', 'asc decr desc incr shuffle'),
  pick: toEnum('Pick', 'any none'),
  pop: toEnum('Pop', 'all first last mixed'),
  scope: toEnum('Scope', 'global local'),
  t: toEnum('T', 'id key label value')
};
