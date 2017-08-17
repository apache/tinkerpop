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

var utils = require('../utils');
var parseArgs = utils.parseArgs;
var itemDone = Object.freeze({ value: null, done: true });
var emptyArray = Object.freeze([]);

function Traversal(graph, traversalStrategies, bytecode) {
  this.graph = graph;
  this.traversalStrategies = traversalStrategies;
  this.bytecode = bytecode;
  this.traversers = null;
  this.sideEffects = null;
  this._traversalStrategiesPromise = null;
  this._traversersIteratorIndex = 0;
}

/** @returns {Bytecode} */
Traversal.prototype.getBytecode = function () {
  return this.bytecode;
};

/**
 * Returns an Array containing the traverser objects.
 * @returns {Promise.<Array>}
 */
Traversal.prototype.toList = function () {
  var self = this;
  return this._applyStrategies().then(function () {
    if (!self.traversers || self._traversersIteratorIndex === self.traversers.length) {
      return emptyArray;
    }
    var arr = new Array(self.traversers.length - self._traversersIteratorIndex);
    for (var i = self._traversersIteratorIndex; i < self.traversers.length; i++) {
      arr[i] = self.traversers[i].object;
    }
    self._traversersIteratorIndex = self.traversers.length;
    return arr;
  });
};

/**
 * Async iterator method implementation.
 * Returns a promise containing an iterator item.
 * @returns {Promise.<{value, done}>}
 */
Traversal.prototype.next = function () {
  var self = this;
  return this._applyStrategies().then(function () {
    if (!self.traversers || self._traversersIteratorIndex === self.traversers.length) {
      return itemDone;
    }
    return { value: self.traversers[self._traversersIteratorIndex++].object, done: false };
  });
};

Traversal.prototype._applyStrategies = function () {
  if (this._traversalStrategiesPromise) {
    // Apply strategies only once
    return this._traversalStrategiesPromise;
  }
  return this._traversalStrategiesPromise = this.traversalStrategies.applyStrategies(this);
};

/**
 * Returns the Bytecode JSON representation of the traversal
 * @returns {String}
 */
Traversal.prototype.toString = function () {
  return this.bytecode.toString();
};

/**
 * Represents an operation.
 * @constructor
 */
function P(operator, value, other) {
  this.operator = operator;
  this.value = value;
  this.other = other;
}

/**
 * Returns the string representation of the instance.
 * @returns {string}
 */
P.prototype.toString = function () {
  if (this.other === undefined) {
    return this.operator + '(' + this.value + ')';
  }
  return this.operator + '(' + this.value + ', ' + this.other + ')';
};

function createP(operator, args) {
  args.unshift(null, operator);
  return new (Function.prototype.bind.apply(P, args));
}

/** @param {...Object} args */
P.between = function (args) {
  return createP('between', parseArgs.apply(null, arguments));
};

/** @param {...Object} args */
P.eq = function (args) {
  return createP('eq', parseArgs.apply(null, arguments));
};

/** @param {...Object} args */
P.gt = function (args) {
  return createP('gt', parseArgs.apply(null, arguments));
};

/** @param {...Object} args */
P.gte = function (args) {
  return createP('gte', parseArgs.apply(null, arguments));
};

/** @param {...Object} args */
P.inside = function (args) {
  return createP('inside', parseArgs.apply(null, arguments));
};

/** @param {...Object} args */
P.lt = function (args) {
  return createP('lt', parseArgs.apply(null, arguments));
};

/** @param {...Object} args */
P.lte = function (args) {
  return createP('lte', parseArgs.apply(null, arguments));
};

/** @param {...Object} args */
P.neq = function (args) {
  return createP('neq', parseArgs.apply(null, arguments));
};

/** @param {...Object} args */
P.not = function (args) {
  return createP('not', parseArgs.apply(null, arguments));
};

/** @param {...Object} args */
P.outside = function (args) {
  return createP('outside', parseArgs.apply(null, arguments));
};

/** @param {...Object} args */
P.test = function (args) {
  return createP('test', parseArgs.apply(null, arguments));
};

/** @param {...Object} args */
P.within = function (args) {
  return createP('within', parseArgs.apply(null, arguments));
};

/** @param {...Object} args */
P.without = function (args) {
  return createP('without', parseArgs.apply(null, arguments));
};

P.prototype.and = function (arg) {
  return new P('and', this, arg);
};

P.prototype.or = function (arg) {
  return new P('or', this, arg);
};

function Traverser(object, bulk) {
  this.object = object;
  this.bulk = bulk == undefined ? 1 : bulk;
}

function TraversalSideEffects() {

}

function toEnum(typeName, keys) {
  var result = {};
  keys.split(' ').forEach(function (k) {
    var jsKey = k;
    if (jsKey === jsKey.toUpperCase()) {
      jsKey = jsKey.toLowerCase();
    }
    result[jsKey] = new EnumValue(typeName, k);
  });
  return result;
}

function EnumValue(typeName, elementName) {
  this.typeName = typeName;
  this.elementName = elementName;
}

module.exports = {
  EnumValue: EnumValue,
  P: P,
  Traversal: Traversal,
  TraversalSideEffects: TraversalSideEffects,
  Traverser: Traverser,
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