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
(function defineTraversalModule() {
  "use strict";

  function Traversal(graph, traversalStrategies, bytecode) {
    this.graph = graph;
    this.traversalStrategies = traversalStrategies;
    this.bytecode = bytecode;
    this.traversers = null;
    this.sideEffects = null;
  }

  /** @returns {Bytecode} */
  Traversal.prototype.getBytecode = function () {
    return this.bytecode;
  };

  /** @param {Function} callback */
  Traversal.prototype.list = function (callback) {
    var self = this;
    this.traversalStrategies.applyStrategies(this, function (err) {
      if (err) {
        return callback(err);
      }
      callback(err, self.traversers);
    });
  };

  /** @param {Function} callback */
  Traversal.prototype.one = function (callback) {
    this.list(function (err, result) {
      callback(err, result && result.length > 0 ? result[0] : null);
    });
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

  /**
   * Creates a new instance of TraversalStrategies.
   * @param {TraversalStrategies} [traversalStrategies]
   * @constructor
   */
  function TraversalStrategies(traversalStrategies) {
    /** @type {Array<TraversalStrategy>} */
    this.strategies = traversalStrategies ? traversalStrategies.strategies : [];
  }

  /** @param {TraversalStrategy} strategy */
  TraversalStrategies.prototype.addStrategy = function (strategy) {
    this.strategies.push(strategy);
  };

  /**
   * @param {Traversal} traversal
   * @param {Function} callback
   */
  TraversalStrategies.prototype.applyStrategies = function (traversal, callback) {
    eachSeries(this.strategies, function eachStrategy(s, next) {
      s.apply(traversal, next);
    }, callback);
  };

  /**
   * @abstract
   * @constructor
   */
  function TraversalStrategy() {

  }

  /**
   * @abstract
   * @param {Traversal} traversal
   * @param {Function} callback
   */
  TraversalStrategy.prototype.apply = function (traversal, callback) {

  };

  /**
   * Creates a new instance of Bytecode
   * @param {Bytecode} [toClone]
   * @constructor
   */
  function Bytecode(toClone) {
    this._bindings = {};
    if (!toClone) {
      this.sourceInstructions = [];
      this.stepInstructions = [];
    }
    else {
      this.sourceInstructions = toClone.sourceInstructions.slice(0);
      this.stepInstructions = toClone.sourceInstructions.slice(0);
    }
  }

  /**
   * Adds a new source instructions
   * @param {String} name
   * @param {Array} values
   * @returns {Bytecode}
   */
  Bytecode.prototype.addSource = function (name, values) {
    if (name === undefined) {
      throw new Error('Name is not defined');
    }
    var instruction = new Array(values.length + 1);
    instruction[0] = name;
    for (var i = 0; i < values.length; ++i) {
      instruction[i + 1] = this._convertToArgument(values[i]);
    }
    this.sourceInstructions.push(this._generateInstruction(name, values));
    return this;
  };

  /**
   * Adds a new step instructions
   * @param {String} name
   * @param {Array} values
   * @returns {Bytecode}
   */
  Bytecode.prototype.addStep = function (name, values) {
    if (name === undefined) {
      throw new Error('Name is not defined');
    }
    this.stepInstructions.push(this._generateInstruction(name, values));
    return this;
  };

  Bytecode.prototype._generateInstruction = function (name, values) {
    var instruction = new Array(values.length + 1);
    instruction[0] = name;
    for (var i = 0; i < values.length; ++i) {
      instruction[i + 1] = this._convertToArgument(values[i]);
    }
    return instruction;
  };

  /**
   * Returns the JSON representation of the source and step instructions
   * @returns {String}
   */
  Bytecode.prototype.toString = function () {
    return (
      (this.sourceInstructions.length > 0 ? JSON.stringify(this.sourceInstructions) : '') +
      (this.stepInstructions.length   > 0 ? JSON.stringify(this.stepInstructions) : '')
    );
  };

  Bytecode.prototype._convertToArgument = function (value) {
    return value;
  };

  function toEnum(typeName, keys) {
    var result = {};
    keys.split(' ').forEach(function (k) {
      if (k === k.toUpperCase()) {
        k = k.toLowerCase();
      }
      result[k] = new EnumValue(typeName, k);
    });
    return result;
  }

  function EnumValue(typeName, elementName) {
    this.typeName = typeName;
    this.elementName = elementName;
  }

  // Utility functions
  /** @returns {Array} */
  function parseArgs() {
    return (arguments.length === 1 ? [ arguments[0] ] : Array.apply(null, arguments));
  }

  /**
   * @param {Array} arr
   * @param {Function} fn
   * @param {Function} [callback]
   */
  function eachSeries(arr, fn, callback) {
    if (!Array.isArray(arr)) {
      throw new TypeError('First parameter is not an Array');
    }
    callback = callback || noop;
    var length = arr.length;
    if (length === 0) {
      return callback();
    }
    var sync;
    var index = 1;
    fn(arr[0], next);
    if (sync === undefined) {
      sync = false;
    }

    function next(err) {
      if (err) {
        return callback(err);
      }
      if (index >= length) {
        return callback();
      }
      if (sync === undefined) {
        sync = true;
      }
      if (sync) {
        return process.nextTick(function () {
          fn(arr[index++], next);
        });
      }
      fn(arr[index++], next);
    }
  }

  function inherits(ctor, superCtor) {
    ctor.super_ = superCtor;
    ctor.prototype = Object.create(superCtor.prototype, {
      constructor: {
        value: ctor,
        enumerable: false,
        writable: true,
        configurable: true
      }
    });
  }

  var toExport = {
    Bytecode: Bytecode,
    EnumValue: EnumValue,
    inherits: inherits,
    P: P,
    parseArgs: parseArgs,
    Traversal: Traversal,
    TraversalSideEffects: TraversalSideEffects,
    TraversalStrategies: TraversalStrategies,
    TraversalStrategy: TraversalStrategy,
    Traverser: Traverser,
    barrier: toEnum('Barrier', 'normSack'),
    cardinality: toEnum('Cardinality', 'list set single'),
    column: toEnum('Column', 'keys values'),
    direction: toEnum('Direction', 'BOTH IN OUT'),
    operator: toEnum('Operator', 'addAll and assign div max min minus mult or sum sumLong'),
    order: toEnum('Order', 'decr incr keyDecr keyIncr shuffle valueDecr valueIncr'),
    pick: toEnum('Pick', 'any none'),
    pop: toEnum('Pop', 'all first last'),
    scope: toEnum('Scope', 'global local'),
    t: toEnum('T', 'id key label value')
  };
  if (typeof module !== 'undefined') {
    // CommonJS
    module.exports = toExport;
    return;
  }
  // Nashorn and rest
  return toExport;
}).call(this);
