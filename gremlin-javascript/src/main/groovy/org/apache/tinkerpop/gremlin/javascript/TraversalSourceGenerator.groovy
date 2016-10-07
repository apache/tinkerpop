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

import org.apache.tinkerpop.gremlin.process.traversal.P
import org.apache.tinkerpop.gremlin.util.CoreImports
import org.apache.tinkerpop.gremlin.javascript.jsr223.SymbolHelper

import java.lang.reflect.Modifier

/**
 * @author Jorge Bay Gondra
 */
class TraversalSourceGenerator {

    public static void create(final String traversalSourceFile) {

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
        moduleOutput.append("""
/**
 * @author Jorge Bay Gondra
 */
(function defineTraversalModule() {
  "use strict";

  function Traversal(graph, traversalStrategies, bytecode) {
    this._graph = graph;
    this._traversalStrategies = traversalStrategies;
    this._bytecode = bytecode;
    this.traversers = null;
    this.sideEffects = null;
  }

  /** @returns {Bytecode} */
  Traversal.prototype.getBytecode = function () {
    return this._bytecode;
  };

  /** @param {Function} callback */
  Traversal.prototype.list = function (callback) {
    var self = this;
    this._traversalStrategies.applyStrategies(this, function (err) {
      if (err) {
        return callback(err);
      }
      callback(err, self.traversers);
    });
  };

  /** @param {Function} callback */
  Traversal.prototype.one = function (callback) {
    this.list(function (err, result) {
      callback(err, result ? result[0] : null);
    });
  };

  /**
   * Returns the Bytecode JSON representation of the traversal
   * @returns {String}
   */
  Traversal.prototype.toString = function () {
    return this._bytecode.toString();
  };
  """);

        moduleOutput.append("""
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
""")
        P.class.getMethods().
                findAll { Modifier.isStatic(it.getModifiers()) }.
                findAll { P.class.isAssignableFrom(it.returnType) }.
                collect { SymbolHelper.toJs(it.name) }.
                unique().
                sort { a, b -> a <=> b }.
                each { method ->
                    moduleOutput.append(
                            """
  /** @param {...Object} args */
  P.${method} = function (args) {
    return createP('${SymbolHelper.toJava(method)}', parseArgs.apply(null, arguments));
  };
""")
                };
        moduleOutput.append("""
  P.prototype.and = function (arg) {
    return new P('and', this, arg);
  };

  P.prototype.or = function (arg) {
    return new P('or', this, arg);
  };
""")

        moduleOutput.append("""
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
    Traverser: Traverser""")
        for (final Class<? extends Enum> enumClass : CoreImports.getClassImports()
                .findAll { Enum.class.isAssignableFrom(it) }
                .sort { a, b -> a.getSimpleName() <=> b.getSimpleName() }
                .collect()) {
            moduleOutput.append(",\n    ${SymbolHelper.decapitalize(enumClass.getSimpleName())}: " +
                    "toEnum('${SymbolHelper.toJs(enumClass.getSimpleName())}', '");
            enumClass.getEnumConstants()
                    .sort { a, b -> a.name() <=> b.name() }
                    .each { value -> moduleOutput.append("${SymbolHelper.toJs(value.name())} "); }
            moduleOutput.deleteCharAt(moduleOutput.length() - 1).append("')")
        }

        moduleOutput.append("""
  };
  if (typeof module !== 'undefined') {
    // CommonJS
    module.exports = toExport;
    return;
  }
  // Nashorn and rest
  return toExport;
}).call(this);""")

        // save to a file
        final File file = new File(traversalSourceFile);
        file.delete()
        moduleOutput.eachLine { file.append(it + "\n") }
    }
}
