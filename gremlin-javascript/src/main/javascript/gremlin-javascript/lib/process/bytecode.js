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

/**
 * Creates a new instance of Bytecode
 * @param {Bytecode} [toClone]
 * @constructor
 */
function Bytecode(toClone) {
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
  var length = (values ? values.length : 0) + 1;
  var instruction = new Array(length);
  instruction[0] = name;
  for (var i = 1; i < length; i++) {
    instruction[i] = this._convertToArgument(values[i - 1]);
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

module.exports = Bytecode;