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

class Bytecode {
  /**
   * Creates a new instance of Bytecode
   * @param {Bytecode} [toClone]
   */
  constructor(toClone) {
    if (!toClone) {
      this.sourceInstructions = [];
      this.stepInstructions = [];
    }
    else {
      this.sourceInstructions = [...toClone.sourceInstructions];
      this.stepInstructions = [...toClone.stepInstructions];
    }
  }

  /**
   * Adds a new source instructions
   * @param {String} name
   * @param {Array} values
   * @returns {Bytecode}
   */
  addSource(name, values) {
    if (name === undefined) {
      throw new Error('Name is not defined');
    }
    const instruction = new Array(values.length + 1);
    instruction[0] = name;
    for (let i = 0; i < values.length; ++i) {
      instruction[i + 1] = values[i];
    }
    this.sourceInstructions.push(Bytecode._generateInstruction(name, values));
    return this;
  }

  /**
   * Adds a new step instructions
   * @param {String} name
   * @param {Array} values
   * @returns {Bytecode}
   */
  addStep(name, values) {
    if (name === undefined) {
      throw new Error('Name is not defined');
    }
    this.stepInstructions.push(Bytecode._generateInstruction(name, values));
    return this;
  }

  static _generateInstruction(name, values) {
    const length = (values ? values.length : 0) + 1;
    const instruction = new Array(length);
    instruction[0] = name;
    for (let i = 1; i < length; i++) {
      instruction[i] = values[i - 1];
    }
    return instruction;
  }

  /**
   * Returns the JSON representation of the source and step instructions
   * @returns {String}
   */
  toString() {
    return (
      (this.sourceInstructions.length > 0 ? JSON.stringify(this.sourceInstructions) : '') +
      (this.stepInstructions.length   > 0 ? JSON.stringify(this.stepInstructions) : '')
    );
  }

  /**
   * Returns a script representations of the step instructions that can be used by standard eval operation.
   * @returns {Object} An object containing a script string and bindings map.
   */
  toScript() {
    let bindings = {};
    let script = 'g';
    let length = this.stepInstructions.length;

    // if eval was passed a script then simply execute the given script.
    if (this.stepInstructions[length - 1][0] === 'eval'
      && this.stepInstructions[length - 1][1] !== undefined
      && this.stepInstructions[length - 1][1] !== null
    ) {
      return {
        script: this.stepInstructions[length - 1][1],
        bindings: this.stepInstructions[length - 1][2]
      }
    }

    if (this.stepInstructions[length - 1][0] === 'eval') {
      this.stepInstructions.pop();
      length = this.stepInstructions.length;
    }

    // build the script from the glv instructions.
    let paramIdx = 1;
    for (let i = 0; i < length; i++) {
      const params = this.stepInstructions[i].slice(1);
      script = script + '.' + this.stepInstructions[i][0] + '(';

      if (params.length) {
        for (let k = 0; k < params.length; k++) {
          if (k > 0) {
            script = script + ', ';
          }

          if (Object(params[k]) === params[k]) {
            script = script + params[k].toString();
          } else {
            const prop = `p${paramIdx++}`;
            script = script + prop;
            
            if (typeof params[k] === 'number') {
              if (Number.isInteger(params[k])) {
                bindings[prop] = Number.parseInt(params[k]);
              } else {
                bindings[prop] = Number.parseFloat(params[k]);
              }
            } else if (params[k] === undefined) {
              bindings[prop] = null;
            } else {
              bindings[prop] = params[k];
            }
          }
        }
      }

      script = script + ')';
    }
    
    return {
      script,
      bindings
    };
  }
}

module.exports = Bytecode;
