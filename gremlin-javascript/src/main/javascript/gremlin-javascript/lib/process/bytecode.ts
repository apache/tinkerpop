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

import { Traversal } from './traversal.js';

export default class Bytecode {
  sourceInstructions: any[] = [];
  stepInstructions: any[] = [];

  /**
   * Creates a new instance of Bytecode
   * @param {Bytecode} [toClone]
   */
  constructor(toClone?: Bytecode) {
    if (!toClone) {
      this.sourceInstructions = [];
      this.stepInstructions = [];
    } else {
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
  addSource(name: string, values: any[]): Bytecode {
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
  addStep(name: string, values?: any[]): Bytecode {
    if (name === undefined) {
      throw new Error('Name is not defined');
    }
    this.stepInstructions.push(Bytecode._generateInstruction(name, values));
    return this;
  }

  private static _generateInstruction(name: string, values?: any[]) {
    const length = (values ? values.length : 0) + 1;
    const instruction = new Array(length);
    instruction[0] = name;
    for (let i = 1; i < length; i++) {
      const val = values?.[i - 1];
      if (val instanceof Traversal && val.graph != null) {
        throw new Error(
          `The child traversal of ${val} was not spawned anonymously - use ` +
            'the __ class rather than a TraversalSource to construct the child traversal',
        );
      }
      instruction[i] = val;
    }
    return instruction;
  }

  /**
   * Returns the JSON representation of the source and step instructions
   * @returns {String}
   */
  toString(): string {
    return JSON.stringify([this.sourceInstructions, this.stepInstructions]);
  }

  /**
   * Adds a new source instructions
   * @param {String} name
   * @param {Array} values
   * @returns {Bytecode}
   */
  static _createGraphOp(name: string, values: any[]): Bytecode {
    const bc = new Bytecode();
    bc.addSource(name, values);
    return bc;
  }

  /**
   * Gets the <code>Bytecode</code> that is meant to be sent as "graph operations" to the server.
   * @returns {{rollback: Bytecode, commit: Bytecode}}
   */
  static get GraphOp(): { rollback: Bytecode; commit: Bytecode } {
    return {
      commit: Bytecode._createGraphOp('tx', ['commit']),
      rollback: Bytecode._createGraphOp('tx', ['rollback']),
    };
  }
}
