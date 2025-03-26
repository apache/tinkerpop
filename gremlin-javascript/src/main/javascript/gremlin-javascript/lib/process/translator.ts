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

import { Traversal } from './traversal.js';
import Bytecode from './bytecode.js';

/**
 * Class to translate glv bytecode steps into executable Gremlin-Groovy script
 */
export default class Translator {
  constructor(private traversalSource: string) {}

  getTraversalSource() {
    return this.traversalSource;
  }

  getTargetLanguage() {
    return 'gremlin-groovy';
  }

  of(traversalSource: string) {
    this.traversalSource = traversalSource;
  }

  /**
   * Returns a script representation of the given bytecode instructions.
   * @param {Object} bytecodeOrTraversal The traversal or bytecode of a traversal containing step instructions.
   * @param {boolean} child Determines if a traversal object should be treated as an anonymous child or if it is a spawn from "g"
   * @returns {string} Gremlin-Groovy script
   */
  translate(bytecodeOrTraversal: Traversal | Bytecode, child: boolean = false): string {
    let script = child ? '__' : this.traversalSource;
    const bc = bytecodeOrTraversal instanceof Bytecode ? bytecodeOrTraversal : bytecodeOrTraversal.getBytecode();

    const instructions = bc.stepInstructions;

    // build the script from the glv instructions.
    for (let i = 0; i < instructions.length; i++) {
      const params = instructions[i].slice(1);
      script += '.' + instructions[i][0] + '(';

      if (params.length) {
        for (let k = 0; k < params.length; k++) {
          if (k > 0) {
            script += ', ';
          }

          script += this.convert(params[k]);
        }
      }

      script += ')';
    }

    return script;
  }

  /**
   * Converts an object to a Gremlin script representation.
   * @param {Object} anyObject The object to convert to a script representation
   * @returns {string} The Gremlin script representation
   */
  convert(anyObject: any): string {
    let script = '';
    if (Object(anyObject) === anyObject) {
      if (anyObject instanceof Traversal) {
        script += this.translate(anyObject.getBytecode(), true);
      } else if (anyObject.toString() === '[object Object]') {
        Object.keys(anyObject).forEach(function (key, index) {
          if (index > 0) {
            script += ', ';
          }
          script += `('${key}', `;
          if (anyObject[key] instanceof String || typeof anyObject[key] === 'string') {
            script += `'${`${anyObject[key]}`.replaceAll("'", "\\'")}'`; // eslint-disable-line quotes
          } else {
            script += anyObject[key];
          }
          script += ')';
        });
      } else if (Array.isArray(anyObject)) {
        const parts = [];
        for (const item of anyObject) {
          parts.push(this.convert(item));
        }
        script += '[' + parts.join(', ') + ']';
      } else {
        script += anyObject.toString();
      }
    } else if (anyObject === undefined || anyObject === null) {
      script += 'null';
    } else if (typeof anyObject === 'number' || typeof anyObject === 'boolean') {
      script += anyObject;
    } else {
      script += `'${`${anyObject}`.replaceAll("'", "\\'")}'`; // eslint-disable-line quotes
    }

    return script;
  }
}
