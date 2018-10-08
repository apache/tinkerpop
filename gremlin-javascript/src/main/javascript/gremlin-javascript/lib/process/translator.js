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
'use strict';

/**
 * Class to translate glv bytecode steps into executable Gremlin-Groovy script
 */
class Translator {
  constructor(traversalSource) {
    this._traversalSource = traversalSource;
  }

  getTraversalSource() {
    return this._traversalSource;
  }

  getTargetLanguage() {
    return "gremlin-groovy";
  }

  of(traversalSource) {
    this._traversalSource = traversalSource;
  }

  /**
   * Returns a script representation of the given bytecode instructions.
   * @param {Object} bytecode The bytecode object containing step instructions.
   * @returns {string} Gremlin-Groovy script
   */
  translate(bytecode) {
    let script = this._traversalSource;
    let instructions = bytecode.stepInstructions;

    // build the script from the glv instructions.
    for (let i = 0; i < instructions.length; i++) {
      const params = instructions[i].slice(1);
      script += '.' + instructions[i][0] + '(';

      if (params.length) {
        for (let k = 0; k < params.length; k++) {
          if (k > 0) {
            script += ', ';
          }

          if (Object(params[k]) === params[k]) {
            if (params[k].toString() === '[object Object]') {
              Object.keys(params[k]).forEach(function(key, index) {
                if (index > 0) script += ', ';
                script += '(\'' + key + '\', ';
                if (params[k][key] instanceof String || typeof params[k][key] === 'string') {
                  script += '\'' + params[k][key] + '\'';
                } else {
                  script += params[k][key];
                }
                script += ')';
              });
            } else {
              script += params[k].toString();
            }
          } else if (params[k] === undefined) {
            script += '';
          } else if (typeof params[k] === 'number') {
            script += params[k];
          } else {
            script += '\'' + params[k] + '\'';
          }
        }
      }

      script += ')';
    }
    
    return script;
  }
}

module.exports = Translator;