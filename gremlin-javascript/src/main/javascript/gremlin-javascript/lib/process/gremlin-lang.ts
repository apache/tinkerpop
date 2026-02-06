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

import { P, TextP, EnumValue } from './traversal.js';
import { OptionsStrategy, TraversalStrategy } from './traversal-strategy.js';

export default class GremlinLang {
  private gremlin: string = '';
  private optionsStrategies: OptionsStrategy[] = [];
  
  constructor(toClone?: GremlinLang) {
    if (toClone) {
      this.gremlin = toClone.gremlin;
      this.optionsStrategies = [...toClone.optionsStrategies];
    }
  }
  
  getOptionsStrategies(): OptionsStrategy[] {
    return this.optionsStrategies;
  }
  
  private _argAsString(arg: any): string {
    if (arg === null || arg === undefined) return 'null';
    if (typeof arg === 'boolean') return arg ? 'true' : 'false';
    if (typeof arg === 'number') return String(arg);
    if (typeof arg === 'string') {
      const escaped = arg.replace(/\\/g, '\\\\').replace(/'/g, "\\'");
      return `'${escaped}'`;
    }
    if (arg instanceof P || arg instanceof TextP) {
      return arg.toString();
    }
    if (arg instanceof EnumValue) {
      return arg.toString();
    }
    if (arg instanceof TraversalStrategy && !(arg instanceof OptionsStrategy)) {
      const simpleName = arg.fqcn.split('.').pop() || arg.fqcn;
      const configEntries = Object.entries(arg.configuration);
      if (configEntries.length === 0) {
        return simpleName;
      }
      const configStr = configEntries.map(([k, v]) => `${k}:${this._argAsString(v)}`).join(', ');
      return `new ${simpleName}(${configStr})`;
    }
    if (Array.isArray(arg)) {
      return '[' + arg.map(a => this._argAsString(a)).join(', ') + ']';
    }
    if (typeof arg === 'object' && arg !== null && arg.constructor === Object) {
      const entries = Object.entries(arg);
      if (entries.length === 0) return '[:]';
      return '[' + entries.map(([k, v]) => `${this._argAsString(k)}:${this._argAsString(v)}`).join(', ') + ']';
    }
    return String(arg);
  }
  
  addStep(name: string, args?: any[]): GremlinLang {
    const argsStr = args?.length ? args.map(a => this._argAsString(a)).join(', ') : '';
    this.gremlin += `.${name}(${argsStr})`;
    return this;
  }
  
  addSource(name: string, args?: any[]): GremlinLang {
    if (name === 'withStrategies' && args) {
      const nonOptionsStrategies: any[] = [];
      for (const strategy of args) {
        if (strategy instanceof OptionsStrategy) {
          this.optionsStrategies.push(strategy);
        } else {
          nonOptionsStrategies.push(strategy);
        }
      }
      if (nonOptionsStrategies.length > 0) {
        const argsStr = nonOptionsStrategies.map(a => this._argAsString(a)).join(', ');
        this.gremlin += `.withStrategies(${argsStr})`;
      }
      return this;
    }
    const argsStr = args?.length ? args.map(a => this._argAsString(a)).join(', ') : '';
    this.gremlin += `.${name}(${argsStr})`;
    return this;
  }
  
  getGremlin(prefix: string = 'g'): string {
    return prefix + this.gremlin;
  }
}