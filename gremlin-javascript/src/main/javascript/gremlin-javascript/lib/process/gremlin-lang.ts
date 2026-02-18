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
import { Long } from '../utils.js';
import { Vertex } from '../structure/graph.js';

export default class GremlinLang {
  private gremlin: string = '';
  private optionsStrategies: OptionsStrategy[] = [];
  private parameters: Map<string, any> = new Map();
  
  constructor(toClone?: GremlinLang) {
    if (toClone) {
      this.gremlin = toClone.gremlin;
      this.optionsStrategies = [...toClone.optionsStrategies];
      this.parameters = new Map(toClone.parameters);
    }
  }
  
  getOptionsStrategies(): OptionsStrategy[] {
    return this.optionsStrategies;
  }

  addG(g: string): void {
    this.parameters.set('g', g);
  }

  getParameters(): Map<string, any> {
    return this.parameters;
  }
  
  private _predicateAsString(p: P | TextP): string {
    if (p.operator === 'and' || p.operator === 'or') {
      return `${this._predicateAsString(p.value)}.${p.operator}(${this._predicateAsString(p.other)})`;
    }
    
    let result = p.operator + '(';
    if (Array.isArray(p.value)) {
      result += '[' + p.value.map(v => this._argAsString(v)).join(',') + ']';
    } else {
      result += this._argAsString(p.value);
      if (p.other !== undefined && p.other !== null) {
        result += ',' + this._argAsString(p.other);
      }
    }
    result += ')';
    return result;
  }

  private _argAsString(arg: any): string {
    if (arg === null || arg === undefined) return 'null';
    if (typeof arg === 'boolean') return arg ? 'true' : 'false';
    if (arg instanceof Long) {
      return String(arg.value) + 'L';
    }
    if (arg instanceof Date) {
      const iso = arg.toISOString();
      return `datetime("${iso}")`;
    }
    if (typeof arg === 'number') {
      if (Number.isNaN(arg)) return 'NaN';
      if (arg === Infinity) return '+Infinity';
      if (arg === -Infinity) return '-Infinity';
      return String(arg);
    }
    if (typeof arg === 'string') {
      // JSON.stringify handles all special character escaping in one call.
      // Strip the outer double quotes and re-wrap in single quotes for Gremlin.
      const escaped = JSON.stringify(arg).slice(1, -1).replace(/'/g, "\\'");
      return `'${escaped}'`;
    }
    if (arg instanceof P || arg instanceof TextP) {
      return this._predicateAsString(arg);
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
      const configStr = configEntries.map(([k, v]) => `${k}:${this._argAsString(v)}`).join(',');
      return `new ${simpleName}(${configStr})`;
    }
    if (typeof arg === 'function' && arg.prototype instanceof TraversalStrategy) {
      return arg.name;
    }
    if (arg instanceof Vertex) {
      return this._argAsString(arg.id);
    }
    if (arg instanceof GremlinLang) {
      return arg.getGremlin('__');
    }
    if (typeof arg.getGremlinLang === 'function') {
      // This is a Traversal - render as anonymous traversal
      if (arg.graph != null) {
        throw new Error('Child traversal must be anonymous - use __ not g');
      }
      return arg.getGremlinLang().getGremlin('__');
    }
    if (arg instanceof Set) {
      const items = [...arg];
      if (items.length === 0) return '{}';
      return '{' + items.map(a => this._argAsString(a)).join(',') + '}';
    }
    if (arg instanceof Map) {
      if (arg.size === 0) return '[:]';
      const entries: string[] = [];
      arg.forEach((v, k) => {
        const ks = this._argAsString(k);
        const vs = this._argAsString(v);
        entries.push(typeof k === 'string' ? `${ks}:${vs}` : `(${ks}):${vs}`);
      });
      return '[' + entries.join(',') + ']';
    }
    if (Array.isArray(arg)) {
      return '[' + arg.map(a => this._argAsString(a)).join(',') + ']';
    }
    if (typeof arg === 'object' && arg.constructor === Object) {
      const entries = Object.entries(arg);
      if (entries.length === 0) return '[:]';
      return '[' + entries.map(([k, v]) => `${this._argAsString(k)}:${this._argAsString(v)}`).join(',') + ']';
    }
    return String(arg);
  }
  
  addStep(name: string, args?: any[]): GremlinLang {
    const argsStr = args?.length ? args.map(a => this._argAsString(a)).join(',') : '';
    this.gremlin += `.${name}(${argsStr})`;
    return this;
  }
  
  addSource(name: string, args?: any[]): GremlinLang {
    if (name === 'CardinalityValueTraversal' && args) {
      const card = args[0] as EnumValue;
      this.gremlin += `Cardinality.${card.elementName}(${this._argAsString(args[1])})`;
      return this;
    }
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
        const argsStr = nonOptionsStrategies.map(a => this._argAsString(a)).join(',');
        this.gremlin += `.withStrategies(${argsStr})`;
      }
      return this;
    }
    const argsStr = args?.length ? args.map(a => this._argAsString(a)).join(',') : '';
    this.gremlin += `.${name}(${argsStr})`;
    return this;
  }
  
  getGremlin(prefix: string = 'g'): string {
    if (this.gremlin.length > 0 && this.gremlin[0] !== '.') {
      return this.gremlin;
    }
    return prefix + this.gremlin;
  }
}