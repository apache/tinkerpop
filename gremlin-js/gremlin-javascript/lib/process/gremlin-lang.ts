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
import { Long, Int, Float, Double, Short, Byte, INT32_MIN, INT32_MAX } from '../utils.js';
import { Vertex, CompositePDT, PrimitivePDT } from '../structure/graph.js';
import { PDTRegistry } from '../structure/PDTRegistry.js';
import { GValue } from './gvalue.js';
import { isDeepStrictEqual } from 'node:util';
import { Buffer } from 'buffer';

const PARAM_NAME_PATTERN = /^[\p{L}_$][\p{L}\p{Nd}_$]*$/u;

export default class GremlinLang {
  private gremlin: string = '';
  private optionsStrategies: OptionsStrategy[] = [];
  private parameters: Map<string, any> = new Map();
  pdtRegistry: PDTRegistry | null = null;
  
  constructor(toClone?: GremlinLang) {
    if (toClone) {
      this.gremlin = toClone.gremlin;
      this.optionsStrategies = [...toClone.optionsStrategies];
      this.parameters = new Map(toClone.parameters);
      this.pdtRegistry = toClone.pdtRegistry;
    }
  }
  
  getOptionsStrategies(): OptionsStrategy[] {
    return this.optionsStrategies;
  }

  appendGremlin(text: string): void {
    this.gremlin += text;
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
      // If all elements are traversals, serialize as comma-separated args (no brackets)
      // This matches the server grammar: within(trav1, trav2) via genericArgumentVarargs
      const allTraversals = p.value.length > 0 && p.value.every((v: any) => v != null && typeof v.getGremlinLang === 'function');
      if (allTraversals) {
        result += p.value.map((v: any) => this._argAsString(v)).join(',');
      } else {
        result += '[' + p.value.map((v: any) => this._argAsString(v)).join(',') + ']';
      }
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
    if (arg instanceof Float) {
      return GremlinLang._fpAsString(arg.value, 'F');
    }
    if (arg instanceof Double) {
      return GremlinLang._fpAsString(arg.value, 'D');
    }
    if (arg instanceof Short) {
      return String(arg.value) + 'S';
    }
    if (arg instanceof Byte) {
      return String(arg.value) + 'B';
    }
    if (arg instanceof Int) {
      return String(arg.value);
    }
    if (typeof arg === 'bigint') {
      return String(arg) + 'N';
    }
    if (typeof arg === 'number') {
      if (Number.isNaN(arg)) return 'NaN';
      if (arg === Infinity) return '+Infinity';
      if (arg === -Infinity) return '-Infinity';
      if (!Number.isInteger(arg)) return String(arg) + 'D';
      if (arg >= INT32_MIN && arg <= INT32_MAX) return String(arg);
      // Outside safe integer range, values have lost precision and may exceed Java Long - emit as Double.
      if (arg > Number.MAX_SAFE_INTEGER || arg < -Number.MAX_SAFE_INTEGER) return String(arg) + 'D';
      return String(arg) + 'L';
    }
    if (arg instanceof Date) {
      const iso = arg.toISOString();
      return `datetime("${iso}")`;
    }
    if (typeof arg === 'string') {
      // JSON.stringify handles all special character escaping in one call.
      // Strip the outer double quotes and re-wrap in single quotes for Gremlin.
      const escaped = JSON.stringify(arg).slice(1, -1).replace(/'/g, "\\'");
      return `'${escaped}'`;
    }
    if (arg instanceof GValue) {
      const key = arg.name;
      if (!PARAM_NAME_PATTERN.test(key)) {
        throw new Error(`Invalid parameter name [${key}].`);
      }
      if (this.parameters.has(key)) {
        if (!isDeepStrictEqual(this.parameters.get(key), arg.value)) {
          throw new Error(`Parameter with name ${key} already exists.`);
        }
      } else {
        this.parameters.set(key, arg.value);
      }
      return key;
    }
    if (arg instanceof P || arg instanceof TextP) {
      return this._predicateAsString(arg);
    }
    if (arg instanceof EnumValue) {
      return arg.toString();
    }
    if (arg instanceof TraversalStrategy && !(arg instanceof OptionsStrategy)) {
      const simpleName = arg.strategyName;
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
    if (arg instanceof PrimitivePDT) {
      // PDT literals use double-quoted strings (consistent with the composite PDT form below);
      // JSON.stringify handles special-character escaping, then the outer quotes are stripped.
      const escapedName = JSON.stringify(arg.name).slice(1, -1);
      const escapedValue = JSON.stringify(arg.value).slice(1, -1);
      return `PDT("${escapedName}","${escapedValue}")`;
    }
    if (arg instanceof CompositePDT) {
      const fields = arg.fields;
      const keys = Object.keys(fields);
      const escapedName = JSON.stringify(arg.name).slice(1, -1);
      if (keys.length === 0) return `PDT("${escapedName}",[:])`; 
      const entries = keys.map(k => `${this._argAsString(k)}:${this._argAsString(fields[k])}`);
      return `PDT("${escapedName}",[${entries.join(',')}])`;
    }
    if (arg instanceof Vertex) {
      return this._argAsString(arg.id);
    }
    if (arg instanceof GremlinLang) {
      // Merge the child's parameters so GValue bindings nested inside a child
      // traversal are still sent to the server alongside the rendered query.
      arg.parameters.forEach((v, k) => this.parameters.set(k, v));
      return arg.getGremlin('__');
    }
    if (typeof arg.getGremlinLang === 'function') {
      // This is a Traversal - render as anonymous traversal
      if (arg.graph != null) {
        throw new Error('Child traversal must be anonymous - use __ not g');
      }
      const childLang = arg.getGremlinLang();
      // Merge the child's parameters so GValue bindings nested inside a child
      // traversal are still sent to the server alongside the rendered query.
      childLang.parameters.forEach((v: any, k: string) => this.parameters.set(k, v));
      return childLang.getGremlin('__');
    }
    if (arg instanceof Uint8Array) {
      return `Binary("${Buffer.from(arg.buffer, arg.byteOffset, arg.byteLength).toString('base64')}")`;
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
    // Registry-based dehydration
    if (this.pdtRegistry && typeof arg === 'object' && arg.constructor) {
      const primitiveEntry = this.pdtRegistry.getPrimitiveAdapterByClass(arg.constructor);
      if (primitiveEntry) {
        const value = primitiveEntry.toValue(arg);
        return this._argAsString(new PrimitivePDT(primitiveEntry.typeName, value));
      }
      const entry = this.pdtRegistry.getAdapterByClass(arg.constructor);
      if (entry) {
        const fields = entry.serialize(arg);
        return this._argAsString(new CompositePDT(entry.typeName, fields));
      }
    }
    throw new TypeError(`GremlinLang contains at least one type [${arg?.constructor?.name ?? typeof arg}] that cannot be represented as text.`);
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
          // Render multilabel/singlelabel in gremlin text (temporary until these options are removed)
          if (strategy.configuration['multilabel'] !== undefined) {
            this.gremlin += '.with("multilabel")';
          }
          if (strategy.configuration['singlelabel'] !== undefined) {
            this.gremlin += '.with("singlelabel")';
          }
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
  
  private static _fpAsString(v: number, suffix: 'F' | 'D'): string {
    if (v === Infinity) return '+Infinity';
    if (v === -Infinity) return '-Infinity';
    if (Number.isNaN(v)) return 'NaN';
    return Number.isInteger(v) ? `${v}.0${suffix}` : `${v}${suffix}`;
  }

  getGremlin(prefix: string = 'g'): string {
    if (this.gremlin.length > 0 && this.gremlin[0] !== '.') {
      return this.gremlin;
    }
    return prefix + this.gremlin;
  }

  getParametersAsString(): string {
    return GremlinLang.convertParametersToString(this.parameters);
  }

  static convertParametersToString(params: Map<string, any> | null): string {
    if (params == null || params.size === 0) return '[:]';

    const helper = new GremlinLang();
    const parts: string[] = [];
    params.forEach((v, k) => {
      parts.push(`${helper._argAsString(k)}:${helper._argAsString(v)}`);
    });
    return '[' + parts.join(',') + ']';
  }
}