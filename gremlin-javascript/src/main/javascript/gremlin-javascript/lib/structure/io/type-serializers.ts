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

import * as t from '../../process/traversal.js';
import * as ts from '../../process/traversal-strategy.js';
import Bytecode from '../../process/bytecode.js';
import * as g from '../graph.js';
import * as utils from '../../utils.js';

export const valueKey = '@value';
export const typeKey = '@type';

export type SerializedValue = { [typeKey]: string; [valueKey]: any };

/**
 * @abstract
 */
export class TypeSerializer<T = any> {
  reader: any;
  writer: any;

  serialize(value: T): T | SerializedValue {
    throw new Error('serialize() method not implemented for ' + this.constructor.name);
  }

  deserialize<TObject extends SerializedValue>(value: TObject): T {
    throw new Error('deserialize() method not implemented for ' + this.constructor.name);
  }

  canBeUsedFor?(value: unknown): boolean {
    throw new Error('canBeUsedFor() method not implemented for ' + this.constructor.name);
  }
}

export class NumberSerializer extends TypeSerializer<number> {
  serialize(item: number) {
    if (isNaN(item)) {
      return {
        [typeKey]: 'g:Double',
        [valueKey]: 'NaN',
      };
    } else if (item === Number.POSITIVE_INFINITY) {
      return {
        [typeKey]: 'g:Double',
        [valueKey]: 'Infinity',
      };
    } else if (item === Number.NEGATIVE_INFINITY) {
      return {
        [typeKey]: 'g:Double',
        [valueKey]: '-Infinity',
      };
    }
    return item;
  }

  deserialize(obj: SerializedValue) {
    const val = obj[valueKey];
    if (val === 'NaN') {
      return NaN;
    } else if (val === 'Infinity') {
      return Number.POSITIVE_INFINITY;
    } else if (val === '-Infinity') {
      return Number.NEGATIVE_INFINITY;
    }
    return parseFloat(val);
  }

  canBeUsedFor(value: unknown) {
    return typeof value === 'number';
  }
}

export class DateSerializer extends TypeSerializer<Date> {
  serialize(item: Date) {
    return {
      [typeKey]: 'g:Date',
      [valueKey]: item.getTime(),
    };
  }

  deserialize(obj: SerializedValue) {
    return new Date(obj[valueKey]);
  }

  canBeUsedFor(value: unknown) {
    return value instanceof Date;
  }
}

export class LongSerializer extends TypeSerializer<utils.Long> {
  serialize(item: utils.Long) {
    return {
      [typeKey]: 'g:Int64',
      [valueKey]: item.value,
    };
  }

  canBeUsedFor(value: unknown) {
    return value instanceof utils.Long;
  }
}

export class BytecodeSerializer extends TypeSerializer<Bytecode> {
  serialize(item: Bytecode) {
    let bytecode = item;
    if (item instanceof t.Traversal) {
      bytecode = item.getBytecode();
    }
    const result: Partial<SerializedValue> = {};
    result[typeKey] = 'g:Bytecode';
    const resultValue: any = (result[valueKey] = {});
    const sources = this._serializeInstructions(bytecode.sourceInstructions);
    if (sources) {
      resultValue['source'] = sources;
    }
    const steps = this._serializeInstructions(bytecode.stepInstructions);
    if (steps) {
      resultValue['step'] = steps;
    }
    return result as SerializedValue;
  }

  _serializeInstructions(instructions: any[]) {
    if (instructions.length === 0) {
      return null;
    }
    const result = new Array(instructions.length);
    result[0] = instructions[0];
    for (let i = 0; i < instructions.length; i++) {
      result[i] = instructions[i].map((item: unknown) => this.writer.adaptObject(item));
    }
    return result;
  }

  canBeUsedFor(value: unknown) {
    return value instanceof Bytecode || value instanceof t.Traversal;
  }
}

export class PSerializer extends TypeSerializer<t.P> {
  /** @param {P} item */
  serialize(item: t.P) {
    const result: Partial<SerializedValue> = {};
    result[typeKey] = 'g:P';
    const resultValue: any = (result[valueKey] = {
      predicate: item.operator,
    });
    if (item.other === undefined || item.other === null) {
      resultValue['value'] = this.writer.adaptObject(item.value);
    } else {
      resultValue['value'] = [this.writer.adaptObject(item.value), this.writer.adaptObject(item.other)];
    }
    return result as SerializedValue;
  }

  canBeUsedFor(value: unknown) {
    return value instanceof t.P;
  }
}

export class TextPSerializer extends TypeSerializer<t.TextP> {
  /** @param {TextP} item */
  serialize(item: t.TextP) {
    const result: Partial<SerializedValue> = {};
    result[typeKey] = 'g:TextP';
    const resultValue: any = (result[valueKey] = {
      predicate: item.operator,
    });
    if (item.other === undefined || item.other === null) {
      resultValue['value'] = this.writer.adaptObject(item.value);
    } else {
      resultValue['value'] = [this.writer.adaptObject(item.value), this.writer.adaptObject(item.other)];
    }
    return result as SerializedValue;
  }

  canBeUsedFor(value: unknown) {
    return value instanceof t.TextP;
  }
}

export class LambdaSerializer extends TypeSerializer<() => unknown[]> {
  /** @param {Function} item */
  serialize(item: () => any[]) {
    const lambdaDef = item();

    // check if the language is specified otherwise assume gremlin-groovy.
    const returnIsString = typeof lambdaDef === 'string';
    const script = returnIsString ? lambdaDef : lambdaDef[0];
    const lang = returnIsString ? 'gremlin-groovy' : lambdaDef[1];

    // detect argument count
    const argCount =
      lang === 'gremlin-groovy' && script.includes('->')
        ? script.substring(0, script.indexOf('->')).includes(',')
          ? 2
          : 1
        : -1;

    return {
      [typeKey]: 'g:Lambda',
      [valueKey]: {
        arguments: argCount,
        language: lang,
        script: script,
      },
    };
  }

  canBeUsedFor(value: unknown) {
    return typeof value === 'function';
  }
}

export class EnumSerializer extends TypeSerializer<t.EnumValue> {
  /** @param {EnumValue} item */
  serialize(item: t.EnumValue) {
    return {
      [typeKey]: 'g:' + item.typeName,
      [valueKey]: item.elementName,
    };
  }

  canBeUsedFor(value: unknown) {
    return value && (value as any).typeName && value instanceof t.EnumValue;
  }
}

export class TraverserSerializer extends TypeSerializer<t.Traverser> {
  /** @param {Traverser} item */
  serialize(item: t.Traverser) {
    return {
      [typeKey]: 'g:Traverser',
      [valueKey]: {
        value: this.writer.adaptObject(item.object),
        bulk: this.writer.adaptObject(item.bulk),
      },
    };
  }

  deserialize(obj: SerializedValue) {
    const value = obj[valueKey];
    return new t.Traverser(this.reader.read(value['value']), this.reader.read(value['bulk']));
  }

  canBeUsedFor(value: unknown) {
    return value instanceof t.Traverser;
  }
}

export class TraversalStrategySerializer extends TypeSerializer<ts.TraversalStrategy> {
  /** @param {TraversalStrategy} item */
  serialize(item: ts.TraversalStrategy) {
    const conf: ts.TraversalStrategyConfiguration = {};
    for (const k in item.configuration) {
      if (item.configuration.hasOwnProperty(k)) {
        conf[k] = this.writer.adaptObject(item.configuration[k]);
      }
    }

    return {
      [typeKey]: 'g:' + item.constructor.name,
      [valueKey]: conf,
    };
  }

  canBeUsedFor(value: unknown) {
    return value instanceof ts.TraversalStrategy;
  }
}

export class VertexSerializer extends TypeSerializer<g.Vertex> {
  deserialize(obj: SerializedValue) {
    const value = obj[valueKey];
    return new g.Vertex(this.reader.read(value['id']), value['label'], this.reader.read(value['properties']));
  }

  /** @param {Vertex} item */
  serialize(item: g.Vertex) {
    return {
      [typeKey]: 'g:Vertex',
      [valueKey]: {
        id: this.writer.adaptObject(item.id),
        label: item.label,
      },
    };
  }

  canBeUsedFor(value: unknown) {
    return value instanceof g.Vertex;
  }
}

export class VertexPropertySerializer extends TypeSerializer<g.VertexProperty> {
  deserialize(obj: SerializedValue) {
    const value = obj[valueKey];
    return new g.VertexProperty(
      this.reader.read(value['id']),
      value['label'],
      this.reader.read(value['value']),
      this.reader.read(value['properties']),
    );
  }
}

export class PropertySerializer extends TypeSerializer<g.Property> {
  deserialize(obj: SerializedValue) {
    const value = obj[valueKey];
    return new g.Property(value['key'], this.reader.read(value['value']));
  }
}

export class EdgeSerializer extends TypeSerializer<g.Edge> {
  deserialize(obj: SerializedValue) {
    const value = obj[valueKey];
    return new g.Edge(
      this.reader.read(value['id']),
      new g.Vertex(this.reader.read(value['outV']), this.reader.read(value['outVLabel'])),
      value['label'],
      new g.Vertex(this.reader.read(value['inV']), this.reader.read(value['inVLabel'])),
      this.reader.read(value['properties']),
    );
  }

  /** @param {Edge} item */
  serialize(item: g.Edge) {
    return {
      [typeKey]: 'g:Edge',
      [valueKey]: {
        id: this.writer.adaptObject(item.id),
        label: item.label,
        outV: this.writer.adaptObject(item.outV.id),
        outVLabel: item.outV.label,
        inV: this.writer.adaptObject(item.inV.id),
        inVLabel: item.inV.label,
      },
    };
  }

  canBeUsedFor(value: unknown) {
    return value instanceof g.Edge;
  }
}

export class PathSerializer extends TypeSerializer<g.Path> {
  deserialize(obj: SerializedValue) {
    const value = obj[valueKey];
    const objects = value['objects'].map((o: unknown) => this.reader.read(o));
    return new g.Path(this.reader.read(value['labels']), objects);
  }
}

export class Path3Serializer extends TypeSerializer<g.Path> {
  deserialize(obj: SerializedValue) {
    const value = obj[valueKey];
    return new g.Path(this.reader.read(value['labels']), this.reader.read(value['objects']));
  }
}

export class TSerializer extends TypeSerializer<t.EnumValue> {
  deserialize(obj: SerializedValue) {
    return t.t[obj[valueKey]];
  }
}

export class DirectionSerializer extends TypeSerializer<t.EnumValue> {
  deserialize(obj: SerializedValue) {
    return t.direction[obj[valueKey].toLowerCase()];
  }
}

class ArraySerializer extends TypeSerializer<unknown[]> {
  constructor(readonly typeKey: string) {
    super();
  }

  deserialize(obj: SerializedValue) {
    const value = obj[valueKey];
    if (!Array.isArray(value)) {
      throw new Error('Expected Array, obtained: ' + value);
    }
    return value.map((x) => this.reader.read(x));
  }

  /** @param {Array} item */
  serialize(item: unknown[]) {
    return {
      [typeKey]: this.typeKey,
      [valueKey]: item.map((x) => this.writer.adaptObject(x)),
    };
  }

  canBeUsedFor(value: unknown) {
    return Array.isArray(value);
  }
}

export class BulkSetSerializer extends TypeSerializer<unknown> {
  deserialize(obj: SerializedValue) {
    const value = obj[valueKey];
    if (!Array.isArray(value)) {
      throw new Error('Expected Array, obtained: ' + value);
    }

    // coerce the BulkSet to List. if the bulk exceeds the int space then we can't coerce to List anyway,
    // so this query will be trouble. we'd need a legit BulkSet implementation here in js. this current
    // implementation is here to replicate the previous functionality that existed on the server side in
    // previous versions.
    let result: unknown[] = [];
    for (let ix = 0, iy = value.length; ix < iy; ix += 2) {
      const pair = value.slice(ix, ix + 2);
      result = result.concat(Array(this.reader.read(pair[1])).fill(this.reader.read(pair[0])));
    }

    return result;
  }
}

export class MapSerializer extends TypeSerializer<Map<unknown, unknown>> {
  deserialize(obj: SerializedValue) {
    const value = obj[valueKey];
    if (!Array.isArray(value)) {
      throw new Error('Expected Array, obtained: ' + value);
    }
    const result = new Map();
    for (let i = 0; i < value.length; i += 2) {
      result.set(this.reader.read(value[i]), this.reader.read(value[i + 1]));
    }
    return result;
  }

  /** @param {Map} map */
  serialize(map: Map<unknown, unknown>) {
    const arr: unknown[] = [];
    map.forEach((v, k) => {
      arr.push(this.writer.adaptObject(k));
      arr.push(this.writer.adaptObject(v));
    });
    return {
      [typeKey]: 'g:Map',
      [valueKey]: arr,
    };
  }

  canBeUsedFor(value: unknown) {
    return value instanceof Map;
  }
}

export class ListSerializer extends ArraySerializer {
  constructor() {
    super('g:List');
  }
}

export class SetSerializer extends ArraySerializer {
  constructor() {
    super('g:Set');
  }

  deserialize(obj: SerializedValue): any {
    return new Set(super.deserialize(obj));
  }
}
