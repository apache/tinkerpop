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

import { Buffer } from 'buffer';
import Bytecode from '../../process/bytecode.js';
import {
  BulkSetSerializer,
  BytecodeSerializer,
  DateSerializer,
  DirectionSerializer,
  EdgeSerializer,
  EnumSerializer,
  LambdaSerializer,
  ListSerializer,
  LongSerializer,
  MapSerializer,
  NumberSerializer,
  PSerializer,
  Path3Serializer,
  PropertySerializer,
  SetSerializer,
  TSerializer,
  TextPSerializer,
  TraversalStrategySerializer,
  TraverserSerializer,
  TypeSerializer,
  VertexPropertySerializer,
  VertexSerializer,
  typeKey,
  valueKey,
} from './type-serializers.js';

export type GraphWriterOptions = {
  serializers?: Record<string, TypeSerializer<any>>;
};

/**
 * GraphSON2 writer.
 */
export class GraphSON2Writer {
  private readonly _serializers: TypeSerializer<any>[];

  /**
   * @param {GraphWriterOptions} [options]
   * @param {TypeSerializer} [options.serializers] An object used as an associative array with GraphSON 2 type name as keys and
   * serializer instances as values, ie: { 'g:Int64': longSerializer }.
   * @constructor
   */
  constructor(private readonly options: GraphWriterOptions = {}) {
    // Create instance of the default serializers
    this._serializers = this.getDefaultSerializers().map((serializerConstructor) => {
      const s = new serializerConstructor();
      s.writer = this;
      return s;
    });

    const customSerializers = this.options.serializers || {};

    Object.keys(customSerializers).forEach((key) => {
      const s = customSerializers[key];
      if (!s.serialize) {
        return;
      }
      s.writer = this;
      // Insert custom serializers first
      this._serializers.unshift(s);
    });
  }

  /**
   * Gets the default serializers to be used.
   * @returns {Array}
   */
  getDefaultSerializers(): any[] {
    return graphSON2Serializers;
  }

  adaptObject<T>(value: T): any {
    let s;

    for (let i = 0; i < this._serializers.length; i++) {
      const currentSerializer = this._serializers[i];
      if (currentSerializer.canBeUsedFor?.(value)) {
        s = currentSerializer;
        break;
      }
    }

    if (s) {
      return s.serialize(value);
    }

    if (Array.isArray(value)) {
      // We need to handle arrays when there is no serializer
      // for older versions of GraphSON
      return value.map((item) => this.adaptObject(item));
    }

    // Default (strings / objects / ...)
    return value;
  }

  /**
   * Returns the GraphSON representation of the provided object instance.
   * @param {Object} obj
   * @returns {String}
   */
  write<T>(obj: T): string {
    return JSON.stringify(this.adaptObject(obj));
  }

  writeRequest({
    requestId,
    op,
    processor,
    args,
  }: {
    processor: string | undefined;
    op: string;
    args: any;
    requestId?: string | null;
  }) {
    const req = {
      requestId: { '@type': 'g:UUID', '@value': requestId },
      op,
      processor,
      args: this._adaptArgs(args, true),
    };

    if (req.args['gremlin'] instanceof Bytecode) {
      req.args['gremlin'] = this.adaptObject(req.args['gremlin']);
    }

    return Buffer.from(JSON.stringify(req));
  }

  /**
   * Takes the given args map and ensures all arguments are passed through to adaptObject
   * @param {Object} args Map of arguments to process.
   * @param {Boolean} protocolLevel Determines whether it's a protocol level binding.
   * @returns {Object}
   * @private
   */
  _adaptArgs<T extends Record<string, any>>(args: T, protocolLevel: boolean): T {
    if (args instanceof Object) {
      const newObj: Record<string, any> = {};
      Object.keys(args).forEach((key) => {
        // bindings key (at the protocol-level needs special handling. without this, it wraps the generated Map
        // in another map for types like EnumValue. Could be a nicer way to do this but for now it's solving the
        // problem with script submission of non JSON native types
        if (protocolLevel && key === 'bindings') {
          newObj[key] = this._adaptArgs(args[key], false);
        } else {
          newObj[key] = this.adaptObject(args[key]);
        }
      });

      return newObj as T;
    }

    return args;
  }
}

/**
 * GraphSON3 writer.
 */
export class GraphSON3Writer extends GraphSON2Writer {
  getDefaultSerializers() {
    return graphSON3Serializers;
  }
}

export type GraphReaderOptions = {
  serializers?: Record<string, TypeSerializer<any>>;
};

/**
 * GraphSON2 reader.
 */
export class GraphSON2Reader {
  private readonly _deserializers: Record<string, TypeSerializer<any>>;

  /**
   * GraphSON Reader
   * @param {GraphReaderOptions} [options]
   * @param {TypeSerializer} [options.serializers] An object used as an associative array with GraphSON 2 type name as keys and
   * deserializer instances as values, ie: { 'g:Int64': longSerializer }.
   * @constructor
   */
  constructor(private readonly options: GraphReaderOptions = {}) {
    this._deserializers = {};

    const defaultDeserializers = this.getDefaultDeserializers();
    Object.keys(defaultDeserializers).forEach((typeName) => {
      const serializerConstructor = defaultDeserializers[typeName as keyof typeof defaultDeserializers];
      const s = new serializerConstructor();
      s.reader = this;
      this._deserializers[typeName] = s;
    });

    if (this.options.serializers) {
      const customSerializers = this.options.serializers || {};
      Object.keys(customSerializers).forEach((key) => {
        const s = customSerializers[key];
        if (!s.deserialize) {
          return;
        }
        s.reader = this;
        this._deserializers[key] = s;
      });
    }
  }

  /**
   * Gets the default deserializers as an associative array.
   */
  getDefaultDeserializers() {
    return graphSON2Deserializers;
  }

  read(obj: any): any {
    if (obj === undefined) {
      return undefined;
    }
    if (obj === null) {
      return null;
    }
    if (Array.isArray(obj)) {
      return obj.map((item: any) => this.read(item));
    }
    const type = obj[typeKey];
    if (type) {
      const d = this._deserializers[type];
      if (d) {
        // Use type serializer
        return d.deserialize(obj);
      }
      return obj[valueKey];
    }
    if (obj && typeof obj === 'object' && obj.constructor === Object) {
      return this._deserializeObject(obj);
    }
    // Default (for boolean, number and other scalars)
    return obj;
  }

  readResponse(buffer: Buffer) {
    return this.read(JSON.parse(buffer.toString()));
  }

  _deserializeObject<T extends Record<any, any>>(obj: T) {
    const keys = Object.keys(obj);
    const result = {};
    for (let i = 0; i < keys.length; i++) {
      // @ts-expect-error
      result[keys[i]] = this.read(obj[keys[i]]);
    }
    return result;
  }
}

/**
 * GraphSON3 reader.
 */
export class GraphSON3Reader extends GraphSON2Reader {
  getDefaultDeserializers() {
    return graphSON3Deserializers;
  }
}

const graphSON2Deserializers = {
  'g:Traverser': TraverserSerializer,
  'g:TraversalStrategy': TraversalStrategySerializer,
  'g:Int32': NumberSerializer,
  'g:Int64': NumberSerializer,
  'g:Float': NumberSerializer,
  'g:Double': NumberSerializer,
  'g:Date': DateSerializer,
  'g:Direction': DirectionSerializer,
  'g:Vertex': VertexSerializer,
  'g:Edge': EdgeSerializer,
  'g:VertexProperty': VertexPropertySerializer,
  'g:Property': PropertySerializer,
  'g:Path': Path3Serializer,
  'g:TextP': TextPSerializer,
  'g:T': TSerializer,
  'g:BulkSet': BulkSetSerializer,
};

const graphSON3Deserializers = Object.assign({}, graphSON2Deserializers, {
  'g:List': ListSerializer,
  'g:Set': SetSerializer,
  'g:Map': MapSerializer,
});

const graphSON2Serializers = [
  NumberSerializer,
  DateSerializer,
  BytecodeSerializer,
  TraverserSerializer,
  TraversalStrategySerializer,
  PSerializer,
  TextPSerializer,
  LambdaSerializer,
  EnumSerializer,
  VertexSerializer,
  EdgeSerializer,
  LongSerializer,
];

// @ts-expect-error
const graphSON3Serializers = graphSON2Serializers.concat([ListSerializer, SetSerializer, MapSerializer]);

export const GraphSONWriter = GraphSON3Writer;

export const GraphSONReader = GraphSON3Reader;
