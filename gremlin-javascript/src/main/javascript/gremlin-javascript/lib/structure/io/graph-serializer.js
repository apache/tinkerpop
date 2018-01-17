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

const typeSerializers = require('./type-serializers');

/**
 * GraphSON Writer
 */
class GraphSONWriter {
  /**
   * @param {Object} [options]
   * @param {Object} options.serializers An object used as an associative array with GraphSON 2 type name as keys and
   * serializer instances as values, ie: { 'g:Int64': longSerializer }.
   * @constructor
   */
  constructor(options) {
    this._options = options || {};
    // Create instance of the default serializers
    this._serializers = serializers.map(serializerConstructor => {
      const s = new serializerConstructor();
      s.writer = this;
      return s;
    });
    const customSerializers = this._options.serializers || {};
    Object.keys(customSerializers).forEach(key => {
      const s = customSerializers[key];
      if (!s.serialize) {
        return;
      }
      s.writer = this;
      // Insert custom serializers first
      this._serializers.unshift(s);
    });
  }

  adaptObject(value) {
    let s;
    if (Array.isArray(value)) {
      return value.map(item => this.adaptObject(item));
    }
    for (let i = 0; i < this._serializers.length; i++) {
      const currentSerializer = this._serializers[i];
      if (currentSerializer.canBeUsedFor && currentSerializer.canBeUsedFor(value)) {
        s = currentSerializer;
        break;
      }
    }
    if (s) {
      return s.serialize(value);
    }
    // Default (strings / objects / ...)
    return value;
  }

  /**
   * Returns the GraphSON representation of the provided object instance.
   * @param {Object} obj
   * @returns {String}
   */
  write(obj) {
    return JSON.stringify(this.adaptObject(obj));
  }
}

class GraphSONReader {
  /**
   * GraphSON Reader
   * @param {Object} [options]
   * @param {Object} [options.serializers] An object used as an associative array with GraphSON 2 type name as keys and
   * deserializer instances as values, ie: { 'g:Int64': longSerializer }.
   * @constructor
   */
  constructor(options) {
    this._options = options || {};
    this._deserializers = {};
    Object.keys(deserializers).forEach(typeName => {
      const serializerConstructor = deserializers[typeName];
      const s = new serializerConstructor();
      s.reader = this;
      this._deserializers[typeName] = s;
    });
    if (this._options.serializers) {
      const customSerializers = this._options.serializers || {};
      Object.keys(customSerializers).forEach(key => {
        const s = customSerializers[key];
        if (!s.deserialize) {
          return;
        }
        s.reader = this;
        this._deserializers[key] = s;
      });
    }
  }

  read(obj) {
    if (obj === undefined) {
      return undefined;
    }
    if (obj === null) {
      return null;
    }
    if (Array.isArray(obj)) {
      return obj.map(item => this.read(item));
    }
    const type = obj[typeSerializers.typeKey];
    if (type) {
      const d = this._deserializers[type];
      if (d) {
        // Use type serializer
        return d.deserialize(obj);
      }
      return obj[typeSerializers.valueKey];
    }
    if (obj && typeof obj === 'object' && obj.constructor === Object) {
      return this._deserializeObject(obj);
    }
    // Default (for boolean, number and other scalars)
    return obj;
  }

  _deserializeObject(obj) {
    const keys = Object.keys(obj);
    const result = {};
    for (let i = 0; i < keys.length; i++) {
      result[keys[i]] = this.read(obj[keys[i]]);
    }
    return result;
  }
}

const deserializers = {
  'g:Traverser': typeSerializers.TraverserSerializer,
  'g:Int32':  typeSerializers.NumberSerializer,
  'g:Int64':  typeSerializers.NumberSerializer,
  'g:Float':  typeSerializers.NumberSerializer,
  'g:Double': typeSerializers.NumberSerializer,
  'g:Vertex': typeSerializers.VertexSerializer,
  'g:Edge': typeSerializers.EdgeSerializer,
  'g:VertexProperty': typeSerializers.VertexPropertySerializer,
  'g:Property': typeSerializers.PropertySerializer,
  'g:Path': typeSerializers.PathSerializer,
  'g:T': typeSerializers.TSerializer
};

const serializers = [
  typeSerializers.NumberSerializer,
  typeSerializers.BytecodeSerializer,
  typeSerializers.TraverserSerializer,
  typeSerializers.PSerializer,
  typeSerializers.LambdaSerializer,
  typeSerializers.EnumSerializer,
  typeSerializers.VertexSerializer,
  typeSerializers.EdgeSerializer,
  typeSerializers.LongSerializer
];

module.exports = {
  GraphSONWriter,
  GraphSONReader
};