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
 * GraphSON2 writer.
 */
class GraphSON2Writer {

  /**
   * @param {Object} [options]
   * @param {Object} [options.serializers] An object used as an associative array with GraphSON 2 type name as keys and
   * serializer instances as values, ie: { 'g:Int64': longSerializer }.
   * @constructor
   */
  constructor(options) {
    this._options = options || {};
    // Create instance of the default serializers
    this._serializers = this.getDefaultSerializers().map(serializerConstructor => {
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

  /**
   * Gets the default serializers to be used.
   * @returns {Array}
   */
  getDefaultSerializers() {
    return graphSON2Serializers;
  }

  adaptObject(value) {
    let s;

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

    if (Array.isArray(value)) {
      // We need to handle arrays when there is no serializer
      // for older versions of GraphSON
      return value.map(item => this.adaptObject(item));
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

/**
 * GraphSON3 writer.
 */
class GraphSON3Writer extends GraphSON2Writer {
  getDefaultSerializers() {
    return graphSON3Serializers;
  }
}

/**
 * GraphSON2 reader.
 */
class GraphSON2Reader {
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

    const defaultDeserializers = this.getDefaultDeserializers();
    Object.keys(defaultDeserializers).forEach(typeName => {
      const serializerConstructor = defaultDeserializers[typeName];
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

  /**
   * Gets the default deserializers as an associative array.
   * @returns {Object}
   */
  getDefaultDeserializers() {
    return graphSON2Deserializers;
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

/**
 * GraphSON3 reader.
 */
class GraphSON3Reader extends GraphSON2Reader {
  getDefaultDeserializers() {
    return graphSON3Deserializers;
  }
}

const graphSON2Deserializers = {
  'g:Traverser': typeSerializers.TraverserSerializer,
  'g:TraversalStrategy': typeSerializers.TraversalStrategySerializer,
  'g:Int32':  typeSerializers.NumberSerializer,
  'g:Int64':  typeSerializers.NumberSerializer,
  'g:Float':  typeSerializers.NumberSerializer,
  'g:Double': typeSerializers.NumberSerializer,
  'g:Date': typeSerializers.DateSerializer,
  'g:Direction': typeSerializers.DirectionSerializer,
  'g:Vertex': typeSerializers.VertexSerializer,
  'g:Edge': typeSerializers.EdgeSerializer,
  'g:VertexProperty': typeSerializers.VertexPropertySerializer,
  'g:Property': typeSerializers.PropertySerializer,
  'g:Path': typeSerializers.Path3Serializer,
  'g:TextP': typeSerializers.TextPSerializer,
  'g:T': typeSerializers.TSerializer,
  'g:BulkSet': typeSerializers.BulkSetSerializer
};

const graphSON3Deserializers = Object.assign({}, graphSON2Deserializers, {
  'g:List': typeSerializers.ListSerializer,
  'g:Set': typeSerializers.SetSerializer,
  'g:Map': typeSerializers.MapSerializer
});

const graphSON2Serializers = [
  typeSerializers.NumberSerializer,
  typeSerializers.DateSerializer,
  typeSerializers.BytecodeSerializer,
  typeSerializers.TraverserSerializer,
  typeSerializers.TraversalStrategySerializer,
  typeSerializers.PSerializer,
  typeSerializers.TextPSerializer,
  typeSerializers.LambdaSerializer,
  typeSerializers.EnumSerializer,
  typeSerializers.VertexSerializer,
  typeSerializers.EdgeSerializer,
  typeSerializers.LongSerializer
];

const graphSON3Serializers = graphSON2Serializers.concat([
  typeSerializers.ListSerializer,
  typeSerializers.SetSerializer,
  typeSerializers.MapSerializer
]);

module.exports = {
  GraphSON3Writer,
  GraphSON3Reader,
  GraphSON2Writer,
  GraphSON2Reader,
  GraphSONWriter: GraphSON3Writer,
  GraphSONReader: GraphSON3Reader
};