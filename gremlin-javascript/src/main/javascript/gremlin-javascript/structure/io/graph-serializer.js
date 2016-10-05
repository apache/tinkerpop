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
(function graphSerializerModule() {
  "use strict";

  var t = loadModule.call(this, '../../process/traversal.js');
  var g = loadModule.call(this, '../graph.js');

  /**
   * A type serializer
   * @typedef {Object} Serializer
   * @property {Function} serialize
   * @property {Function} deserialize
   * @property {Function} [canBeUsedFor]
   */

  /**
   * @const
   * @private
   */
  var valueKey = '@value';

  /**
   * @const
   * @private
   */
  var typeKey = '@type';

  var deserializers = {
    'g:Traverser': TraverserSerializer,
    'g:Int32':  NumberSerializer,
    'g:Int64':  NumberSerializer,
    'g:Float':  NumberSerializer,
    'g:Double': NumberSerializer,
    'g:Vertex': VertexSerializer,
    'g:Edge': EdgeSerializer,
    'g:VertexProperty': VertexPropertySerializer,
    'g:Property': PropertySerializer,
    'g:Path': PathSerializer
  };

  var serializers = [
    NumberSerializer,
    BytecodeSerializer,
    TraverserSerializer,
    PSerializer,
    LambdaSerializer,
    EnumSerializer
  ];

  /**
   * GraphSON Writer
   * @param {Object} [options]
   * @param {Object} options.serializers An object used as an associative array with GraphSON 2 type name as keys and
   * serializer instances as values, ie: { 'g:Int64': longSerializer }.
   * @constructor
   */
  function GraphSONWriter(options) {
    this._options = options || {};
    // Create instance of the default serializers
    this._serializers = serializers.map(function (serializerConstructor) {
      var s = new serializerConstructor();
      s.writer = this;
      return s;
    }, this);
    var customSerializers = this._options.serializers || {};
    Object.keys(customSerializers).forEach(function (key) {
      var s = customSerializers[key];
      if (!s.serialize) {
        return;
      }
      s.writer = this;
      // Insert custom serializers first
      this._serializers.unshift(s);
    }, this);
  }

  GraphSONWriter.prototype.adaptObject = function (value) {
    var s;
    if (Array.isArray(value)) {
      return value.map(function (item) {
        return this.adaptObject(item);
      }, this);
    }
    for (var i = 0; i < this._serializers.length; i++) {
      var currentSerializer = this._serializers[i];
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
  };

  /**
   * Returns the GraphSON representation of the provided object instance.
   * @param {Object} obj
   * @returns {String}
   */
  GraphSONWriter.prototype.write = function (obj) {
    return JSON.stringify(this.adaptObject(obj));
  };

  /**
   * GraphSON Reader
   * @param {Object} [options]
   * @param {Object} [options.serializers] An object used as an associative array with GraphSON 2 type name as keys and
   * deserializer instances as values, ie: { 'g:Int64': longSerializer }.
   * @constructor
   */
  function GraphSONReader(options) {
    this._options = options || {};
    this._deserializers = {};
    Object.keys(deserializers).forEach(function (typeName) {
      var serializerConstructor = deserializers[typeName];
      var s = new serializerConstructor();
      s.reader = this;
      this._deserializers[typeName] = s;
    }, this);
    if (this._options.serializers) {
      var customSerializers = this._options.serializers || {};
      Object.keys(customSerializers).forEach(function (key) {
        var s = customSerializers[key];
        if (!s.deserialize) {
          return;
        }
        s.reader = this;
        this._deserializers[key] = s;
      }, this);
    }
  }

  GraphSONReader.prototype.read = function (obj) {
    if (Array.isArray(obj)) {
      return obj.map(function mapEach(item) {
        return this.read(item);
      }, this);
    }
    var type = obj[typeKey];
    if (type) {
      var d = this._deserializers[type];
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
  };

  GraphSONReader.prototype._deserializeObject = function (obj) {
    var keys = Object.keys(obj);
    var result = {};
    for (var i = 0; i < keys.length; i++) {
      result[keys[i]] = this.read(obj[keys[i]]);
    }
    return result;
  };

  function NumberSerializer() {

  }

  NumberSerializer.prototype.serialize = function (item) {
    return item;
  };

  NumberSerializer.prototype.deserialize = function (obj) {
    var value = obj[valueKey];
    return parseFloat(value);
  };

  NumberSerializer.prototype.canBeUsedFor = function (value) {
    return (typeof value === 'number');
  };

  function BytecodeSerializer() {

  }

  BytecodeSerializer.prototype.serialize = function (item) {
    var bytecode = item;
    if (item instanceof t.Traversal) {
      bytecode = item.getBytecode();
    }
    var result = {};
    result[typeKey] = 'g:Bytecode';
    var resultValue = result[valueKey] = {};
    var sources = this._serializeInstructions(bytecode.sourceInstructions);
    if (sources) {
      resultValue['source'] = sources;
    }
    var steps = this._serializeInstructions(bytecode.stepInstructions);
    if (steps) {
      resultValue['step'] = steps;
    }
    return result;
  };

  BytecodeSerializer.prototype._serializeInstructions = function (instructions) {
    if (instructions.length === 0) {
      return null;
    }
    var result = new Array(instructions.length);
    result[0] = instructions[0];
    for (var i = 1; i < instructions.length; i++) {
      result[i] = this.writer.adaptObject(instructions[i]);
    }
    return result;
  };

  BytecodeSerializer.prototype.canBeUsedFor = function (value) {
    return (value instanceof t.Bytecode) || (value instanceof t.Traversal);
  };

  function PSerializer() {

  }

  /** @param {P} item */
  PSerializer.prototype.serialize = function (item) {
    var result = {};
    result[typeKey] = 'g:P';
    var resultValue = result[valueKey] = {
      'predicate': item.operator
    };
    if (item.other == undefined) {
      resultValue['value'] = this.writer.adaptObject(item.value);
    }
    else {
      resultValue['value'] = [ this.writer.adaptObject(item.value), this.writer.adaptObject(item.other) ];
    }
    return result;
  };

  PSerializer.prototype.canBeUsedFor = function (value) {
    return (value instanceof t.P);
  };

  function LambdaSerializer() {

  }

  /** @param {Function} item */
  LambdaSerializer.prototype.serialize = function (item) {
    var result = {};
    result[typeKey] = 'g:Lambda';
    result[valueKey] = {
      'arguments': item.length,
      'language': 'gremlin-javascript',
      'script': item.toString()
    };
    return result;
  };

  LambdaSerializer.prototype.canBeUsedFor = function (value) {
    return (typeof value === 'function');
  };

  function EnumSerializer() {

  }

  /** @param {EnumValue} item */
  EnumSerializer.prototype.serialize = function (item) {
    var result = {};
    result[typeKey] = 'g:' + item.typeName;
    result[valueKey] = item.elementName;
    return result;
  };

  EnumSerializer.prototype.canBeUsedFor = function (value) {
    return value && value.typeName && value instanceof t.EnumValue;
  };

  function TraverserSerializer() {

  }

  /** @param {Traverser} item */
  TraverserSerializer.prototype.serialize = function (item) {
    var result = {};
    result[typeKey] = 'g:Traverser';
    result[valueKey] = {
      'value': this.writer.adaptObject(item.object),
      'bulk': this.writer.adaptObject(item.bulk)
    };
    return result;
  };

  TraverserSerializer.prototype.deserialize = function (obj) {
    var value = obj[valueKey];
    return new t.Traverser(this.reader.read(value['value']), this.reader.read(value['bulk']));
  };

  TraverserSerializer.prototype.canBeUsedFor = function (value) {
    return (value instanceof t.Traverser);
  };

  function VertexSerializer() {

  }

  VertexSerializer.prototype.deserialize = function (obj) {
    var value = obj[valueKey];
    return new g.Vertex(this.reader.read(value['id']), value['label'], this.reader.read(value['properties']));
  };

  function VertexPropertySerializer() {

  }

  VertexPropertySerializer.prototype.deserialize = function (obj) {
    var value = obj[valueKey];
    return new g.VertexProperty(this.reader.read(value['id']), value['label'], this.reader.read(value['value']));
  };

  function PropertySerializer() {

  }

  PropertySerializer.prototype.deserialize = function (obj) {
    var value = obj[valueKey];
    return new g.Property(
      value['key'],
      this.reader.read(value['value']));
  };

  function EdgeSerializer() {

  }

  EdgeSerializer.prototype.deserialize = function (obj) {
    var value = obj[valueKey];
    return new g.Edge(
      this.reader.read(value['id']),
      this.reader.read(value['outV']),
      value['label'],
      this.reader.read(value['inV'])
    );
  };

  function PathSerializer() {

  }

  PathSerializer.prototype.deserialize = function (obj) {
    var value = obj[valueKey];
    var objects = value['objects'].map(function objectMapItem(o) {
      return this.reader.read(o);
    }, this);
    return new g.Path(this.reader.read(value['labels']), objects);
  };

  function loadModule(moduleName) {
    if (typeof require !== 'undefined') {
      return require(moduleName);
    }
    if (typeof load !== 'undefined') {
      var path = new java.io.File(__DIR__ + moduleName).getCanonicalPath();
      this.__dependencies = this.__dependencies || {};
      return this.__dependencies[path] = (this.__dependencies[path] || load(path));
    }
    throw new Error('No module loader was found');
  }

  var toExport = {
    GraphSONWriter: GraphSONWriter,
    GraphSONReader: GraphSONReader
  };
  if (typeof module !== 'undefined') {
    // CommonJS
    module.exports = toExport;
    return;
  }
  // Nashorn and rest
  return toExport;
}).call(this);