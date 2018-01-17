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

const t = require('../../process/traversal');
const Bytecode = require('../../process/bytecode');
const g = require('../graph');
const utils = require('../../utils');

const valueKey = '@value';
const typeKey = '@type';

/**
 * @abstract
 */
class TypeSerializer {
  serialize() {
    throw new Error('serialize() method not implemented for ' + this.constructor.name);
  }

  deserialize() {
    throw new Error('deserialize() method not implemented for ' + this.constructor.name);
  }

  canBeUsedFor() {
    throw new Error('canBeUsedFor() method not implemented for ' + this.constructor.name);
  }
}

class NumberSerializer extends TypeSerializer {
  serialize(item) {
    return item;
  }

  deserialize(obj) {
    return parseFloat(obj[valueKey]);
  }

  canBeUsedFor(value) {
    return (typeof value === 'number');
  }
}

class LongSerializer extends TypeSerializer {
  serialize(item) {
    return {
      [typeKey]: 'g:Int64',
      [valueKey]: item.value
    };
  }

  canBeUsedFor(value) {
    return (value instanceof utils.Long);
  }
}

class BytecodeSerializer extends TypeSerializer {
  serialize(item) {
    let bytecode = item;
    if (item instanceof t.Traversal) {
      bytecode = item.getBytecode();
    }
    const result = {};
    result[typeKey] = 'g:Bytecode';
    const resultValue = result[valueKey] = {};
    const sources = this._serializeInstructions(bytecode.sourceInstructions);
    if (sources) {
      resultValue['source'] = sources;
    }
    const steps = this._serializeInstructions(bytecode.stepInstructions);
    if (steps) {
      resultValue['step'] = steps;
    }
    return result;
  }

  _serializeInstructions(instructions) {
    if (instructions.length === 0) {
      return null;
    }
    const result = new Array(instructions.length);
    result[0] = instructions[0];
    for (let i = 0; i < instructions.length; i++) {
      result[i] = this.writer.adaptObject(instructions[i]);
    }
    return result;
  }

  canBeUsedFor(value) {
    return (value instanceof Bytecode) || (value instanceof t.Traversal);
  }
}

class PSerializer extends TypeSerializer {
  /** @param {P} item */
  serialize(item) {
    const result = {};
    result[typeKey] = 'g:P';
    const resultValue = result[valueKey] = {
      'predicate': item.operator
    };
    if (item.other === undefined || item.other === null) {
      resultValue['value'] = this.writer.adaptObject(item.value);
    }
    else {
      resultValue['value'] = [ this.writer.adaptObject(item.value), this.writer.adaptObject(item.other) ];
    }
    return result;
  }

  canBeUsedFor(value) {
    return (value instanceof t.P);
  }
}

class LambdaSerializer extends TypeSerializer {
  /** @param {Function} item */
  serialize(item) {
    return {
      [typeKey]: 'g:Lambda',
      [valueKey]: {
        'arguments': item.length,
        'language': 'gremlin-javascript',
        'script': item.toString()
      }
    };
  }

  canBeUsedFor(value) {
    return (typeof value === 'function');
  }
}

class EnumSerializer extends TypeSerializer {
  /** @param {EnumValue} item */
  serialize(item) {
    return {
      [typeKey]: 'g:' + item.typeName,
      [valueKey]: item.elementName
    };
  }

  canBeUsedFor(value) {
    return value && value.typeName && value instanceof t.EnumValue;
  }
}

class TraverserSerializer extends TypeSerializer {
  /** @param {Traverser} item */
  serialize(item) {
    return {
      [typeKey]: 'g:Traverser',
      [valueKey]: {
        'value': this.writer.adaptObject(item.object),
        'bulk': this.writer.adaptObject(item.bulk)
      }
    };
  }

  deserialize(obj) {
    const value = obj[valueKey];
    return new t.Traverser(this.reader.read(value['value']), this.reader.read(value['bulk']));
  }

  canBeUsedFor(value) {
    return (value instanceof t.Traverser);
  }
}

class VertexSerializer extends TypeSerializer {
  deserialize(obj) {
    const value = obj[valueKey];
    return new g.Vertex(this.reader.read(value['id']), value['label'], this.reader.read(value['properties']));
  }

  /** @param {Vertex} item */
  serialize(item) {
    return {
      [typeKey]: 'g:Vertex',
      [valueKey]: {
        'id': this.writer.adaptObject(item.id),
        'label': item.label
      }
    };
  }

  canBeUsedFor(value) {
    return (value instanceof g.Vertex);
  }
}

class VertexPropertySerializer extends TypeSerializer {
  deserialize(obj) {
    const value = obj[valueKey];
    return new g.VertexProperty(
      this.reader.read(value['id']),
      value['label'],
      this.reader.read(value['value']),
      this.reader.read(value['properties'])
    );
  }
}

class PropertySerializer extends TypeSerializer {
  deserialize(obj) {
    const value = obj[valueKey];
    return new g.Property(
      value['key'],
      this.reader.read(value['value']));
  }
}

class EdgeSerializer extends TypeSerializer {
  deserialize(obj) {
    const value = obj[valueKey];
    return new g.Edge(
      this.reader.read(value['id']),
      this.reader.read(value['outV']),
      value['label'],
      this.reader.read(value['inV']),
      this.reader.read(value['properties'])
    );
  }

  /** @param {Edge} item */
  serialize(item) {
    return {
      [typeKey]: 'g:Edge',
      [valueKey]: {
        'id': this.writer.adaptObject(item.id),
        'label': item.label,
        'outV': this.writer.adaptObject(item.outV.id),
        'outVLabel': item.outV.label,
        'inV': this.writer.adaptObject(item.inV.id),
        'inVLabel': item.inV.label
      }
    };
  }

  canBeUsedFor(value) {
    return (value instanceof g.Edge);
  }
}

class PathSerializer extends TypeSerializer {
  deserialize(obj) {
    const value = obj[valueKey];
    const objects = value['objects'].map(o => this.reader.read(o));
    return new g.Path(this.reader.read(value['labels']), objects);
  }
}

class TSerializer extends TypeSerializer {
  deserialize(obj) {
    return t.t[obj[valueKey]];
  }
}

module.exports = {
  BytecodeSerializer,
  EdgeSerializer,
  EnumSerializer,
  LambdaSerializer,
  LongSerializer,
  NumberSerializer,
  PathSerializer,
  PropertySerializer,
  PSerializer,
  TSerializer,
  TraverserSerializer,
  typeKey,
  valueKey,
  VertexPropertySerializer,
  VertexSerializer
};