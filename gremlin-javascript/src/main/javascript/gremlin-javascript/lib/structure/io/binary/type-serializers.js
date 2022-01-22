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
 * @author Igor Ostapenko
 */
'use strict';

const Bytecode = require('../../../process/bytecode');
const DataType = require('./data-type');
const INT32_MIN = -2147483648;
const INT32_MAX = 2147483647;

class AnySerializer {

  static getSerializerCanBeUsedFor(item) {
    // TODO: align with Java.parse(GraphSON) logic
    const serializers = [ // specifically ordered, the first canBeUsedFor=true wins
      IntSerializer,
      BytecodeSerializer,
      MapSerializer,
      UuidSerializer,
      StringSerializer,
    ];
    for (let i = 0; i < serializers.length; i++)
      if (serializers[i].canBeUsedFor(item))
        return serializers[i];

    return StringSerializer; // TODO: is it what we want with falling back to a string?
  }

  static serialize(item, fullyQualifiedFormat = true) {
    return AnySerializer
      .getSerializerCanBeUsedFor(item)
      .serialize(item, fullyQualifiedFormat);
  }

}

class IntSerializer {

  static canBeUsedFor(value) {
    if (typeof value !== 'number')
      return false;
    if (value < INT32_MIN || value > INT32_MAX)
      return false;
    return true;
  }

  static serialize(item, fullyQualifiedFormat = true) {
    if (item === undefined || item === null)
      if (fullyQualifiedFormat)
        return Buffer.from([DataType.INT, 0x01]);
      else
        return Buffer.from([0x00, 0x00, 0x00, 0x00]);

    // TODO: this and other serializers could be optimised, e.g. to allocate a buf once, instead of multiple bufs to concat, etc
    const bufs = [];
    if (fullyQualifiedFormat)
      bufs.push( Buffer.from([DataType.INT, 0x00]) );
    const v = Buffer.alloc(4);
    v.writeInt32BE(item); // TODO: what if item is not within int32 limits, for now writeInt32BE would error
    bufs.push(v);

    return Buffer.concat(bufs);
  }

  static deserialize(buffer) {
    // TODO
  }

}

class StringSerializer {

  static canBeUsedFor(value) {
    return (typeof value === 'string');
  }

  static serialize(item, fullyQualifiedFormat = true) {
    if (item === undefined || item === null)
      if (fullyQualifiedFormat)
        return Buffer.from([DataType.STRING, 0x01]);
      else
        return IntSerializer.serialize(0, false);

    const bufs = [];
    if (fullyQualifiedFormat)
      bufs.push( Buffer.from([DataType.STRING, 0x00]) );
    const v = Buffer.from(String(item), 'utf8');
    bufs.push( IntSerializer.serialize(v.length, false) ); // TODO: what if len > INT32_MAX, for now it's backed by logic of IntSerializer.serialize itself
    bufs.push( v );

    return Buffer.concat(bufs);
  }

  static deserialize(buffer) {
    // TODO
  }

}

class MapSerializer {

  static canBeUsedFor(value) {
    return (typeof value === 'object');
  }

  static serialize(item, fullyQualifiedFormat = true) {
    if (item === undefined || item === null)
      if (fullyQualifiedFormat)
        return Buffer.from([DataType.MAP, 0x01]);
      else
        return IntSerializer.serialize(0, false);

    const keys = Object.keys(item);
    let map_length = keys.length;
    if (map_length < 0)
      map_length = 0;
    else if (map_length > INT32_MAX)
      map_length = INT32_MAX; // TODO: is it expected to be silenced?

    const bufs = [];
    if (fullyQualifiedFormat)
      bufs.push( Buffer.from([DataType.MAP, 0x00]) );
    bufs.push( IntSerializer.serialize(map_length, false) );
    for (let i = 0; i < map_length; i++) {
      const key = keys[i];
      const value = item[key];
      bufs.push( AnySerializer.serialize(key), AnySerializer.serialize(value) );
    }
    return Buffer.concat(bufs);
  }

  static deserialize(buffer) {
    // TODO
  }

}

class UuidSerializer {

  static canBeUsedFor(value) {
    // TODO
  }

  static serialize(item, fullyQualifiedFormat = true) {
    if (item === undefined || item === null)
      if (fullyQualifiedFormat)
        return Buffer.from([DataType.UUID, 0x01]);
      else
        return Buffer.from([0x00,0x00,0x00,0x00, 0x00,0x00,0x00,0x00, 0x00,0x00,0x00,0x00, 0x00,0x00,0x00,0x00]);

    const uuid_str = String(item)
      .replace(/^urn:uuid:/, '')
      .replace(/[{}-]/g, '');

    const bufs = [];
    if (fullyQualifiedFormat)
      bufs.push( Buffer.from([DataType.UUID, 0x00]) );

    const v = Buffer.alloc(16, 0);
    for (let i = 0; i < 16 && i*2 < uuid_str.length; i++)
      v[i] = parseInt(uuid_str.slice(i*2, i*2+2), 16);
    bufs.push(v);

    return Buffer.concat(bufs);
  }

  static deserialize(buffer, fullyQualifiedFormat=true, nullable=false) {
    try {
      if (buffer === undefined || buffer === null || !(buffer instanceof Buffer))
        throw new Error('buffer is missing');
      if (buffer.length < 1)
        throw new Error('buffer is empty');

      let len = 0;
      let cursor = buffer;

      if (fullyQualifiedFormat) {
        const type_code = cursor.readUInt8(); len++; cursor = cursor.slice(1);
        if (type_code !== DataType.UUID)
          throw new Error('unexpected type code');
      }
      if (fullyQualifiedFormat || nullable) {
        if (cursor.length < 1)
          throw new Error('value flag is missing');
        const value_flag = cursor.readUInt8(); len++; cursor = cursor.slice(1);
        if (value_flag === 1)
          return { v: null, len };
        if (value_flag !== 0)
          throw new Error('unexpected value flag');
      }

      if (cursor.length < 16)
        throw new Error('unexpected value length');
      len += 16;

      // Example: 2075278D-F624-4B2B-960D-25D374D57C04
      const v =
          cursor.slice(0, 4).toString('hex')
        + '-'
        + cursor.slice(4, 6).toString('hex')
        + '-'
        + cursor.slice(6, 8).toString('hex')
        + '-'
        + cursor.slice(8, 10).toString('hex')
        + '-'
        + cursor.slice(10, 16).toString('hex')
      ;
      return { v, len };
    }
    catch (e) {
      throw des_error({ des: this.name, args: arguments, msg: e.message });
    }
  }

}

class BytecodeSerializer {

  static canBeUsedFor(value) {
    return (value instanceof Bytecode);
  }

  static serialize(item, fullyQualifiedFormat = true) {
    if (item === undefined || item === null)
      if (fullyQualifiedFormat)
        return Buffer.from([DataType.BYTECODE, 0x01]);
      else
        return Buffer.from([0x00,0x00,0x00,0x00, 0x00,0x00,0x00,0x00]); // {steps_length} = 0, {sources_length} = 0

    const bufs = [];
    if (fullyQualifiedFormat)
      bufs.push( Buffer.from([DataType.BYTECODE, 0x00]) );

    // {steps_length}{step_0}...{step_n}
    bufs.push( IntSerializer.serialize(item.stepInstructions.length, false) ); // TODO: what if steps_length > INT32_MAX
    for (let i = 0; i < item.stepInstructions.length; i++) {
      // {step_i} is composed of {name}{values_length}{value_0}...{value_n}
      const step = item.stepInstructions[i];
      const name = step[0];
      const values_length = step.length - 1;
      bufs.push( StringSerializer.serialize(name, false) );
      bufs.push( IntSerializer.serialize(values_length, false) );
      for (let j = 0; j < values_length; j++)
        bufs.push( AnySerializer.serialize(step[1 + j], true) );
    }

    // {sources_length}{source_0}...{source_n}
    bufs.push( IntSerializer.serialize(item.sourceInstructions.length, false) ); // TODO: what if sources_length > INT32_MAX
    for (let i = 0; i < item.sourceInstructions.length; i++) {
      // {source_i} is composed of {name}{values_length}{value_0}...{value_n}
      const source = item.sourceInstructions[i];
      const name = source[0];
      const values_length = source.length - 1;
      bufs.push( StringSerializer.serialize(name, false) );
      bufs.push( IntSerializer.serialize(values_length, false) );
      for (let j = 0; j < values_length; j++)
        bufs.push( AnySerializer(source[1 + j], true) );
    }

    return Buffer.concat(bufs);
  }

  static deserialize(buffer) {
    // TODO
  }

}

const des_error = ({ des, args, msg }) => {
  let buffer = args[0];
  let buffer_tail = '';
  if (buffer instanceof Buffer) {
    if (buffer.length > 32)
      buffer_tail = '...';
    buffer = buffer.slice(0, 32).toString('hex');
  }
  const fullyQualifiedFormat = args[1];
  const nullable = args[2];

  return new Error(`${des}.deserialize(buffer=${buffer}${buffer_tail}, fullyQualifiedFormat=${fullyQualifiedFormat}, nullable=${nullable}): ${msg}.`);
};

module.exports = {
  AnySerializer,

  IntSerializer,
  StringSerializer,
  MapSerializer,
  UuidSerializer,
  BytecodeSerializer,
};
