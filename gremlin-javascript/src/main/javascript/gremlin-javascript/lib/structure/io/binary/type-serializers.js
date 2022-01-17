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

const DataType = require('./data-type');
const INT32_MIN = -2147483648;
const INT32_MAX = 2147483647;

class AnySerializer {

  static getSerializerCanBeUsedFor(item) {
    // TODO: align with Java.parse(GraphSON) logic
    const serializers = [ // specifically ordered, the first canBeUsedFor=true wins
      IntSerializer,
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
      .replaceAll(/[{}-]/g, '');

    const bufs = [];
    if (fullyQualifiedFormat)
      bufs.push( Buffer.from([DataType.UUID, 0x00]) );

    const v = Buffer.alloc(16, 0);
    for (let i = 0; i < 16 && i*2 < uuid_str.length; i++)
      v[i] = parseInt(uuid_str.slice(i*2, i*2+2), 16);
    bufs.push(v);

    return Buffer.concat(bufs);
  }

  static deserialize(buffer) {
    // TODO
  }

}

class BytecodeSerializer {

  static canBeUsedFor(value) {
    // TODO
  }

  static serialize(item, fullyQualifiedFormat = true) {
    // TODO
  }

  static deserialize(buffer) {
    // TODO
  }

}

module.exports = {
  AnySerializer,

  IntSerializer,
  StringSerializer,
  MapSerializer,
  UuidSerializer,
  BytecodeSerializer,
};
