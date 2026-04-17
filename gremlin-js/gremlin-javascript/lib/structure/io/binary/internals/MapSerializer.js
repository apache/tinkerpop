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

import { Buffer } from 'buffer';

export default class MapSerializer {
  constructor(ioc) {
    this.ioc = ioc;
    this.ioc.serializers[ioc.DataType.MAP] = this;
  }

  canBeUsedFor(value) {
    if (value === null || value === undefined) {
      return false;
    }
    if (value instanceof Map) {
      return true;
    }
    if (Array.isArray(value)) {
      return false;
    }

    return typeof value === 'object';
  }

  serialize(item, fullyQualifiedFormat = true) {
    if (item === undefined || item === null) {
      if (fullyQualifiedFormat) {
        return Buffer.from([this.ioc.DataType.MAP, 0x01]);
      }
      return this.ioc.intSerializer.serialize(0, false);
    }

    const isMap = item instanceof Map;

    const keys = isMap ? Array.from(item.keys()) : Object.keys(item);
    let map_length = keys.length;
    if (map_length < 0) {
      map_length = 0;
    } else if (map_length > this.ioc.intSerializer.INT32_MAX) {
      map_length = this.ioc.intSerializer.INT32_MAX; // TODO: is it expected to be silenced? or it's better to error instead?
    }

    const bufs = [];
    if (fullyQualifiedFormat) {
      bufs.push(Buffer.from([this.ioc.DataType.MAP, 0x00]));
    }
    bufs.push(this.ioc.intSerializer.serialize(map_length, false));
    for (let i = 0; i < map_length; i++) {
      const key = keys[i];
      const value = isMap ? item.get(key) : item[key];
      bufs.push(this.ioc.anySerializer.serialize(key), this.ioc.anySerializer.serialize(value));
    }
    return Buffer.concat(bufs);
  }

  /**
   * Async deserialization of map value bytes from a StreamReader.
   * @param {StreamReader} reader
   * @param {number} valueFlag
   * @param {number} typeCode
   * @returns {Promise<Map>}
   */
  async deserializeValue(reader, valueFlag, typeCode) {
    const length = await this.ioc.intSerializer.deserializeBare(reader);
    if (length < 0) {
      throw new Error(`MapSerializer: {length}=${length} is less than zero`);
    }

    const v = new Map();
    for (let i = 0; i < length; i++) {
      const key = await this.ioc.anySerializer.deserialize(reader);
      const value = await this.ioc.anySerializer.deserialize(reader);
      v.set(key, value);
    }

    return v;
  }

  /**
   * Async fully-qualified deserialization from a StreamReader.
   * @param {StreamReader} reader
   * @returns {Promise<Map|null>}
   */
  async deserialize(reader) {
    const type_code = await reader.readUInt8();
    if (type_code !== this.ioc.DataType.MAP) {
      throw new Error(`MapSerializer: unexpected {type_code}=0x${type_code.toString(16)}`);
    }
    const value_flag = await reader.readUInt8();
    if (value_flag === 0x01) {
      return null;
    }
    if (value_flag !== 0x00) {
      throw new Error(`MapSerializer: unexpected {value_flag}=0x${value_flag.toString(16)}`);
    }
    return this.deserializeValue(reader, value_flag, type_code);
  }
}
