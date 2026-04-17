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

export default class IntSerializer {
  get INT32_MIN() {
    return -2147483648;
  }
  get INT32_MAX() {
    return 2147483647;
  }

  constructor(ioc) {
    this.ioc = ioc;
    this.ioc.serializers[ioc.DataType.INT] = this;
  }

  canBeUsedFor(value) {
    if (typeof value !== 'number') {
      return false;
    }
    if (value < this.INT32_MIN || value > this.INT32_MAX) {
      return false;
    }
    return true;
  }

  serialize(item, fullyQualifiedFormat = true) {
    if (item === undefined || item === null) {
      if (fullyQualifiedFormat) {
        return Buffer.from([this.ioc.DataType.INT, 0x01]);
      }
      return Buffer.from([0x00, 0x00, 0x00, 0x00]);
    }

    const bufs = [];
    if (fullyQualifiedFormat) {
      bufs.push(Buffer.from([this.ioc.DataType.INT, 0x00]));
    }
    const v = Buffer.alloc(4);
    v.writeInt32BE(item); // if item is not within int32 limits writeInt32BE will error
    bufs.push(v);

    return Buffer.concat(bufs);
  }

  /**
   * Read a bare int32 value from the StreamReader (no type_code or value_flag).
   * @param {StreamReader} reader
   * @returns {Promise<number>}
   */
  async deserializeBare(reader) {
    return await reader.readInt32BE();
  }

  /**
   * @param {StreamReader} reader
   * @param {number} valueFlag - already consumed by AnySerializer
   * @param {number} typeCode
   * @returns {Promise<number>}
   */
  async deserializeValue(reader, valueFlag, typeCode) {
    return await reader.readInt32BE();
  }

  /**
   * Read a fully-qualified int from the StreamReader (type_code + value_flag + value).
   * @param {StreamReader} reader
   * @returns {Promise<number|null>}
   */
  async deserialize(reader) {
    const type_code = await reader.readUInt8();
    if (type_code !== this.ioc.DataType.INT) {
      throw new Error(`IntSerializer: unexpected {type_code}=0x${type_code.toString(16)}`);
    }
    const value_flag = await reader.readUInt8();
    if (value_flag === 0x01) {
      return null;
    }
    if (value_flag !== 0x00) {
      throw new Error(`IntSerializer: unexpected {value_flag}=0x${value_flag.toString(16)}`);
    }
    return this.deserializeValue(reader, value_flag, type_code);
  }
}
