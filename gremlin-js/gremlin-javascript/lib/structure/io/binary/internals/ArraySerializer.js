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

export default class ArraySerializer {
  constructor(ioc, ID) {
    this.ioc = ioc;
    this.ID = ID;
    this.ioc.serializers[ID] = this;
  }

  canBeUsedFor(value) {
    return Array.isArray(value);
  }

  serialize(item, fullyQualifiedFormat = true) {
    if (item === undefined || item === null) {
      if (fullyQualifiedFormat) {
        return Buffer.from([this.ID, 0x01]);
      }
      return Buffer.from([0x00, 0x00, 0x00, 0x00]); // {length} = 0
    }

    const bufs = [];
    if (fullyQualifiedFormat) {
      bufs.push(Buffer.from([this.ID, 0x00]));
    }

    // {length}
    let length = item.length;
    if (length < 0) {
      length = 0; // TODO: test it
    }
    if (length > this.ioc.intSerializer.INT32_MAX) {
      throw new Error(
        `Array length=${length} is greater than supported max_length=${this.ioc.intSerializer.INT32_MAX}.`,
      );
    }
    bufs.push(this.ioc.intSerializer.serialize(length, false));

    // {item_0}...{item_n}
    for (let i = 0; i < length; i++) {
      bufs.push(this.ioc.anySerializer.serialize(item[i]));
    }

    return Buffer.concat(bufs);
  }

  /**
   * Async deserialization of array value bytes from a StreamReader.
   * @param {StreamReader} reader
   * @param {number} valueFlag - 0x00 for normal, 0x02 for bulked
   * @param {number} typeCode
   * @returns {Promise<Array>}
   */
  async deserializeValue(reader, valueFlag, typeCode) {
    const isBulked = valueFlag === 0x02;
    const length = await this.ioc.intSerializer.deserializeBare(reader);
    if (length < 0) {
      throw new Error(`ArraySerializer: {length}=${length} is less than zero`);
    }

    const v = [];
    for (let i = 0; i < length; i++) {
      const value = await this.ioc.anySerializer.deserialize(reader);

      if (isBulked) {
        const bulkCount = await reader.readBigInt64BE();
        for (let j = 0n; j < bulkCount; j++) {
          v.push(value);
        }
      } else {
        v.push(value);
      }
    }

    return v;
  }

  /**
   * Async fully-qualified deserialization from a StreamReader.
   * @param {StreamReader} reader
   * @returns {Promise<Array|null>}
   */
  async deserialize(reader) {
    const type_code = await reader.readUInt8();
    if (type_code !== this.ID) {
      throw new Error(`ArraySerializer: unexpected {type_code}=0x${type_code.toString(16)}`);
    }
    const value_flag = await reader.readUInt8();
    if (value_flag === 0x01) {
      return null;
    }
    if (value_flag !== 0x00 && value_flag !== 0x02) {
      throw new Error(`ArraySerializer: unexpected {value_flag}=0x${value_flag.toString(16)}`);
    }
    return this.deserializeValue(reader, value_flag, type_code);
  }
}
