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

export default class BinarySerializer {
  constructor(ioc) {
    this.ioc = ioc;
    this.ioc.serializers[ioc.DataType.BINARY] = this;
  }

  canBeUsedFor(value) {
    return value instanceof Buffer;
  }

  serialize(item, fullyQualifiedFormat = true) {
    if (item === undefined || item === null) {
      if (fullyQualifiedFormat) {
        return Buffer.from([this.ioc.DataType.BINARY, 0x01]);
      }
      return Buffer.from([0x00, 0x00, 0x00, 0x00]); // {length} = 0
    }

    const bufs = [];
    if (fullyQualifiedFormat) {
      bufs.push(Buffer.from([this.ioc.DataType.BINARY, 0x00]));
    }

    // {length}
    let length = item.length;
    if (length < 0) {
      length = 0; // TODO: test it
    }
    if (length > this.ioc.intSerializer.INT32_MAX) {
      throw new Error(
        `Buffer length=${length} is greater than supported max_length=${this.ioc.intSerializer.INT32_MAX}.`,
      );
    }
    bufs.push(this.ioc.intSerializer.serialize(length, false));

    // {value} sequence of bytes
    bufs.push(item);

    return Buffer.concat(bufs);
  }

  /**
   * @param {StreamReader} reader
   * @param {number} valueFlag
   * @param {number} typeCode
   * @returns {Promise<Buffer>}
   */
  async deserializeValue(reader, valueFlag, typeCode) {
    const length = await this.ioc.intSerializer.deserializeBare(reader);
    if (length < 0) {
      throw new Error(`BinarySerializer: {length}=${length} is less than zero`);
    }
    if (length === 0) {
      return Buffer.alloc(0);
    }
    return reader.readBytes(length);
  }

  /**
   * @param {StreamReader} reader
   * @returns {Promise<Buffer|null>}
   */
  async deserialize(reader) {
    const type_code = await reader.readUInt8();
    if (type_code !== this.ioc.DataType.BINARY) {
      throw new Error(`BinarySerializer: unexpected {type_code}=0x${type_code.toString(16)}`);
    }
    const value_flag = await reader.readUInt8();
    if (value_flag === 0x01) {
      return null;
    }
    if (value_flag !== 0x00) {
      throw new Error(`BinarySerializer: unexpected {value_flag}=0x${value_flag.toString(16)}`);
    }
    return this.deserializeValue(reader, value_flag, type_code);
  }
}
