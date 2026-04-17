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

export default class LongSerializer {
  constructor(ioc) {
    this.ioc = ioc;
    this.ioc.serializers[ioc.DataType.LONG] = this;
  }

  canBeUsedFor() {
    // it's not expected to be used, see NumberSerializationStrategy
  }

  serialize(item, fullyQualifiedFormat = true) {
    if (item === undefined || item === null) {
      if (fullyQualifiedFormat) {
        return Buffer.from([this.ioc.DataType.LONG, 0x01]);
      }
      return Buffer.from([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
    }

    const bufs = [];
    if (fullyQualifiedFormat) {
      bufs.push(Buffer.from([this.ioc.DataType.LONG, 0x00]));
    }
    const v = Buffer.alloc(8);
    v.writeBigInt64BE(BigInt(item));
    bufs.push(v);

    return Buffer.concat(bufs);
  }

  /**
   * @param {StreamReader} reader
   * @param {number} valueFlag - already consumed by AnySerializer
   * @param {number} typeCode
   * @returns {Promise<number|bigint>}
   */
  async deserializeValue(reader, valueFlag, typeCode) {
    let v = await reader.readBigInt64BE();
    if (v >= Number.MIN_SAFE_INTEGER && v <= Number.MAX_SAFE_INTEGER) {
      v = Number(v);
    }
    return v;
  }

  /**
   * Read a fully-qualified long from the StreamReader.
   * @param {StreamReader} reader
   * @returns {Promise<number|bigint|null>}
   */
  async deserialize(reader) {
    const type_code = await reader.readUInt8();
    if (type_code !== this.ioc.DataType.LONG) {
      throw new Error(`LongSerializer: unexpected {type_code}=0x${type_code.toString(16)}`);
    }
    const value_flag = await reader.readUInt8();
    if (value_flag === 0x01) {
      return null;
    }
    if (value_flag !== 0x00) {
      throw new Error(`LongSerializer: unexpected {value_flag}=0x${value_flag.toString(16)}`);
    }
    return this.deserializeValue(reader, value_flag, type_code);
  }
}
