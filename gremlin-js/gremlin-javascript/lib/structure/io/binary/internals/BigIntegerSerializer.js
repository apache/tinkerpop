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

// TODO: it has room for performance improvements
export default class BigIntegerSerializer {
  constructor(ioc) {
    this.ioc = ioc;
    this.ioc.serializers[ioc.DataType.BIGINTEGER] = this;
  }

  canBeUsedFor(value) {
    // it's not expected to be used directly, see NumberSerializationStrategy
    return typeof value === 'bigint';
  }

  serialize(item, fullyQualifiedFormat = true) {
    if (item === undefined || item === null) {
      if (fullyQualifiedFormat) {
        return Buffer.from([this.ioc.DataType.BIGINTEGER, 0x01]);
      }
      return Buffer.from([0x00, 0x00, 0x00, 0x01, 0x00]);
    }

    const bufs = [];
    if (fullyQualifiedFormat) {
      bufs.push(Buffer.from([this.ioc.DataType.BIGINTEGER, 0x00]));
    }

    let v;

    // positive number
    if (item >= 0) {
      let hex_str = item.toString(16);
      if (hex_str.length % 2 !== 0) {
        hex_str = '0' + hex_str; // Buffer.from() expects exactly two hex digits per byte
      }
      if (Number.parseInt(hex_str[0], 16) > 7) {
        hex_str = '00' + hex_str; // to keep sign bit unset for positive number
      }
      v = Buffer.from(hex_str, 'hex');
    }

    // negative number
    else {
      let hex_str = (-item).toString(16);

      const bytes = (hex_str.length + (hex_str.length % 2)) / 2; // number's size in bytes
      let N = BigInt(bytes) * BigInt(8); // number's size in bits
      const INTN_MIN = -(BigInt(2) ** (N - BigInt(1))); // minimum negative number we can keep within N bits
      if (item < INTN_MIN) {
        N += BigInt(8); // to provide room for smaller negative number
      }

      const twos_complement = BigInt(2) ** N + item;
      hex_str = twos_complement.toString(16);
      if (hex_str.length % 2 !== 0) {
        hex_str = '0' + hex_str; // Buffer.from() expects exactly two hex digits per byte
      }

      v = Buffer.from(hex_str, 'hex');
    }

    bufs.push(this.ioc.intSerializer.serialize(v.length, false));
    bufs.push(v);

    return Buffer.concat(bufs);
  }

  /**
   * @param {StreamReader} reader
   * @param {number} valueFlag
   * @param {number} typeCode
   * @returns {Promise<bigint>}
   */
  async deserializeValue(reader, valueFlag, typeCode) {
    const length = await this.ioc.intSerializer.deserializeBare(reader);
    if (length < 1) {
      throw new Error(`BigIntegerSerializer: {length}=${length} is less than one`);
    }
    const bytes = await reader.readBytes(length);
    let v = BigInt(`0x${bytes.toString('hex')}`);
    const is_sign_bit_set = (bytes[0] & 0x80) === 0x80;
    if (is_sign_bit_set) {
      v = BigInt.asIntN(length * 8, v);
    }
    return v;
  }

  /**
   * @param {StreamReader} reader
   * @returns {Promise<bigint|null>}
   */
  async deserialize(reader) {
    const type_code = await reader.readUInt8();
    if (type_code !== this.ioc.DataType.BIGINTEGER) {
      throw new Error(`BigIntegerSerializer: unexpected {type_code}=0x${type_code.toString(16)}`);
    }
    const value_flag = await reader.readUInt8();
    if (value_flag === 0x01) {
      return null;
    }
    if (value_flag !== 0x00) {
      throw new Error(`BigIntegerSerializer: unexpected {value_flag}=0x${value_flag.toString(16)}`);
    }
    return this.deserializeValue(reader, value_flag, type_code);
  }
}
