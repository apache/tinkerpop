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

export default class UuidSerializer {
  constructor(ioc) {
    this.ioc = ioc;
    this.ioc.serializers[ioc.DataType.UUID] = this;
  }

  canBeUsedFor(value) {
    // TODO: any idea to support UUID automatic serialization via AnySerializer in future?
  }

  serialize(item, fullyQualifiedFormat = true) {
    if (item === undefined || item === null) {
      if (fullyQualifiedFormat) {
        return Buffer.from([this.ioc.DataType.UUID, 0x01]);
      }
      return Buffer.from([
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      ]);
    }

    const uuid_str = String(item)
      .replace(/^urn:uuid:/, '')
      .replace(/[{}-]/g, '');

    const bufs = [];
    if (fullyQualifiedFormat) {
      bufs.push(Buffer.from([this.ioc.DataType.UUID, 0x00]));
    }

    const v = Buffer.alloc(16, 0);
    for (let i = 0; i < 16 && i * 2 < uuid_str.length; i++) {
      v[i] = parseInt(uuid_str.slice(i * 2, i * 2 + 2), 16);
    }
    bufs.push(v);

    return Buffer.concat(bufs);
  }

  /**
   * @param {StreamReader} reader
   * @param {number} valueFlag
   * @param {number} typeCode
   * @returns {Promise<string>}
   */
  async deserializeValue(reader, valueFlag, typeCode) {
    const bytes = await reader.readBytes(16);
    return (
      bytes.subarray(0, 4).toString('hex') +
      '-' +
      bytes.subarray(4, 6).toString('hex') +
      '-' +
      bytes.subarray(6, 8).toString('hex') +
      '-' +
      bytes.subarray(8, 10).toString('hex') +
      '-' +
      bytes.subarray(10, 16).toString('hex')
    );
  }

  /**
   * @param {StreamReader} reader
   * @returns {Promise<string|null>}
   */
  async deserialize(reader) {
    const type_code = await reader.readUInt8();
    if (type_code !== this.ioc.DataType.UUID) {
      throw new Error(`UuidSerializer: unexpected {type_code}=0x${type_code.toString(16)}`);
    }
    const value_flag = await reader.readUInt8();
    if (value_flag === 0x01) {
      return null;
    }
    if (value_flag !== 0x00) {
      throw new Error(`UuidSerializer: unexpected {value_flag}=0x${value_flag.toString(16)}`);
    }
    return this.deserializeValue(reader, value_flag, type_code);
  }
}
