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

export default class StringSerializer {
  constructor(ioc, ID) {
    this.ioc = ioc;
    this.ID = ID;
    this.ioc.serializers[ID] = this;
  }

  canBeUsedFor(value) {
    return typeof value === 'string';
  }

  serialize(item, fullyQualifiedFormat = true) {
    if (item === undefined || item === null) {
      if (fullyQualifiedFormat) {
        return Buffer.from([this.ID, 0x01]);
      }
      return this.ioc.intSerializer.serialize(0, false);
    }

    const bufs = [];
    if (fullyQualifiedFormat) {
      bufs.push(Buffer.from([this.ID, 0x00]));
    }
    const v = Buffer.from(String(item), 'utf8');
    bufs.push(this.ioc.intSerializer.serialize(v.length, false)); // TODO: what if len > INT32_MAX, for now it's backed by logic of IntSerializer.serialize itself
    bufs.push(v);

    return Buffer.concat(bufs);
  }

  /**
   * Read the string value bytes from the StreamReader (length + text_value).
   * @param {StreamReader} reader
   * @param {number} valueFlag
   * @param {number} typeCode
   * @returns {Promise<string>}
   */
  async deserializeValue(reader, valueFlag, typeCode) {
    const length = await this.ioc.intSerializer.deserializeBare(reader);
    if (length < 0) {
      throw new Error(`StringSerializer: {length}=${length} is less than zero`);
    }
    if (length === 0) {
      return '';
    }
    const bytes = await reader.readBytes(length);
    return bytes.toString('utf8');
  }

  /**
   * Read a fully-qualified string from the StreamReader.
   * @param {StreamReader} reader
   * @returns {Promise<string|null>}
   */
  async deserialize(reader) {
    const type_code = await reader.readUInt8();
    if (type_code !== this.ID) {
      throw new Error(`StringSerializer: unexpected {type_code}=0x${type_code.toString(16)}`);
    }
    const value_flag = await reader.readUInt8();
    if (value_flag === 0x01) {
      return null;
    }
    if (value_flag !== 0x00) {
      throw new Error(`StringSerializer: unexpected {value_flag}=0x${value_flag.toString(16)}`);
    }
    return this.deserializeValue(reader, value_flag, type_code);
  }
}
