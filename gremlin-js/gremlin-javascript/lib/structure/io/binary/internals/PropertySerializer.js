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
import { Property } from '../../../graph.js';

export default class PropertySerializer {
  constructor(ioc) {
    this.ioc = ioc;
    this.ioc.serializers[ioc.DataType.PROPERTY] = this;
  }

  canBeUsedFor(value) {
    return value instanceof Property;
  }

  serialize(item, fullyQualifiedFormat = true) {
    if (item === undefined || item === null) {
      if (fullyQualifiedFormat) {
        return Buffer.from([this.ioc.DataType.PROPERTY, 0x01]);
      }
      return Buffer.concat([
        this.ioc.stringSerializer.serialize('', false), // {key}=''
        this.ioc.unspecifiedNullSerializer.serialize(null), // {value}=null
        this.ioc.unspecifiedNullSerializer.serialize(null), // {parent}=null
      ]);
    }

    const bufs = [];
    if (fullyQualifiedFormat) {
      bufs.push(Buffer.from([this.ioc.DataType.PROPERTY, 0x00]));
    }

    // {key}
    bufs.push(this.ioc.stringSerializer.serialize(item.key, false));

    // {value}
    bufs.push(this.ioc.anySerializer.serialize(item.value));

    // {parent}
    bufs.push(this.ioc.unspecifiedNullSerializer.serialize(null));

    return Buffer.concat(bufs);
  }

  /**
   * Async deserialization of property value bytes from a StreamReader.
   * @param {StreamReader} reader
   * @param {number} valueFlag
   * @param {number} typeCode
   * @returns {Promise<Property>}
   */
  async deserializeValue(reader, valueFlag, typeCode) {
    // {key} bare string (length + text_value)
    const key = await this.ioc.stringSerializer.deserializeValue(reader, 0x00, typeCode);

    // {value} fully qualified
    const value = await this.ioc.anySerializer.deserialize(reader);

    // {parent} fully qualified (always null in current TinkerPop)
    await this.ioc.anySerializer.deserialize(reader);

    return new Property(key, value);
  }

  /**
   * Async fully-qualified deserialization from a StreamReader.
   * @param {StreamReader} reader
   * @returns {Promise<Property|null>}
   */
  async deserialize(reader) {
    const type_code = await reader.readUInt8();
    if (type_code !== this.ioc.DataType.PROPERTY) {
      throw new Error(`PropertySerializer: unexpected {type_code}=0x${type_code.toString(16)}`);
    }
    const value_flag = await reader.readUInt8();
    if (value_flag === 0x01) {
      return null;
    }
    if (value_flag !== 0x00) {
      throw new Error(`PropertySerializer: unexpected {value_flag}=0x${value_flag.toString(16)}`);
    }
    return this.deserializeValue(reader, value_flag, type_code);
  }
}
