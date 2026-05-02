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
import { direction, t as _t, merge as _merge, EnumValue } from '../../../../process/traversal.js';

export default class EnumSerializer {
  constructor(ioc) {
    this.ioc = ioc;

    // process/traversal.js:toEnum() lowercases element names (e.g. OUT => out) for end users' convenience
    // but we need original mapping as is for wire format lookup
    const to_orig_enum = (obj) => {
      const r = {};
      Object.values(obj).forEach((e) => (r[e.elementName] = e));
      return r;
    };

    this.types = {
      Direction: { code: ioc.DataType.DIRECTION, enum: to_orig_enum(direction) },
      Merge: { code: ioc.DataType.MERGE, enum: to_orig_enum(_merge) },
      T: { code: ioc.DataType.T, enum: to_orig_enum(_t) },
    };

    this.ioc.serializers[ioc.DataType.DIRECTION] = this;
    this.ioc.serializers[ioc.DataType.MERGE] = this;
    this.ioc.serializers[ioc.DataType.T] = this;
  }

  canBeUsedFor(value) {
    return (
      value instanceof EnumValue &&
      (value.typeName === 'Direction' || value.typeName === 'Merge' || value.typeName === 'T')
    );
  }

  serialize(item, fullyQualifiedFormat = true) {
    const type = this.types[item.typeName];
    if (item.elementName === undefined || item.elementName === null) {
      if (fullyQualifiedFormat) {
        return Buffer.from([type.code, 0x01]);
      }
      return this.ioc.stringSerializer.serialize(null, false);
    }

    const bufs = [];
    if (fullyQualifiedFormat) {
      bufs.push(Buffer.from([type.code, 0x00]));
    }
    bufs.push(this.ioc.stringSerializer.serialize(item.elementName, true));

    return Buffer.concat(bufs);
  }

  /**
   * Resolve the type_code (already read by AnySerializer) to a typeName.
   * Called by AnySerializer before dispatching.
   */
  _typeNameForCode(type_code) {
    if (type_code === this.ioc.DataType.DIRECTION) {
      return 'Direction';
    }
    if (type_code === this.ioc.DataType.MERGE) {
      return 'Merge';
    }
    if (type_code === this.ioc.DataType.T) {
      return 'T';
    }
    return undefined;
  }

  /**
   * @param {StreamReader} reader
   * @param {number} valueFlag - already consumed by AnySerializer
   * @param {number} typeCode - the type_code byte already read by AnySerializer
   * @returns {Promise<EnumValue>}
   */
  async deserializeValue(reader, valueFlag, typeCode) {
    const typeName = this._typeNameForCode(typeCode);
    // elementName is a fully-qualified String (type_code + value_flag + length + text)
    const elementName = await this.ioc.stringSerializer.deserialize(reader);

    if (typeName) {
      return this.types[typeName].enum[elementName];
    }
    return new EnumValue(undefined, elementName);
  }

  /**
   * Async fully-qualified deserialization from a StreamReader.
   * @param {StreamReader} reader
   * @returns {Promise<EnumValue|null>}
   */
  async deserialize(reader) {
    const type_code = await reader.readUInt8();
    if (!this._typeNameForCode(type_code)) {
      throw new Error(`EnumSerializer: unexpected {type_code}=0x${type_code.toString(16)}`);
    }
    const value_flag = await reader.readUInt8();
    if (value_flag === 0x01) {
      return null;
    }
    if (value_flag !== 0x00) {
      throw new Error(`EnumSerializer: unexpected {value_flag}=0x${value_flag.toString(16)}`);
    }
    return this.deserializeValue(reader, value_flag, type_code);
  }
}
