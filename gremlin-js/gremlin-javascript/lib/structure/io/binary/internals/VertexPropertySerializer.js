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
import { VertexProperty } from '../../../graph.js';

export default class VertexPropertySerializer {
  constructor(ioc) {
    this.ioc = ioc;
    this.ioc.serializers[ioc.DataType.VERTEXPROPERTY] = this;
  }

  canBeUsedFor(value) {
    return value instanceof VertexProperty;
  }

  serialize(item, fullyQualifiedFormat = true) {
    if (item === undefined || item === null) {
      if (fullyQualifiedFormat) {
        return Buffer.from([this.ioc.DataType.VERTEXPROPERTY, 0x01]);
      }
      return Buffer.concat([
        this.ioc.unspecifiedNullSerializer.serialize(null), // {id}=null
        this.ioc.listSerializer.serialize([], false), // {label}=empty list
        this.ioc.unspecifiedNullSerializer.serialize(null), // {value}=null
        this.ioc.unspecifiedNullSerializer.serialize(null), // {parent}=null
        this.ioc.listSerializer.serialize([], true), // {properties}=empty list
      ]);
    }

    const bufs = [];
    if (fullyQualifiedFormat) {
      bufs.push(Buffer.from([this.ioc.DataType.VERTEXPROPERTY, 0x00]));
    }

    // {id}
    bufs.push(this.ioc.anySerializer.serialize(item.id));

    // {label}
    const labels = Array.isArray(item.label) ? item.label : item.label ? [item.label] : [];
    bufs.push(this.ioc.listSerializer.serialize(labels, false));

    // {value}
    bufs.push(this.ioc.anySerializer.serialize(item.value));

    // {parent}
    bufs.push(this.ioc.unspecifiedNullSerializer.serialize(null));

    // {properties}
    bufs.push(this.ioc.listSerializer.serialize([], true));

    return Buffer.concat(bufs);
  }

  /**
   * Async deserialization of vertex property value bytes from a StreamReader.
   * @param {StreamReader} reader
   * @param {number} valueFlag
   * @param {number} typeCode
   * @returns {Promise<VertexProperty>}
   */
  async deserializeValue(reader, valueFlag, typeCode) {
    // {id} fully qualified
    const id = await this.ioc.anySerializer.deserialize(reader);

    // {label} bare list, extract first element
    const labelList = await this.ioc.listSerializer.deserializeValue(reader, 0x00, this.ioc.DataType.LIST);
    const label = Array.isArray(labelList) && labelList.length > 0 ? labelList[0] : labelList;

    // {value} fully qualified
    const value = await this.ioc.anySerializer.deserialize(reader);

    // {parent} fully qualified (always null in current TinkerPop)
    await this.ioc.anySerializer.deserialize(reader);

    // {properties} fully qualified
    const properties = await this.ioc.anySerializer.deserialize(reader);

    return new VertexProperty(id, label, value, properties || []);
  }

  /**
   * Async fully-qualified deserialization from a StreamReader.
   * @param {StreamReader} reader
   * @returns {Promise<VertexProperty|null>}
   */
  async deserialize(reader) {
    const type_code = await reader.readUInt8();
    if (type_code !== this.ioc.DataType.VERTEXPROPERTY) {
      throw new Error(`VertexPropertySerializer: unexpected {type_code}=0x${type_code.toString(16)}`);
    }
    const value_flag = await reader.readUInt8();
    if (value_flag === 0x01) {
      return null;
    }
    if (value_flag !== 0x00) {
      throw new Error(`VertexPropertySerializer: unexpected {value_flag}=0x${value_flag.toString(16)}`);
    }
    return this.deserializeValue(reader, value_flag, type_code);
  }
}
