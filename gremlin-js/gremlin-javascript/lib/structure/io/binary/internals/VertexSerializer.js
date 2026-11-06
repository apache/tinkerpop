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
import { Vertex } from '../../../graph.js';

export default class VertexSerializer {
  constructor(ioc) {
    this.ioc = ioc;
    this.ioc.serializers[ioc.DataType.VERTEX] = this;
  }

  canBeUsedFor(value) {
    return value instanceof Vertex;
  }

  serialize(item, fullyQualifiedFormat = true) {
    if (item === undefined || item === null) {
      if (fullyQualifiedFormat) {
        return Buffer.from([this.ioc.DataType.VERTEX, 0x01]);
      }
      const id = [0x03, 0x00, 0x00, 0x00, 0x00, 0x00]; // String ''
      const label = [0x00, 0x00, 0x00, 0x00]; // empty list
      const properties = [0x09, 0x00, 0x00, 0x00, 0x00, 0x00]; // empty list
      return Buffer.from([...id, ...label, ...properties]);
    }

    const bufs = [];
    if (fullyQualifiedFormat) {
      bufs.push(Buffer.from([this.ioc.DataType.VERTEX, 0x00]));
    }

    // {id}
    bufs.push(this.ioc.anySerializer.serialize(item.id));

    // {label}
    const labels =
      item.labels instanceof Set
        ? Array.from(item.labels)
        : Array.isArray(item.label)
          ? item.label
          : item.label
            ? [item.label]
            : [];
    bufs.push(this.ioc.listSerializer.serialize(labels, false));

    // {properties}
    bufs.push(this.ioc.listSerializer.serialize([], true));

    return Buffer.concat(bufs);
  }

  /**
   * Async deserialization of vertex value bytes from a StreamReader.
   * @param {StreamReader} reader
   * @param {number} valueFlag
   * @param {number} typeCode
   * @returns {Promise<Vertex>}
   */
  async deserializeValue(reader, valueFlag, typeCode) {
    // {id} fully qualified
    const id = await this.ioc.anySerializer.deserialize(reader);

    // {label} bare list - full multi-label list
    const labelList = await this.ioc.listSerializer.deserializeValue(reader, 0x00, this.ioc.DataType.LIST);
    const labels = Array.isArray(labelList) ? labelList : labelList ? [labelList] : [];
    const label = labels.length > 0 ? labels[0] : 'vertex';

    // {properties} fully qualified
    const properties = await this.ioc.anySerializer.deserialize(reader);

    return new Vertex(id, label, properties || [], labels);
  }

  /**
   * Async fully-qualified deserialization from a StreamReader.
   * @param {StreamReader} reader
   * @returns {Promise<Vertex|null>}
   */
  async deserialize(reader) {
    const type_code = await reader.readUInt8();
    if (type_code !== this.ioc.DataType.VERTEX) {
      throw new Error(`VertexSerializer: unexpected {type_code}=0x${type_code.toString(16)}`);
    }
    const value_flag = await reader.readUInt8();
    if (value_flag === 0x01) {
      return null;
    }
    if (value_flag !== 0x00) {
      throw new Error(`VertexSerializer: unexpected {value_flag}=0x${value_flag.toString(16)}`);
    }
    return this.deserializeValue(reader, value_flag, type_code);
  }
}
