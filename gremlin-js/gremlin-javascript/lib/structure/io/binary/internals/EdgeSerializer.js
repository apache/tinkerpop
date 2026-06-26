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
import { Edge, Vertex } from '../../../graph.js';

export default class EdgeSerializer {
  constructor(ioc) {
    this.ioc = ioc;
    this.ioc.serializers[ioc.DataType.EDGE] = this;
  }

  canBeUsedFor(value) {
    return value instanceof Edge;
  }

  serialize(item, fullyQualifiedFormat = true) {
    if (item === undefined || item === null) {
      if (fullyQualifiedFormat) {
        return Buffer.from([this.ioc.DataType.EDGE, 0x01]);
      }
      const id = [0x03, 0x00, 0x00, 0x00, 0x00, 0x00]; // String ''
      const label = [0x00, 0x00, 0x00, 0x00]; // empty list
      const inVId = [0x03, 0x00, 0x00, 0x00, 0x00, 0x00]; // String ''
      const inVLabel = [0x00, 0x00, 0x00, 0x00]; // empty list
      const outVId = [0x03, 0x00, 0x00, 0x00, 0x00, 0x00]; // String ''
      const outVLabel = [0x00, 0x00, 0x00, 0x00]; // empty list
      const parent = [0xfe, 0x01]; // null
      const properties = [0x09, 0x00, 0x00, 0x00, 0x00, 0x00]; // empty list
      return Buffer.from([...id, ...label, ...inVId, ...inVLabel, ...outVId, ...outVLabel, ...parent, ...properties]);
    }

    const bufs = [];
    if (fullyQualifiedFormat) {
      bufs.push(Buffer.from([this.ioc.DataType.EDGE, 0x00]));
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

    // {inVId}
    const inVId = item.inV && item.inV.id;
    bufs.push(this.ioc.anySerializer.serialize(inVId));

    // {inVLabel}
    const inVLabels =
      item.inV && item.inV.labels instanceof Set
        ? Array.from(item.inV.labels)
        : item.inV && Array.isArray(item.inV.label)
          ? item.inV.label
          : item.inV && item.inV.label
            ? [item.inV.label]
            : [];
    bufs.push(this.ioc.listSerializer.serialize(inVLabels, false));

    // {outVId}
    const outVId = item.outV && item.outV.id;
    bufs.push(this.ioc.anySerializer.serialize(outVId));

    // {outVLabel}
    const outVLabels =
      item.outV && item.outV.labels instanceof Set
        ? Array.from(item.outV.labels)
        : item.outV && Array.isArray(item.outV.label)
          ? item.outV.label
          : item.outV && item.outV.label
            ? [item.outV.label]
            : [];
    bufs.push(this.ioc.listSerializer.serialize(outVLabels, false));

    // {parent}
    bufs.push(this.ioc.unspecifiedNullSerializer.serialize(null));

    // {properties}
    bufs.push(this.ioc.listSerializer.serialize([], true));

    return Buffer.concat(bufs);
  }

  /**
   * Async deserialization of edge value bytes from a StreamReader.
   * @param {StreamReader} reader
   * @param {number} valueFlag
   * @param {number} typeCode
   * @returns {Promise<Edge>}
   */
  async deserializeValue(reader, valueFlag, typeCode) {
    // {id} fully qualified
    const id = await this.ioc.anySerializer.deserialize(reader);

    // {label} bare list - full multi-label list
    const labelList = await this.ioc.listSerializer.deserializeValue(reader, 0x00, this.ioc.DataType.LIST);
    const labels = Array.isArray(labelList) ? labelList : labelList ? [labelList] : [];
    const label = labels.length > 0 ? labels[0] : 'edge';

    // {inVId} fully qualified
    const inVId = await this.ioc.anySerializer.deserialize(reader);

    // {inVLabel} bare list - full multi-label list
    const inVLabelList = await this.ioc.listSerializer.deserializeValue(reader, 0x00, this.ioc.DataType.LIST);
    const inVLabels = Array.isArray(inVLabelList) ? inVLabelList : inVLabelList ? [inVLabelList] : [];
    const inVLabel = inVLabels.length > 0 ? inVLabels[0] : 'vertex';

    // {outVId} fully qualified
    const outVId = await this.ioc.anySerializer.deserialize(reader);

    // {outVLabel} bare list - full multi-label list
    const outVLabelList = await this.ioc.listSerializer.deserializeValue(reader, 0x00, this.ioc.DataType.LIST);
    const outVLabels = Array.isArray(outVLabelList) ? outVLabelList : outVLabelList ? [outVLabelList] : [];
    const outVLabel = outVLabels.length > 0 ? outVLabels[0] : 'vertex';

    // {parent} fully qualified (always null in current TinkerPop)
    await this.ioc.anySerializer.deserialize(reader);

    // {properties} fully qualified
    const properties = await this.ioc.anySerializer.deserialize(reader);

    return new Edge(
      id,
      new Vertex(outVId, outVLabel, null, outVLabels),
      label,
      new Vertex(inVId, inVLabel, null, inVLabels),
      properties || [],
      labels,
    );
  }

  /**
   * Async fully-qualified deserialization from a StreamReader.
   * @param {StreamReader} reader
   * @returns {Promise<Edge|null>}
   */
  async deserialize(reader) {
    const type_code = await reader.readUInt8();
    if (type_code !== this.ioc.DataType.EDGE) {
      throw new Error(`EdgeSerializer: unexpected {type_code}=0x${type_code.toString(16)}`);
    }
    const value_flag = await reader.readUInt8();
    if (value_flag === 0x01) {
      return null;
    }
    if (value_flag !== 0x00) {
      throw new Error(`EdgeSerializer: unexpected {value_flag}=0x${value_flag.toString(16)}`);
    }
    return this.deserializeValue(reader, value_flag, type_code);
  }
}
