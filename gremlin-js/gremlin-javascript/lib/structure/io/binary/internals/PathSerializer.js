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
import { Path } from '../../../graph.js';

export default class PathSerializer {
  constructor(ioc) {
    this.ioc = ioc;
    this.ioc.serializers[ioc.DataType.PATH] = this;
  }

  canBeUsedFor(value) {
    return value instanceof Path;
  }

  serialize(item, fullyQualifiedFormat = true) {
    if (item === undefined || item === null) {
      if (fullyQualifiedFormat) {
        return Buffer.from([this.ioc.DataType.PATH, 0x01]);
      }
      return Buffer.concat([
        this.ioc.listSerializer.serialize([]), // {labels}=[]
        this.ioc.listSerializer.serialize([]), // {objects}=[]
      ]);
    }

    const bufs = [];
    if (fullyQualifiedFormat) {
      bufs.push(Buffer.from([this.ioc.DataType.PATH, 0x00]));
    }

    // {labels} is a List in which each item is a Set of String
    bufs.push(this.ioc.listSerializer.serialize(item.labels));

    // {objects} is a List of fully qualified typed values
    bufs.push(this.ioc.listSerializer.serialize(item.objects));

    return Buffer.concat(bufs);
  }

  /**
   * Async deserialization of path value bytes from a StreamReader.
   * @param {StreamReader} reader
   * @param {number} valueFlag
   * @param {number} typeCode
   * @returns {Promise<Path>}
   */
  async deserializeValue(reader, valueFlag, typeCode) {
    // {labels} fully qualified list
    const labels = await this.ioc.anySerializer.deserialize(reader);

    // {objects} fully qualified list
    const objects = await this.ioc.anySerializer.deserialize(reader);

    return new Path(labels, objects);
  }

  /**
   * Async fully-qualified deserialization from a StreamReader.
   * @param {StreamReader} reader
   * @returns {Promise<Path|null>}
   */
  async deserialize(reader) {
    const type_code = await reader.readUInt8();
    if (type_code !== this.ioc.DataType.PATH) {
      throw new Error(`PathSerializer: unexpected {type_code}=0x${type_code.toString(16)}`);
    }
    const value_flag = await reader.readUInt8();
    if (value_flag === 0x01) {
      return null;
    }
    if (value_flag !== 0x00) {
      throw new Error(`PathSerializer: unexpected {value_flag}=0x${value_flag.toString(16)}`);
    }
    return this.deserializeValue(reader, value_flag, type_code);
  }
}
