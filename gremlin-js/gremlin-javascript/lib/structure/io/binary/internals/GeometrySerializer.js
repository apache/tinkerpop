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

import { Buffer } from 'buffer';
import { Geometry, readWkb } from '../../../geo.js';

/**
 * GraphBinary serializer for the Geometry type (0x31).
 *
 * Wire format:
 *   value = {int srid}{ISO WKB}
 * Writes big-endian WKB and reads either byte order.
 */
export default class GeometrySerializer {
  constructor(ioc) {
    this.ioc = ioc;
    this.ID = ioc.DataType.GEOMETRY;
    this.ioc.serializers[this.ID] = this;
  }

  canBeUsedFor(value) {
    return value instanceof Geometry;
  }

  serialize(item, fullyQualifiedFormat = true) {
    if (item === undefined || item === null) {
      if (fullyQualifiedFormat) {
        return Buffer.from([this.ID, 0x01]);
      }
      // every WKB geometry carries a byte order marker and a type code, so
      // there is no byte sequence that stands for a missing geometry
      throw new Error('GeometrySerializer: a null geometry cannot be serialized without a value flag');
    }

    const bufs = [];
    if (fullyQualifiedFormat) {
      bufs.push(Buffer.from([this.ID, 0x00]));
    }
    bufs.push(this.ioc.intSerializer.serialize(item.srid, false));
    bufs.push(item.toWKB());
    return Buffer.concat(bufs);
  }

  /**
   * @param {StreamReader} reader
   * @param {number} valueFlag
   * @param {number} typeCode
   * @returns {Promise<Geometry>}
   */
  async deserializeValue(reader, valueFlag, typeCode) {
    const srid = await this.ioc.intSerializer.deserializeBare(reader);
    return readWkb(reader, srid);
  }

  /**
   * @param {StreamReader} reader
   * @returns {Promise<Geometry|null>}
   */
  async deserialize(reader) {
    const type_code = await reader.readUInt8();
    if (type_code !== this.ID) {
      throw new Error(`GeometrySerializer: unexpected {type_code}=0x${type_code.toString(16)}`);
    }
    const value_flag = await reader.readUInt8();
    if (value_flag === 0x01) {
      return null;
    }
    if (value_flag !== 0x00) {
      throw new Error(`GeometrySerializer: unexpected {value_flag}=0x${value_flag.toString(16)}`);
    }
    return this.deserializeValue(reader, value_flag, type_code);
  }
}
