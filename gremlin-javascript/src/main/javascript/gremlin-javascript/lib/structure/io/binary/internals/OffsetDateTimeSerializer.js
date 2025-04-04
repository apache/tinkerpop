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

'use strict';

const { Buffer } = require('buffer');

module.exports = class OffsetDateTimeSerializer {
  constructor(ioc, ID) {
    this.ioc = ioc;
    this.ID = ID;
    this.ioc.serializers[ID] = this;
  }

  canBeUsedFor(value) {
    return value instanceof Date;
  }

  serialize(item, fullyQualifiedFormat = true) {
    if (item === undefined || item === null) {
      if (fullyQualifiedFormat) {
        return Buffer.from([this.ID, 0x01]);
      }
      return Buffer.from([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
    }

    const bufs = [];
    if (fullyQualifiedFormat) {
      bufs.push(Buffer.from([this.ID, 0x00]));
    }

    // {year}
    let v = Buffer.alloc(4);
    v.writeInt32BE(item.getFullYear());
    bufs.push(v);

    // {month}
    v = Buffer.alloc(1);
    v.writeUInt8(item.getMonth() + 1); // Java Core DateTime serializer uses 1 - 12 for months, JS uses indices
    bufs.push(v);

    // {day}
    v = Buffer.alloc(1);
    v.writeUInt8(item.getDate());
    bufs.push(v);

    // {nanoseconds}
    const h = item.getHours();
    const m = item.getMinutes();
    const s = item.getSeconds();
    const ms = item.getMilliseconds();
    const ns = h * 60 * 60 * 1e9 + m * 60 * 1e9 + s * 1e9 + ms * 1e6;
    v = Buffer.alloc(8);
    v.writeBigInt64BE(BigInt(ns));
    bufs.push(v);

    // {zone offset}
    v = Buffer.alloc(4);
    v.writeInt32BE(item.getTimezoneOffset() * -60);
    bufs.push(v);

    return Buffer.concat(bufs);
  }

  deserialize(buffer, fullyQualifiedFormat = true) {
    let len = 0;
    let cursor = buffer;

    try {
      if (buffer === undefined || buffer === null || !(buffer instanceof Buffer)) {
        throw new Error('buffer is missing');
      }
      if (buffer.length < 1) {
        throw new Error('buffer is empty');
      }

      if (fullyQualifiedFormat) {
        const type_code = cursor.readUInt8();
        len++;
        if (type_code !== this.ID) {
          throw new Error('unexpected {type_code}');
        }
        cursor = cursor.slice(1);

        if (cursor.length < 1) {
          throw new Error('{value_flag} is missing');
        }
        const value_flag = cursor.readUInt8();
        len++;
        if (value_flag === 1) {
          return { v: null, len };
        }
        if (value_flag !== 0) {
          throw new Error('unexpected {value_flag}');
        }
        cursor = cursor.slice(1);
      }

      if (cursor.length < 8) {
        throw new Error('unexpected {value} length');
      }
      len += 8;

      const year = cursor.readInt32BE();
      cursor = cursor.slice(4);
      const month = cursor.readUInt8() - 1;
      cursor = cursor.slice(1);
      const date = cursor.readUInt8();
      cursor = cursor.slice(1);
      const ns = cursor.readBigInt64BE();
      // calculate hour, minute, second, and ms from ns as JS Date doesn't have ns precision
      const totalMS = ns / BigInt(1e6);
      const ms = Number(totalMS) % 1e3;
      const totalS = Math.trunc(Number(totalMS) / 1e3);
      const s = totalS % 60;
      const totalM = Math.trunc(totalS / 60);
      const m = totalM % 60;
      const h = Math.trunc(totalM / 60);

      cursor = cursor.slice(8);
      cursor.readInt32BE();
      // offset isn't used as Date automatically uses UTC time based on offset
      const v = new Date(year, month, date, h, m, s, ms);

      len += 18;

      return { v, len };
    } catch (err) {
      throw this.ioc.utils.des_error({ serializer: this, args: arguments, cursor, err });
    }
  }
};
