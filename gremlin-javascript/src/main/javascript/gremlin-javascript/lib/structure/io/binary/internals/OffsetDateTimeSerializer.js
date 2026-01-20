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

export default class OffsetDateTimeSerializer {
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

    // NOTE: js Date will always display time in UTC, but regular date/hour getters will return in local system time.
    // To avoid inconsistency we will always serialize the UTC representation of the Date object with offset of 0.

    // {year}
    let v = Buffer.alloc(4);
    v.writeInt32BE(item.getUTCFullYear());
    bufs.push(v);

    // {month}
    v = Buffer.alloc(1);
    v.writeUInt8(item.getUTCMonth() + 1); // Java Core DateTime serializer uses 1 - 12 for months, JS uses indices
    bufs.push(v);

    // {day} - in UTC
    v = Buffer.alloc(1);
    v.writeUInt8(item.getUTCDate());
    bufs.push(v);

    // {nanoseconds}
    const h = item.getUTCHours(); // in UTC
    const m = item.getUTCMinutes();
    const s = item.getUTCSeconds();
    const ms = item.getUTCMilliseconds();
    const ns = h * 60 * 60 * 1e9 + m * 60 * 1e9 + s * 1e9 + ms * 1e6;
    v = Buffer.alloc(8);
    v.writeBigInt64BE(BigInt(ns));
    bufs.push(v);

    // {zone offset} - UTC is always used for serialization, as such offset will be 0
    v = Buffer.alloc(4);
    v.writeInt32BE(0);
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
      len += 18;

      const year = cursor.readInt32BE();
      cursor = cursor.slice(4);
      const month = cursor.readUInt8() - 1;
      cursor = cursor.slice(1);
      const date = cursor.readUInt8();
      cursor = cursor.slice(1);
      const ns = cursor.readBigInt64BE();
      cursor = cursor.slice(8);
      const offset = cursor.readInt32BE();
      cursor.slice(4);

      // calculate hour, minute, second, and ms from ns as JS Date doesn't have ns precision
      const totalMS = ns / BigInt(1e6);
      const ms = Number(totalMS) % 1e3;
      const totalS = Math.trunc(Number(totalMS) / 1e3) - offset; // js Date doesn't have a way to set offset properly, account offset here to use UTC
      const s = totalS % 60;
      const totalM = Math.trunc(totalS / 60);
      const m = totalM % 60;
      const h = Math.trunc(totalM / 60);

      // use UTC time calculated with offset above
      const v = new Date(Date.UTC(year, month, date, h, m, s, ms));

      return { v, len };
    } catch (err) {
      throw this.ioc.utils.des_error({ serializer: this, args: arguments, cursor, err });
    }
  }
}
