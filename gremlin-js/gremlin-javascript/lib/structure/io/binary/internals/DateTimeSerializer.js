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

export default class DateTimeSerializer {
  constructor(ioc) {
    this.ioc = ioc;
    this.ID = ioc.DataType.DATETIME;
    this.ioc.serializers[this.ID] = this;
  }

  canBeUsedFor(value) {
    return value instanceof Date;
  }

  serialize(item, fullyQualifiedFormat = true) {
    if (item === undefined || item === null) {
      if (fullyQualifiedFormat) {
        return Buffer.from([this.ID, 0x01]);
      }
      return Buffer.alloc(18);
    }

    const bufs = [];
    if (fullyQualifiedFormat) {
      bufs.push(Buffer.from([this.ID, 0x00]));
    }

    const v = Buffer.alloc(18);
    let offset = 0;

    // Year (Int32BE)
    v.writeInt32BE(item.getUTCFullYear(), offset);
    offset += 4;

    // Month (UInt8, 1-based)
    v.writeUInt8(item.getUTCMonth() + 1, offset);
    offset += 1;

    // Day (UInt8, 1-based)
    v.writeUInt8(item.getUTCDate(), offset);
    offset += 1;

    // Nanoseconds since midnight (BigInt64BE)
    const hours = item.getUTCHours();
    const minutes = item.getUTCMinutes();
    const seconds = item.getUTCSeconds();
    const millis = item.getUTCMilliseconds();
    const nanos = BigInt(hours * 3600 + minutes * 60 + seconds) * 1_000_000_000n + BigInt(millis) * 1_000_000n;
    v.writeBigInt64BE(nanos, offset);
    offset += 8;

    // UTC offset in seconds (Int32BE) - always 0 for JS Date
    v.writeInt32BE(0, offset);

    bufs.push(v);
    return Buffer.concat(bufs);
  }

  /**
   * @param {StreamReader} reader
   * @param {number} valueFlag
   * @param {number} typeCode
   * @returns {Promise<Date>}
   */
  async deserializeValue(reader, valueFlag, typeCode) {
    // 18 bytes: year(4) + month(1) + day(1) + nanos(8) + utcOffset(4)
    const buf = await reader.readBytes(18);

    let offset = 0;
    const year = buf.readInt32BE(offset);
    offset += 4;
    const month = buf.readUInt8(offset);
    offset += 1;
    const day = buf.readUInt8(offset);
    offset += 1;
    const nanos = buf.readBigInt64BE(offset);
    offset += 8;
    const utcOffset = buf.readInt32BE(offset);

    // Convert nanos to time components
    const hours = Number(nanos / 3_600_000_000_000n);
    const remainingNanos = nanos % 3_600_000_000_000n;
    const minutes = Number(remainingNanos / 60_000_000_000n);
    const remainingNanos2 = remainingNanos % 60_000_000_000n;
    const seconds = Number(remainingNanos2 / 1_000_000_000n);
    const millis = Number((remainingNanos2 % 1_000_000_000n) / 1_000_000n);

    const v = new Date(Date.UTC(year, month - 1, day, hours, minutes, seconds, millis));
    // Date.UTC treats years 0-99 as 1900-1999, correct it
    if (year >= 0 && year <= 99) {
      v.setUTCFullYear(year);
    }
    // Adjust for non-zero UTC offset (JS Date is always UTC internally)
    if (utcOffset !== 0) {
      v.setTime(v.getTime() - utcOffset * 1000);
    }

    return v;
  }

  /**
   * @param {StreamReader} reader
   * @returns {Promise<Date|null>}
   */
  async deserialize(reader) {
    const type_code = await reader.readUInt8();
    if (type_code !== this.ID) {
      throw new Error(`DateTimeSerializer: unexpected {type_code}=0x${type_code.toString(16)}`);
    }
    const value_flag = await reader.readUInt8();
    if (value_flag === 0x01) {
      return null;
    }
    if (value_flag !== 0x00) {
      throw new Error(`DateTimeSerializer: unexpected {value_flag}=0x${value_flag.toString(16)}`);
    }
    return this.deserializeValue(reader, value_flag, type_code);
  }
}
