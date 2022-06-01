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
'use strict';

module.exports = class UuidSerializer {
  constructor(ioc) {
    this.ioc = ioc;
    this.ioc.serializers[ioc.DataType.UUID] = this;
  }

  canBeUsedFor(value) {
    // TODO: any idea to support UUID automatic serialization via AnySerializer in future?
  }

  serialize(item, fullyQualifiedFormat = true) {
    if (item === undefined || item === null) {
      if (fullyQualifiedFormat) {
        return Buffer.from([this.ioc.DataType.UUID, 0x01]);
      }
      return Buffer.from([
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      ]);
    }

    const uuid_str = String(item)
      .replace(/^urn:uuid:/, '')
      .replace(/[{}-]/g, '');

    const bufs = [];
    if (fullyQualifiedFormat) {
      bufs.push(Buffer.from([this.ioc.DataType.UUID, 0x00]));
    }

    const v = Buffer.alloc(16, 0);
    for (let i = 0; i < 16 && i * 2 < uuid_str.length; i++) {
      v[i] = parseInt(uuid_str.slice(i * 2, i * 2 + 2), 16);
    }
    bufs.push(v);

    return Buffer.concat(bufs);
  }

  deserialize(buffer, fullyQualifiedFormat = true, nullable = false) {
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
        if (type_code !== this.ioc.DataType.UUID) {
          throw new Error('unexpected {type_code}');
        }
        cursor = cursor.slice(1);
      }
      if (fullyQualifiedFormat || nullable) {
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

      if (cursor.length < 16) {
        throw new Error('unexpected {value} length');
      }
      len += 16;

      // Example: 2075278D-F624-4B2B-960D-25D374D57C04
      const v =
        cursor.slice(0, 4).toString('hex') +
        '-' +
        cursor.slice(4, 6).toString('hex') +
        '-' +
        cursor.slice(6, 8).toString('hex') +
        '-' +
        cursor.slice(8, 10).toString('hex') +
        '-' +
        cursor.slice(10, 16).toString('hex');
      return { v, len };
    } catch (err) {
      throw this.ioc.utils.des_error({ serializer: this, args: arguments, cursor, err });
    }
  }
};
