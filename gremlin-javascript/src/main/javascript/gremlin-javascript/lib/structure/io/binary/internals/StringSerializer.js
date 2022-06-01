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

module.exports = class StringSerializer {
  constructor(ioc, ID) {
    this.ioc = ioc;
    this.ID = ID;
    this.ioc.serializers[ID] = this;
  }

  canBeUsedFor(value) {
    return typeof value === 'string';
  }

  serialize(item, fullyQualifiedFormat = true) {
    if (item === undefined || item === null) {
      if (fullyQualifiedFormat) {
        return Buffer.from([this.ID, 0x01]);
      }
      return this.ioc.intSerializer.serialize(0, false);
    }

    const bufs = [];
    if (fullyQualifiedFormat) {
      bufs.push(Buffer.from([this.ID, 0x00]));
    }
    const v = Buffer.from(String(item), 'utf8');
    bufs.push(this.ioc.intSerializer.serialize(v.length, false)); // TODO: what if len > INT32_MAX, for now it's backed by logic of IntSerializer.serialize itself
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
        if (type_code !== this.ID) {
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

      let length, length_len;
      try {
        ({ v: length, len: length_len } = this.ioc.intSerializer.deserialize(cursor, false));
        len += length_len;
      } catch (err) {
        err.message = '{length}: ' + err.message;
        throw err;
      }
      if (length < 0) {
        throw new Error('{length} is less than zero');
      }
      cursor = cursor.slice(length_len);

      if (cursor.length < length) {
        throw new Error('unexpected {text_value} length');
      }
      len += length;

      const v = cursor.toString('utf8', 0, length);
      return { v, len };
    } catch (err) {
      throw this.ioc.utils.des_error({ serializer: this, args: arguments, cursor, err });
    }
  }
};
