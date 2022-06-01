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

const t = require('../../../../process/traversal');

module.exports = class TraverserSerializer {
  constructor(ioc) {
    this.ioc = ioc;
    this.ioc.serializers[ioc.DataType.TRAVERSER] = this;
  }

  canBeUsedFor(value) {
    return value instanceof t.Traverser;
  }

  serialize(item, fullyQualifiedFormat = true) {
    if (item === undefined || item === null) {
      if (fullyQualifiedFormat) {
        return Buffer.from([this.ioc.DataType.TRAVERSER, 0x01]);
      }
      const bulk = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01]; // 1
      const value = [0xfe, 0x01]; // null
      return Buffer.from([...bulk, ...value]);
    }

    const bufs = [];
    if (fullyQualifiedFormat) {
      bufs.push(Buffer.from([this.ioc.DataType.TRAVERSER, 0x00]));
    }

    // {bulk}
    bufs.push(this.ioc.longSerializer.serialize(item.bulk, false));
    // {value}
    bufs.push(this.ioc.anySerializer.serialize(item.object));

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
        if (type_code !== this.ioc.DataType.TRAVERSER) {
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

      let bulk, bulk_len;
      try {
        ({ v: bulk, len: bulk_len } = this.ioc.longSerializer.deserialize(cursor, false));
        len += bulk_len;
      } catch (err) {
        err.message = '{bulk}: ' + err.message;
        throw err;
      }
      if (bulk < 0) {
        throw new Error('{bulk} is less than zero');
      }
      cursor = cursor.slice(bulk_len);

      let value, value_len;
      try {
        ({ v: value, len: value_len } = this.ioc.anySerializer.deserialize(cursor));
        len += value_len;
      } catch (err) {
        err.message = '{value}: ' + err.message;
        throw err;
      }
      cursor = cursor.slice(value_len);

      const v = new t.Traverser(value, bulk);
      return { v, len };
    } catch (err) {
      throw this.ioc.utils.des_error({ serializer: this, args: arguments, cursor, err });
    }
  }
};
