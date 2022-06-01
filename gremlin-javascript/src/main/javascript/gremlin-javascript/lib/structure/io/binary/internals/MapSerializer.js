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

module.exports = class MapSerializer {
  constructor(ioc) {
    this.ioc = ioc;
    this.ioc.serializers[ioc.DataType.MAP] = this;
  }

  canBeUsedFor(value) {
    if (value === null || value === undefined) {
      return false;
    }
    if (value instanceof Map) {
      return true;
    }
    if (Array.isArray(value)) {
      return false;
    }

    return typeof value === 'object';
  }

  serialize(item, fullyQualifiedFormat = true) {
    if (item === undefined || item === null) {
      if (fullyQualifiedFormat) {
        return Buffer.from([this.ioc.DataType.MAP, 0x01]);
      }
      return this.ioc.intSerializer.serialize(0, false);
    }

    const isMap = item instanceof Map;

    const keys = isMap ? Array.from(item.keys()) : Object.keys(item);
    let map_length = keys.length;
    if (map_length < 0) {
      map_length = 0;
    } else if (map_length > this.ioc.intSerializer.INT32_MAX) {
      map_length = this.ioc.intSerializer.INT32_MAX; // TODO: is it expected to be silenced? or it's better to error instead?
    }

    const bufs = [];
    if (fullyQualifiedFormat) {
      bufs.push(Buffer.from([this.ioc.DataType.MAP, 0x00]));
    }
    bufs.push(this.ioc.intSerializer.serialize(map_length, false));
    for (let i = 0; i < map_length; i++) {
      const key = keys[i];
      const value = isMap ? item.get(key) : item[key];
      bufs.push(this.ioc.anySerializer.serialize(key), this.ioc.anySerializer.serialize(value));
    }
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
        if (type_code !== this.ioc.DataType.MAP) {
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

      const v = new Map();
      for (let i = 0; i < length; i++) {
        let key, key_len;
        try {
          ({ v: key, len: key_len } = this.ioc.anySerializer.deserialize(cursor));
          len += key_len;
        } catch (err) {
          err.message = `{item_${i}} key: ` + err.message;
          throw err;
        }
        cursor = cursor.slice(key_len);

        let value, value_len;
        try {
          ({ v: value, len: value_len } = this.ioc.anySerializer.deserialize(cursor));
          len += value_len;
        } catch (err) {
          err.message = `{item_${i}} value: ` + err.message;
          throw err;
        }
        cursor = cursor.slice(value_len);

        v.set(key, value);
      }

      return { v, len };
    } catch (err) {
      throw this.ioc.utils.des_error({ serializer: this, args: arguments, cursor, err });
    }
  }
};
