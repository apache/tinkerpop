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

const g = require('../../../graph');

module.exports = class PropertySerializer {
  constructor(ioc) {
    this.ioc = ioc;
    this.ioc.serializers[ioc.DataType.PROPERTY] = this;
  }

  canBeUsedFor(value) {
    return value instanceof g.Property;
  }

  serialize(item, fullyQualifiedFormat = true) {
    if (item === undefined || item === null) {
      if (fullyQualifiedFormat) {
        return Buffer.from([this.ioc.DataType.PROPERTY, 0x01]);
      }
      return Buffer.concat([
        this.ioc.stringSerializer.serialize('', false), // {key}=''
        this.ioc.unspecifiedNullSerializer.serialize(null), // {value}=null
        this.ioc.unspecifiedNullSerializer.serialize(null), // {parent}=null
      ]);
    }

    const bufs = [];
    if (fullyQualifiedFormat) {
      bufs.push(Buffer.from([this.ioc.DataType.PROPERTY, 0x00]));
    }

    // {key}
    bufs.push(this.ioc.stringSerializer.serialize(item.key, false));

    // {value}
    bufs.push(this.ioc.anySerializer.serialize(item.value));

    // {parent}
    bufs.push(this.ioc.unspecifiedNullSerializer.serialize(null));

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
        if (type_code !== this.ioc.DataType.PROPERTY) {
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

      // {key} is a String value
      let key, key_len;
      try {
        ({ v: key, len: key_len } = this.ioc.stringSerializer.deserialize(cursor, false));
        len += key_len;
      } catch (err) {
        err.message = '{key}: ' + err.message;
        throw err;
      }
      cursor = cursor.slice(key_len);

      // {value} is a fully qualified typed value composed of {type_code}{type_info}{value_flag}{value}
      let value, value_len;
      try {
        ({ v: value, len: value_len } = this.ioc.anySerializer.deserialize(cursor));
        len += value_len;
      } catch (err) {
        err.message = '{value}: ' + err.message;
        throw err;
      }
      cursor = cursor.slice(value_len);

      // {parent} is a fully qualified typed value composed of {type_code}{type_info}{value_flag}{value} which is either an Edge or VertexProperty.
      // Note that as TinkerPop currently sends "references" only this value will always be null.
      let parent_len;
      try {
        ({ len: parent_len } = this.ioc.unspecifiedNullSerializer.deserialize(cursor));
        len += parent_len;
      } catch (err) {
        err.message = '{parent}: ' + err.message;
        throw err;
      }
      // TODO: should we verify that parent is null?
      cursor = cursor.slice(parent_len);

      const v = new g.Property(key, value);
      return { v, len };
    } catch (err) {
      throw this.ioc.utils.des_error({ serializer: this, args: arguments, cursor, err });
    }
  }
};
