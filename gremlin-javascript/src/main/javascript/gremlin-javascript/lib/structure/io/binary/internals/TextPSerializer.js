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

module.exports = class TextPSerializer {
  constructor(ioc) {
    this.ioc = ioc;
    this.ioc.serializers[ioc.DataType.TEXTP] = this;
  }

  canBeUsedFor(value) {
    return value instanceof t.TextP;
  }

  serialize(item, fullyQualifiedFormat = true) {
    if (item === undefined || item === null) {
      if (fullyQualifiedFormat) {
        return Buffer.from([this.ioc.DataType.TEXTP, 0x01]);
      }
      const name = [0x00, 0x00, 0x00, 0x00]; // ''
      const values_length = [0x00, 0x00, 0x00, 0x00]; // 0
      return Buffer.from([...name, ...values_length]);
    }

    const bufs = [];
    if (fullyQualifiedFormat) {
      bufs.push(Buffer.from([this.ioc.DataType.TEXTP, 0x00]));
    }

    // {name} is a String
    bufs.push(this.ioc.stringSerializer.serialize(item.operator, false));

    // {values_length}{value_0}...{value_n}
    // It tries to resemble the same if-else structure as
    // GraphSON.TextPSerializer.serialize() does.
    let list;
    if (item.other === undefined || item.other === null) {
      // follows the same idea as for t.P
      if (Array.isArray(item.value)) {
        list = item.value;
      } else {
        list = [item.value];
      }
    } else {
      list = [item.value, item.other];
    }
    bufs.push(this.ioc.listSerializer.serialize(list, false));

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
        if (type_code !== this.ioc.DataType.TEXTP) {
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

      let name, name_len;
      try {
        ({ v: name, len: name_len } = this.ioc.stringSerializer.deserialize(cursor, false));
        len += name_len;
      } catch (err) {
        err.message = '{name}: ' + err.message;
        throw err;
      }
      cursor = cursor.slice(name_len);

      let values, values_len;
      try {
        ({ v: values, len: values_len } = this.ioc.listSerializer.deserialize(cursor, false));
        len += values_len;
      } catch (err) {
        err.message = '{values}: ' + err.message;
        throw err;
      }
      cursor = cursor.slice(values_len);

      if (values.length < 1) {
        return { v: new t.TextP(''), len };
      }

      let v;
      const TextP_static = t.TextP[name];
      if (typeof TextP_static === 'function') {
        v = TextP_static(...values); // it's better to follow existing logic which may depend on an operator name
      } else {
        v = new t.TextP(name, ...values);
      }

      return { v, len };
    } catch (err) {
      throw this.ioc.utils.des_error({ serializer: this, args: arguments, cursor, err });
    }
  }
};
