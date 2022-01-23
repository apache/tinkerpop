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

module.exports = class ListSerializer {

  constructor(ioc) {
    this.ioc = ioc;
    this.ioc.serializers[ioc.DataType.LIST] = this;
  }

  canBeUsedFor() {
    // TODO
  }

  serialize() {
    // TODO
  }

  deserialize(buffer, fullyQualifiedFormat=true) {
    let len = 0;
    let cursor = buffer;

    try {
      if (buffer === undefined || buffer === null || !(buffer instanceof Buffer))
        throw new Error('buffer is missing');
      if (buffer.length < 1)
        throw new Error('buffer is empty');

      if (fullyQualifiedFormat) {
        const type_code = cursor.readUInt8(); len++; cursor = cursor.slice(1);
        if (type_code !== this.ioc.DataType.LIST)
          throw new Error('unexpected {type_code}');

        if (cursor.length < 1)
          throw new Error('{value_flag} is missing');
        const value_flag = cursor.readUInt8(); len++; cursor = cursor.slice(1);
        if (value_flag === 1)
          return { v: null, len };
        if (value_flag !== 0)
          throw new Error('unexpected {value_flag}');
      }

      if (cursor.length < 4)
        throw new Error('unexpected {length} length');
      const length = cursor.readInt32BE(); len += 4; cursor = cursor.slice(4);
      if (length < 0)
        throw new Error('{length} is less than zero');

      const v = [];
      for (let i = 0; i < length; i++) {
        let value, value_len;
        try {
          ({ v: value, len: value_len } = this.ioc.anySerializer.deserialize(cursor));
          len += value_len; cursor = cursor.slice(value_len);
        } catch (e) {
          throw new Error(`{item_${i}}: ${e.message}`);
        }
        v.push(value);
      }

      return { v, len };
    }
    catch (e) {
      throw this.ioc.utils.des_error({ serializer: this, args: arguments, cursor, msg: e.message });
    }
  }
};
