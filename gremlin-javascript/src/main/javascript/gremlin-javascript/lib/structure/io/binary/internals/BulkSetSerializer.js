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

module.exports = class BulkSetSerializer {
  constructor(ioc) {
    this.ioc = ioc;
    this.ioc.serializers[ioc.DataType.BULKSET] = this;
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
        cursor = cursor.slice(1);
        if (type_code !== this.ioc.DataType.BULKSET) {
          throw new Error('unexpected {type_code}');
        }

        if (cursor.length < 1) {
          throw new Error('{value_flag} is missing');
        }
        const value_flag = cursor.readUInt8();
        len++;
        cursor = cursor.slice(1);
        if (value_flag === 1) {
          return { v: null, len };
        }
        if (value_flag !== 0) {
          throw new Error('unexpected {value_flag}');
        }
      }

      // {length}
      let length, length_len;
      try {
        ({ v: length, len: length_len } = this.ioc.intSerializer.deserialize(cursor, false));
        len += length_len;
        cursor = cursor.slice(length_len);
      } catch (e) {
        throw new Error(`{length}: ${e.message}`);
      }
      if (length < 0) {
        throw new Error('{length} is less than zero');
      }

      // Official GraphBinary 1.0 spec:
      // If the implementing language does not have a BulkSet object to deserialize into,
      // this format can be coerced to a List and still be considered compliant with Gremlin.
      // Simply "expand the bulk" by adding the item to the List the number of times specified by the bulk.

      // {item_0}...{item_n}
      let v = new Array();
      for (let i = 0; i < length; i++) {
        let value, value_len;
        try {
          ({ v: value, len: value_len } = this.ioc.anySerializer.deserialize(cursor));
          len += value_len;
          cursor = cursor.slice(value_len);
        } catch (e) {
          throw new Error(`{item_${i}} value: ${e.message}`);
        }

        let bulk, bulk_len;
        try {
          ({ v: bulk, len: bulk_len } = this.ioc.longSerializer.deserialize(cursor, false));
          len += bulk_len;
          cursor = cursor.slice(bulk_len);
        } catch (e) {
          throw new Error(`{item_${i}} bulk: ${e.message}`);
        }
        if (bulk < 0) {
          throw new Error(`{item_${i}}: bulk is less than zero`);
        }
        if (bulk > 4294967295) {
          // arrayLength of Array() constructor is expected to be an integer between 0 and 2^32 - 1
          throw new Error(`{item_${i}}: bulk is greater than 2^32-1`);
        }

        bulk = Number(bulk); // coersion to Number is required for BigInt if LongSerializerNg is used
        const item = new Array(bulk).fill(value);
        v = v.concat(item);
      }

      return { v, len };
    } catch (e) {
      throw this.ioc.utils.des_error({ serializer: this, args: arguments, cursor, msg: e.message });
    }
  }
};
