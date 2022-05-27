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

// TODO: it has room for performance improvements
module.exports = class BigIntegerSerializer {

  constructor(ioc) {
    this.ioc = ioc;
    this.ioc.serializers[ioc.DataType.BIGINTEGER] = this;
  }

  canBeUsedFor(value) {
    // it's not expected to be used directly, see NumberSerializationStrategy
    return (typeof(value) === 'bigint');
  }

  serialize(item, fullyQualifiedFormat=true) {
    if (item === undefined || item === null)
      if (fullyQualifiedFormat)
        return Buffer.from([this.ioc.DataType.BIGINTEGER, 0x01]);
      else
        return Buffer.from([0x00,0x00,0x00,0x01, 0x00]);

    const bufs = [];
    if (fullyQualifiedFormat)
      bufs.push( Buffer.from([this.ioc.DataType.BIGINTEGER, 0x00]) );

    let v;

    // positive number
    if (item >= 0) {
      let hex_str = item.toString(16);
      if (hex_str.length % 2 !== 0)
        hex_str = '0' + hex_str; // Buffer.from() expects exactly two hex digits per byte
      if (Number.parseInt(hex_str[0], 16) > 7)
        hex_str = '00' + hex_str; // to keep sign bit unset for positive number
      v = Buffer.from(hex_str, 'hex');
    }

    // negative number
    else {
      let hex_str = (-item).toString(16);

      const bytes = (hex_str.length + hex_str.length%2) / 2; // number's size in bytes
      let N = BigInt(bytes) * 8n; // number's size in bits
      const INTN_MIN = - (2n ** (N-1n)); // minimum negative number we can keep within N bits
      if (item < INTN_MIN)
        N += 8n; // to provide room for smaller negative number

      const twos_complement = 2n ** N + item;
      hex_str = twos_complement.toString(16);
      if (hex_str.length % 2 !== 0)
        hex_str = '0' + hex_str; // Buffer.from() expects exactly two hex digits per byte

      v = Buffer.from(hex_str, 'hex');
    }

    bufs.push( this.ioc.intSerializer.serialize(v.length, false) );
    bufs.push(v);

    return Buffer.concat(bufs);
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
        if (type_code !== this.ioc.DataType.BIGINTEGER)
          throw new Error('unexpected {type_code}');

        if (cursor.length < 1)
          throw new Error('{value_flag} is missing');
        const value_flag = cursor.readUInt8(); len++; cursor = cursor.slice(1);
        if (value_flag === 1)
          return { v: null, len };
        if (value_flag !== 0)
          throw new Error('unexpected {value_flag}');
      }

      // {length}
      let length, length_len;
      try {
        ({ v: length, len: length_len } = this.ioc.intSerializer.deserialize(cursor, false));
        len += length_len; cursor = cursor.slice(length_len);
      } catch (e) {
        throw new Error(`{length}: ${e.message}`);
      }

      if (length < 1)
        throw new Error(`{length}=${length} is less than one`);

      cursor = cursor.slice(0, length);
      len += length;

      let v = BigInt(`0x${cursor.toString('hex')}`);
      if ((cursor[0] & 0x80) === 0x80) // if sign bit is set
        v = BigInt.asIntN(length*8, v); // now we get expected negative number

      return { v, len };
    }
    catch (e) {
      throw this.ioc.utils.des_error({ serializer: this, args: arguments, cursor, msg: e.message });
    }
  }

};
