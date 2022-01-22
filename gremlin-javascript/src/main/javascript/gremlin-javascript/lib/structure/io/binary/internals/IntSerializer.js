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

module.exports = class {

  static INT32_MIN = -2147483648;
  static INT32_MAX = 2147483647;

  constructor(ioc) {
    this.ioc = ioc;
  }

  canBeUsedFor(value) {
    if (typeof value !== 'number')
      return false;
    if (value < this.INT32_MIN || value > this.INT32_MAX)
      return false;
    return true;
  }

  serialize(item, fullyQualifiedFormat = true) {
    if (item === undefined || item === null)
      if (fullyQualifiedFormat)
        return Buffer.from([this.ioc.DataType.INT, 0x01]);
      else
        return Buffer.from([0x00, 0x00, 0x00, 0x00]);

    // TODO: this and other serializers could be optimised, e.g. to allocate a buf once, instead of multiple bufs to concat, etc
    const bufs = [];
    if (fullyQualifiedFormat)
      bufs.push( Buffer.from([this.ioc.DataType.INT, 0x00]) );
    const v = Buffer.alloc(4);
    v.writeInt32BE(item); // TODO: what if item is not within int32 limits, for now writeInt32BE would error
    bufs.push(v);

    return Buffer.concat(bufs);
  }

  deserialize(buffer, fullyQualifiedFormat=true) {
    try {
      if (buffer === undefined || buffer === null || !(buffer instanceof Buffer))
        throw new Error('buffer is missing');
      if (buffer.length < 1)
        throw new Error('buffer is empty');

      let len = 0;
      let cursor = buffer;

      if (fullyQualifiedFormat) {
        const type_code = cursor.readUInt8(); len++; cursor = cursor.slice(1);
        if (type_code !== this.ioc.DataType.INT)
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
        throw new Error('unexpected {value} length');
      len += 4;

      const v = cursor.readInt32BE();
      return { v, len };
    } catch (e) {
      throw this.ioc.utils.des_error({ des: this.name, args: arguments, msg: e.message });
    }
  }

}
