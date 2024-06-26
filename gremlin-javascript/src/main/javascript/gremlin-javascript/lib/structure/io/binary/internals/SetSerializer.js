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

import { Buffer } from 'buffer';
export default class SetSerializer {
  constructor(ioc, ID) {
    this.ioc = ioc;
    this.ID = ID;
    this.ioc.serializers[ID] = this;
  }

  canBeUsedFor(value) {
    return value instanceof Set;
  }

  serialize(item, fullyQualifiedFormat = true) {
    if (item === undefined || item === null) {
      if (fullyQualifiedFormat) {
        return Buffer.from([this.ID, 0x01]);
      }
      return Buffer.from([0x00, 0x00, 0x00, 0x00]); // {length} = 0
    }

    const bufs = [];
    if (fullyQualifiedFormat) {
      bufs.push(Buffer.from([this.ID, 0x00]));
    }

    // {length}
    let length = item.size;
    if (length < 0) {
      length = 0;
    }
    if (length > this.ioc.intSerializer.INT32_MAX) {
      throw new Error(`Set length=${length} is greater than supported max_length=${this.ioc.intSerializer.INT32_MAX}.`);
    }
    bufs.push(this.ioc.intSerializer.serialize(length, false));

    // {item_0}...{item_n}
    for (const i of item) {
      bufs.push(this.ioc.anySerializer.serialize(i));
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
        const typeCode = cursor.readUInt8();
        len++;
        if (typeCode !== this.ID) {
          throw new Error('unexpected {type_code}');
        }
        cursor = cursor.slice(1);

        if (cursor.length < 1) {
          throw new Error('{value_flag} is missing');
        }
        const valueFlag = cursor.readUInt8();
        len++;
        if (valueFlag === 1) {
          return { v: null, len };
        }
        if (valueFlag !== 0) {
          throw new Error('unexpected {value_flag}');
        }
        cursor = cursor.slice(1);
      }

      let length, lengthLen;
      try {
        ({ v: length, len: lengthLen } = this.ioc.intSerializer.deserialize(cursor, false));
        len += lengthLen;
      } catch (err) {
        err.message = '{length}: ' + err.message;
        throw err;
      }
      if (length < 0) {
        throw new Error('{length} is less than zero');
      }
      cursor = cursor.slice(lengthLen);

      const v = new Set();
      for (let i = 0; i < length; i++) {
        let value, valueLen;
        try {
          ({ v: value, len: valueLen } = this.ioc.anySerializer.deserialize(cursor));
          len += valueLen;
        } catch (err) {
          err.message = `{item_${i}}: ` + err.message;
          throw err;
        }
        cursor = cursor.slice(valueLen);
        v.add(value);
      }

      return { v, len };
    } catch (err) {
      throw this.ioc.utils.des_error({ serializer: this, args: arguments, cursor, err });
    }
  }
}
