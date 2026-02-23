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
import { direction, t as _t, EnumValue } from '../../../../process/traversal.js';

export default class EnumSerializer {
  constructor(ioc) {
    this.ioc = ioc;

    // process/traversal.js:toEnum() lowercases element names (e.g. OUT => out) for end users' convenience
    // but we need original mapping as is for wire format lookup
    const to_orig_enum = (obj) => {
      const r = {};
      Object.values(obj).forEach((e) => (r[e.elementName] = e));
      return r;
    };

    this.types = {
      Direction: { code: ioc.DataType.DIRECTION, enum: to_orig_enum(direction) },
      T: { code: ioc.DataType.T, enum: to_orig_enum(_t) },
    };

    this.ioc.serializers[ioc.DataType.DIRECTION] = this;
    this.ioc.serializers[ioc.DataType.T] = this;
  }

  canBeUsedFor(value) {
    return value instanceof EnumValue && (value.typeName === 'Direction' || value.typeName === 'T');
  }

  serialize(item, fullyQualifiedFormat = true) {
    const type = this.types[item.typeName];
    if (item.elementName === undefined || item.elementName === null) {
      if (fullyQualifiedFormat) {
        return Buffer.from([type.code, 0x01]);
      }
      return this.ioc.stringSerializer.serialize(null, false);
    }

    const bufs = [];
    if (fullyQualifiedFormat) {
      bufs.push(Buffer.from([type.code, 0x00]));
    }
    bufs.push(this.ioc.stringSerializer.serialize(item.elementName, true));

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

      let typeName;
      if (fullyQualifiedFormat) {
        const type_code = cursor.readUInt8();
        len++;
        if (type_code === this.ioc.DataType.DIRECTION) {
          typeName = 'Direction';
        } else if (type_code === this.ioc.DataType.T) {
          typeName = 'T';
        } else {
          throw new Error(`unexpected {type_code}=${type_code}`);
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

      let elementName, elementName_len;
      try {
        ({ v: elementName, len: elementName_len } = this.ioc.stringSerializer.deserialize(cursor, true));
        len += elementName_len;
      } catch (err) {
        err.message = 'elementName: ' + err.message;
        throw err;
      }

      let v;
      if (typeName) {
        v = this.types[typeName].enum[elementName];
      } else {
        v = new EnumValue(undefined, elementName);
      }

      return { v, len };
    } catch (err) {
      throw this.ioc.utils.des_error({ serializer: this, args: arguments, cursor, err });
    }
  }
}
