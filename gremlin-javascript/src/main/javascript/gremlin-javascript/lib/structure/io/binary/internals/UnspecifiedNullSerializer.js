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

module.exports = class UnspecifiedNullSerializer {
  constructor(ioc) {
    this.ioc = ioc;
    this.ioc.serializers[ioc.DataType.UNSPECIFIED_NULL] = this;
  }

  canBeUsedFor(value) {
    return value === null || value === undefined;
  }

  serialize(item) {
    // fullyQualifiedFormat always is true
    return Buffer.from([this.ioc.DataType.UNSPECIFIED_NULL, 0x01]);
  }

  deserialize(buffer) {
    // fullyQualifiedFormat always is true
    let len = 0;
    let cursor = buffer;

    try {
      if (buffer === undefined || buffer === null || !(buffer instanceof Buffer)) {
        throw new Error('buffer is missing');
      }
      if (buffer.length < 1) {
        throw new Error('buffer is empty');
      }

      const type_code = cursor.readUInt8();
      len++;
      if (type_code !== this.ioc.DataType.UNSPECIFIED_NULL) {
        throw new Error('unexpected {type_code}');
      }
      cursor = cursor.slice(1);

      if (cursor.length < 1) {
        throw new Error('{value_flag} is missing');
      }
      const value_flag = cursor.readUInt8();
      len++;
      if (value_flag !== 1) {
        throw new Error('unexpected {value_flag}');
      }
      cursor = cursor.slice(1);

      return { v: null, len };
    } catch (err) {
      throw this.ioc.utils.des_error({ serializer: this, args: arguments, cursor, err });
    }
  }
};
