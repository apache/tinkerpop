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

module.exports = class VertexSerializer {
  constructor(ioc) {
    this.ioc = ioc;
    this.ioc.serializers[ioc.DataType.VERTEX] = this;
  }

  canBeUsedFor(value) {
    return value instanceof g.Vertex;
  }

  serialize(item, fullyQualifiedFormat = true) {
    if (item === undefined || item === null) {
      if (fullyQualifiedFormat) {
        return Buffer.from([this.ioc.DataType.VERTEX, 0x01]);
      }
      const id = [0x03, 0x00, 0x00, 0x00, 0x00, 0x00]; // String ''
      const label = [0x00, 0x00, 0x00, 0x00]; // ''
      const properties = [0xfe, 0x01]; // null
      return Buffer.from([...id, ...label, ...properties]);
    }

    const bufs = [];
    if (fullyQualifiedFormat) {
      bufs.push(Buffer.from([this.ioc.DataType.VERTEX, 0x00]));
    }

    // {id}
    bufs.push(this.ioc.anySerializer.serialize(item.id));

    // {label}
    bufs.push(this.ioc.stringSerializer.serialize(item.label, false));

    // {properties}
    bufs.push(this.ioc.anySerializer.serialize(item.properties));

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
        if (type_code !== this.ioc.DataType.VERTEX) {
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

      let id, id_len;
      try {
        ({ v: id, len: id_len } = this.ioc.anySerializer.deserialize(cursor));
        len += id_len;
      } catch (err) {
        err.message = '{id}: ' + err.message;
        throw err;
      }
      cursor = cursor.slice(id_len);

      let label, label_len;
      try {
        ({ v: label, len: label_len } = this.ioc.stringSerializer.deserialize(cursor, false));
        len += label_len;
      } catch (err) {
        err.message = '{label}: ' + err.message;
        throw err;
      }
      cursor = cursor.slice(label_len);

      let properties, properties_len;
      try {
        ({ v: properties, len: properties_len } = this.ioc.anySerializer.deserialize(cursor));
        len += properties_len;
      } catch (err) {
        err.message = '{properties}: ' + err.message;
        throw err;
      }
      cursor = cursor.slice(properties_len);

      const v = new g.Vertex(id, label, properties);
      return { v, len };
    } catch (err) {
      throw this.ioc.utils.des_error({ serializer: this, args: arguments, cursor, err });
    }
  }
};
