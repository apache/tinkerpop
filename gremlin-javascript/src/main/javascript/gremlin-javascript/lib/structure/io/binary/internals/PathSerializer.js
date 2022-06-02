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

module.exports = class PathSerializer {
  constructor(ioc) {
    this.ioc = ioc;
    this.ioc.serializers[ioc.DataType.PATH] = this;
  }

  canBeUsedFor(value) {
    return value instanceof g.Path;
  }

  serialize(item, fullyQualifiedFormat = true) {
    if (item === undefined || item === null) {
      if (fullyQualifiedFormat) {
        return Buffer.from([this.ioc.DataType.PATH, 0x01]);
      }
      return Buffer.concat([
        this.ioc.listSerializer.serialize([]), // {labels}=[]
        this.ioc.listSerializer.serialize([]), // {objects}=[]
      ]);
    }

    const bufs = [];
    if (fullyQualifiedFormat) {
      bufs.push(Buffer.from([this.ioc.DataType.PATH, 0x00]));
    }

    // {labels} is a List in which each item is a Set of String
    bufs.push(this.ioc.listSerializer.serialize(item.labels));

    // {objects} is a List of fully qualified typed values
    bufs.push(this.ioc.listSerializer.serialize(item.objects));

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
        if (type_code !== this.ioc.DataType.PATH) {
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

      let labels, labels_len;
      try {
        ({ v: labels, len: labels_len } = this.ioc.listSerializer.deserialize(cursor));
        len += labels_len;
      } catch (err) {
        err.message = '{labels}: ' + err.message;
        throw err;
      }
      // TODO: should we check content of labels to make sure it's List< Set<String> > ?
      cursor = cursor.slice(labels_len);

      let objects, objects_len;
      try {
        ({ v: objects, len: objects_len } = this.ioc.listSerializer.deserialize(cursor));
        len += objects_len;
      } catch (err) {
        err.message = '{objects}: ' + err.message;
        throw err;
      }
      cursor = cursor.slice(objects_len);

      const v = new g.Path(labels, objects);
      return { v, len };
    } catch (err) {
      throw this.ioc.utils.des_error({ serializer: this, args: arguments, cursor, err });
    }
  }
};
