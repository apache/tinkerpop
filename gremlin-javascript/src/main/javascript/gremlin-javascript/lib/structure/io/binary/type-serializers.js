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

const DataType = require('./data-type');

class IntSerializer {

  static serialize(item, fullyQualifiedFormat = true) {
    if (item === undefined || item === null)
      if (fullyQualifiedFormat)
        return Buffer.from([DataType.INT, 0x01]);
      else
        return Buffer.from([0x00, 0x00, 0x00, 0x00]);

    const v = Buffer.alloc(4);
    v.writeInt32BE(item); // TODO: what if item is not within int32 limits

    return Buffer.from([
      ...(fullyQualifiedFormat ? [DataType.INT, 0x00] : []),
      ...v,
    ]);
  }

  static deserialize(buffer) {
    // TODO
  }

  static canBeUsedFor(value) {
    // TODO
  }
}

class StringSerializer {

  static serialize(item, fullyQualifiedFormat = true) {
    if (item === undefined || item === null)
      if (fullyQualifiedFormat)
        return Buffer.from([DataType.STRING, 0x01]);
      else
        return IntSerializer.serialize(0, false);

    const v = Buffer.from(item, 'utf8');
    return Buffer.from([
      ...(fullyQualifiedFormat ? [DataType.STRING, 0x00] : []),
      ...IntSerializer.serialize(v.length, false),
      ...v,
    ]);
  }

  static deserialize(buffer) {
    // TODO
  }

  static canBeUsedFor(value) {
    return (typeof value === 'string');
  }
}

class MapSerializer {
  // TODO
}

class UuidSerializer {
  // TODO
}

module.exports = {
  IntSerializer,
  StringSerializer,
  MapSerializer,
  UuidSerializer,
};
