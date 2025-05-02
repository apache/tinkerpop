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

'use strict';

const { TraversalStrategy } = require('../../../../process/traversal-strategy');
const { Buffer } = require('buffer');

module.exports = class ClassSerializer {
  constructor(ioc) {
    this.ioc = ioc;
    this.ioc.serializers[ioc.DataType.CLASS] = this;
  }

  canBeUsedFor(value) {
    return (
      typeof value === 'function' &&
      !!value.prototype &&
      !!value.prototype.constructor.name &&
      new value() instanceof TraversalStrategy
    );
  }

  serialize(item, fullyQualifiedFormat = true) {
    if (item === undefined || item === null) {
      if (fullyQualifiedFormat) {
        return Buffer.from([this.ioc.DataType.CLASS, 0x01]);
      }
      return this.ioc.intSerializer.serialize(0, false);
    }

    const bufs = [];
    if (fullyQualifiedFormat) {
      bufs.push(Buffer.from([this.ioc.DataType.CLASS, 0x00]));
    }

    const fqcn = new item().fqcn;
    const v = Buffer.from(fqcn, 'utf8');
    bufs.push(this.ioc.intSerializer.serialize(fqcn.length, false));
    bufs.push(v);

    return Buffer.concat(bufs);
  }
};
