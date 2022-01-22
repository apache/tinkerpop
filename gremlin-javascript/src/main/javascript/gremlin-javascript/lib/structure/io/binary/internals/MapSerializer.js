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

  constructor(ioc) {
    this.ioc = ioc;
  }

  canBeUsedFor(value) {
    return (typeof value === 'object');
  }

  serialize(item, fullyQualifiedFormat = true) {
    if (item === undefined || item === null)
      if (fullyQualifiedFormat)
        return Buffer.from([this.ioc.DataType.MAP, 0x01]);
      else
        return this.ioc.intSerializer.serialize(0, false);

    const keys = Object.keys(item);
    let map_length = keys.length;
    if (map_length < 0)
      map_length = 0;
    else if (map_length > this.ioc.intSerializer.INT32_MAX)
      map_length = this.ioc.intSerializer.INT32_MAX; // TODO: is it expected to be silenced?

    const bufs = [];
    if (fullyQualifiedFormat)
      bufs.push( Buffer.from([this.ioc.DataType.MAP, 0x00]) );
    bufs.push( this.ioc.intSerializer.serialize(map_length, false) );
    for (let i = 0; i < map_length; i++) {
      const key = keys[i];
      const value = item[key];
      bufs.push(
        this.ioc.anySerializer.serialize(key),
        this.ioc.anySerializer.serialize(value),
      );
    }
    return Buffer.concat(bufs);
  }

  deserialize(buffer) {
    // TODO
  }

}
