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

export default class UnspecifiedNullSerializer {
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

  /**
   * @param {StreamReader} reader
   * @param {number} valueFlag - already consumed by AnySerializer (always 0x01 for null)
   * @returns {Promise<null>}
   */
  // eslint-disable-next-line require-await
  async deserializeValue(reader, valueFlag) {
    throw new Error('UnspecifiedNull should always have value_flag=0x01');
  }
}
