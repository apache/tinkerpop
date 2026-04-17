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

export const END_OF_STREAM = Symbol('EndOfStream');

export default class MarkerSerializer {
  constructor(ioc) {
    this.ioc = ioc;
    this.ioc.serializers[ioc.DataType.MARKER] = this;
  }

  /**
   * @param {StreamReader} reader
   * @param {number} valueFlag - already consumed by AnySerializer
   * @returns {Promise<symbol>}
   */
  async deserializeValue(reader, valueFlag) {
    const value = await reader.readUInt8();
    if (value !== 0) {
      throw new Error(`unexpected marker value: ${value}`);
    }
    return END_OF_STREAM;
  }

  /**
   * Async fully-qualified deserialization from a StreamReader.
   * @param {StreamReader} reader
   * @returns {Promise<symbol|null>}
   */
  async deserialize(reader) {
    const type_code = await reader.readUInt8();
    if (type_code !== this.ioc.DataType.MARKER) {
      throw new Error(`MarkerSerializer: unexpected {type_code}=0x${type_code.toString(16)}`);
    }
    const value_flag = await reader.readUInt8();
    if (value_flag === 0x01) {
      return null;
    }
    if (value_flag !== 0x00) {
      throw new Error(`MarkerSerializer: unexpected {value_flag}=0x${value_flag.toString(16)}`);
    }
    return this.deserializeValue(reader, value_flag);
  }
}
