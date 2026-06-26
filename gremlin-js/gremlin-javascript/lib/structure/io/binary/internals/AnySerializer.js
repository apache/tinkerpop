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

export default class AnySerializer {
  constructor(ioc, { postDeserialize } = {}) {
    this.ioc = ioc;
    this._postDeserialize = postDeserialize || null;

    // specifically ordered, the first canBeUsedFor=true wins
    this.serializers = [
      ioc.unspecifiedNullSerializer,
      ioc.numberSerializationStrategy,
      ioc.booleanSerializer,
      ioc.dateTimeSerializer,
      ioc.setSerializer,
      ioc.listSerializer,
      ioc.uuidSerializer,
      ioc.edgeSerializer,
      ioc.pathSerializer,
      ioc.propertySerializer,
      ioc.vertexSerializer,
      ioc.vertexPropertySerializer,
      ioc.graphSerializer,
      ioc.enumSerializer,
      ioc.stringSerializer,
      ioc.binarySerializer,
      ioc.compositePDTSerializer,
      ioc.treeSerializer,
      ioc.mapSerializer,
    ];
  }

  getSerializerCanBeUsedFor(item) {
    for (let i = 0; i < this.serializers.length; i++) {
      if (this.serializers[i].canBeUsedFor(item)) {
        return this.serializers[i];
      }
    }

    throw new Error(
      `No serializer found to support item where typeof(item)='${typeof item}' and String(item)='${String(item)}'.`,
    );
  }

  serialize(item, fullyQualifiedFormat = true) {
    return this.getSerializerCanBeUsedFor(item).serialize(item, fullyQualifiedFormat);
  }

  /**
   * Async deserialization from a StreamReader.
   * Reads type_code + value_flag, then dispatches to the appropriate serializer's deserializeValue().
   * @param {StreamReader} reader
   * @returns {Promise<any>}
   */
  async deserialize(reader) {
    const pos = reader.position;
    const type_code = await reader.readUInt8();
    const serializer = this.ioc.serializers[type_code];
    if (!serializer) {
      throw new Error(`AnySerializer: unknown {type_code}=0x${type_code.toString(16)} at position ${pos}`);
    }

    const value_flag = await reader.readUInt8();
    if (value_flag === 0x01) {
      return null;
    }
    if (value_flag !== 0x00 && value_flag !== 0x02) {
      throw new Error(`AnySerializer: unexpected {value_flag}=0x${value_flag.toString(16)} at position ${pos}`);
    }

    let result;
    try {
      result = await serializer.deserializeValue(reader, value_flag, type_code);
    } catch (err) {
      err.message = `${serializer.constructor.name}.deserializeValue() at position ${pos}: ${err.message}`;
      throw err;
    }

    if (this._postDeserialize) {
      return this._postDeserialize(result, type_code);
    }
    return result;
  }
}
