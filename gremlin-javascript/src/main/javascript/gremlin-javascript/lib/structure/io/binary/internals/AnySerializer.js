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

module.exports = class AnySerializer {
  constructor(ioc) {
    this.ioc = ioc;

    // specifically ordered, the first canBeUsedFor=true wins
    this.serializers = [
      ioc.unspecifiedNullSerializer,

      ioc.numberSerializationStrategy,

      ioc.booleanSerializer,
      ioc.dateSerializer,
      ioc.bytecodeSerializer,
      ioc.pSerializer,
      ioc.traverserSerializer,
      ioc.enumSerializer,
      ioc.listSerializer,
      ioc.uuidSerializer,
      ioc.edgeSerializer,
      ioc.pathSerializer,
      ioc.propertySerializer,
      ioc.vertexSerializer,
      ioc.vertexPropertySerializer,
      ioc.stringSerializer,
      ioc.textPSerializer,
      ioc.traversalStrategySerializer,

      ioc.byteBufferSerializer, // Buffer instance
      ioc.lambdaSerializer, // any function
      ioc.mapSerializer, // Map or any Object
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

  deserialize(buffer) {
    // obviously, fullyQualifiedFormat always is true
    try {
      if (buffer === undefined || buffer === null || !(buffer instanceof Buffer)) {
        throw new Error('buffer is missing');
      }
      if (buffer.length < 1) {
        throw new Error('buffer is empty');
      }

      const type_code = buffer.readUInt8();
      const serializer = this.ioc.serializers[type_code];
      if (!serializer) {
        throw new Error('unknown {type_code}');
      }

      return serializer.deserialize(buffer);
    } catch (err) {
      throw this.ioc.utils.des_error({ serializer: this, args: arguments, err });
    }
  }
};
