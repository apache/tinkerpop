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

import { Long, Int, Float, Double, Short, Byte, INT32_MIN, INT32_MAX } from '../../../../utils.js';

// Based on GraphSON NumberSerializer.serialize().
// It's tested by AnySerializer.serialize() tests.
export default class NumberSerializationStrategy {
  constructor(ioc) {
    this.ioc = ioc;
  }

  canBeUsedFor(value) {
    if (
      value instanceof Long ||
      value instanceof Int ||
      value instanceof Float ||
      value instanceof Double ||
      value instanceof Short ||
      value instanceof Byte
    ) {
      return true;
    }
    if (Number.isNaN(value) || value === Number.POSITIVE_INFINITY || value === Number.NEGATIVE_INFINITY) {
      return true;
    }
    if (typeof value === 'number') {
      return true;
    }
    if (typeof value === 'bigint') {
      return true;
    }

    return false;
  }

  serialize(item, fullyQualifiedFormat = true) {
    if (item instanceof Float) {
      return this.ioc.floatSerializer.serialize(item.value, fullyQualifiedFormat);
    }
    if (item instanceof Double) {
      return this.ioc.doubleSerializer.serialize(item.value, fullyQualifiedFormat);
    }
    if (item instanceof Int) {
      return this.ioc.intSerializer.serialize(item.value, fullyQualifiedFormat);
    }
    if (item instanceof Long) {
      return this.ioc.longSerializer.serialize(item.value, fullyQualifiedFormat);
    }
    if (item instanceof Short) {
      return this.ioc.shortSerializer.serialize(item.value, fullyQualifiedFormat);
    }
    if (item instanceof Byte) {
      return this.ioc.byteSerializer.serialize(item.value, fullyQualifiedFormat);
    }

    if (typeof item === 'number') {
      if (
        Number.isNaN(item) ||
        item === Number.POSITIVE_INFINITY ||
        item === Number.NEGATIVE_INFINITY ||
        !Number.isInteger(item) ||
        Object.is(item, -0)
      ) {
        return this.ioc.doubleSerializer.serialize(item, fullyQualifiedFormat);
      }

      if (item >= INT32_MIN && item <= INT32_MAX) {
        return this.ioc.intSerializer.serialize(item, fullyQualifiedFormat);
      }
      if (item >= Number.MIN_SAFE_INTEGER && item <= Number.MAX_SAFE_INTEGER) {
        return this.ioc.longSerializer.serialize(item, fullyQualifiedFormat);
      }
      // Integers outside safe range are huge doubles that only appear integral due to IEEE 754 precision loss
      return this.ioc.doubleSerializer.serialize(item, fullyQualifiedFormat);
    }

    if (typeof item === 'bigint') {
      return this.ioc.bigIntegerSerializer.serialize(item, fullyQualifiedFormat);
    }
  }
}
