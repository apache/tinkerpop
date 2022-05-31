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

// Based on GraphSON NumberSerializer.serialize().
// It's tested by AnySerializer.serialize() tests.
module.exports = class NumberSerializationStrategy {
  constructor(ioc) {
    this.ioc = ioc;
  }

  canBeUsedFor(value) {
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
    if (typeof item === 'number') {
      if (
        Number.isNaN(item) ||
        item === Number.POSITIVE_INFINITY ||
        item === Number.NEGATIVE_INFINITY ||
        !Number.isInteger(item)
      ) {
        return this.ioc.doubleSerializer.serialize(item, fullyQualifiedFormat);
      }

      if (item >= -2147483648 && item <= 2147483647) {
        // INT32_MIN/MAX
        return this.ioc.intSerializer.serialize(item, fullyQualifiedFormat);
      }
      return this.ioc.longSerializer.serialize(item, fullyQualifiedFormat);
    }

    if (typeof item === 'bigint') {
      return this.ioc.bigIntegerSerializer.serialize(item, fullyQualifiedFormat);
    }
  }
};
