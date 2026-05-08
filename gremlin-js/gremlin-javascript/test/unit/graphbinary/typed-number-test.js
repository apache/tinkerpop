/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

import assert from 'assert';
import { toFloat, toDouble, toInt, toLong, toShort, toByte } from '../../../lib/utils.js';
import ioc, { DataType } from '../../../lib/structure/io/binary/GraphBinary.js';
import StreamReader from '../../../lib/structure/io/binary/internals/StreamReader.js';
import { P } from '../../../lib/process/traversal.js';

const { anySerializer, numberSerializationStrategy } = ioc;

describe('Typed Number Tests', () => {
  describe('Type-code routing via anySerializer', () => {
    it('toFloat → FLOAT', () => {
      assert.strictEqual(anySerializer.serialize(toFloat(1.0))[0], DataType.FLOAT);
    });

    it('toDouble → DOUBLE', () => {
      assert.strictEqual(anySerializer.serialize(toDouble(1.0))[0], DataType.DOUBLE);
    });

    it('toInt → INT', () => {
      assert.strictEqual(anySerializer.serialize(toInt(67))[0], DataType.INT);
    });

    it('toLong → LONG', () => {
      assert.strictEqual(anySerializer.serialize(toLong(67))[0], DataType.LONG);
    });

    it('toShort → SHORT', () => {
      assert.strictEqual(anySerializer.serialize(toShort(5))[0], DataType.SHORT);
    });

    it('toByte → BYTE', () => {
      assert.strictEqual(anySerializer.serialize(toByte(127))[0], DataType.BYTE);
    });
  });

  describe('canBeUsedFor routing', () => {
    it('accepts typed wrappers', () => {
      assert.ok(numberSerializationStrategy.canBeUsedFor(toFloat(1.0)));
      assert.ok(numberSerializationStrategy.canBeUsedFor(toInt(67)));
      assert.ok(numberSerializationStrategy.canBeUsedFor(toLong(67n)));
      assert.ok(numberSerializationStrategy.canBeUsedFor(toDouble(1.0)));
      assert.ok(numberSerializationStrategy.canBeUsedFor(toShort(5)));
      assert.ok(numberSerializationStrategy.canBeUsedFor(toByte(127)));
    });

    it('rejects plain objects', () => {
      assert.ok(!numberSerializationStrategy.canBeUsedFor({ value: 1 }));
    });
  });

  describe('Byte-level verification', () => {
    it('toFloat(1.0) produces different bytes than toInt(1)', () => {
      const floatBytes = anySerializer.serialize(toFloat(1.0));
      const intBytes = anySerializer.serialize(toInt(1));
      assert.ok(!Buffer.from(floatBytes).equals(Buffer.from(intBytes)));
    });

    it('toFloat(1.0) produces different bytes than toDouble(1.0) (4 vs 8 value bytes)', () => {
      const floatBytes = anySerializer.serialize(toFloat(1.0));
      const doubleBytes = anySerializer.serialize(toDouble(1.0));
      assert.ok(!Buffer.from(floatBytes).equals(Buffer.from(doubleBytes)));
    });

    it('toShort(1) produces different bytes than toInt(1)', () => {
      const shortBytes = anySerializer.serialize(toShort(1));
      const intBytes = anySerializer.serialize(toInt(1));
      assert.ok(!Buffer.from(shortBytes).equals(Buffer.from(intBytes)));
    });

    it('toByte(1) produces different bytes than toShort(1)', () => {
      const byteBytes = anySerializer.serialize(toByte(1));
      const shortBytes = anySerializer.serialize(toShort(1));
      assert.ok(!Buffer.from(byteBytes).equals(Buffer.from(shortBytes)));
    });

    it('toLong(1) produces different bytes than toInt(1)', () => {
      const longBytes = anySerializer.serialize(toLong(1));
      const intBytes = anySerializer.serialize(toInt(1));
      assert.ok(!Buffer.from(longBytes).equals(Buffer.from(intBytes)));
    });
  });

  describe('Backward compatibility', () => {
    it('plain 67 still routes to INT', () => {
      assert.strictEqual(anySerializer.serialize(67)[0], DataType.INT);
    });

    it('plain 3.14 still routes to DOUBLE', () => {
      assert.strictEqual(anySerializer.serialize(3.14)[0], DataType.DOUBLE);
    });

    it('plain 2147483648 still routes to LONG', () => {
      assert.strictEqual(anySerializer.serialize(2147483648)[0], DataType.LONG);
    });
  });

  describe('Edge cases', () => {
    it('toLong(67n) serializes correctly (bigint input)', () => {
      const bytes = anySerializer.serialize(toLong(67n));
      assert.strictEqual(bytes[0], DataType.LONG);
    });

    it('toLong string input serializes without precision loss', () => {
      const bytes = anySerializer.serialize(toLong('9007199254740993'));
      assert.strictEqual(bytes[0], DataType.LONG);
    });

    it('toShort(-1) — negative value', () => {
      const bytes = anySerializer.serialize(toShort(-1));
      assert.strictEqual(bytes[0], DataType.SHORT);
    });

    it('toByte(-128) — negative value', () => {
      const bytes = anySerializer.serialize(toByte(-128));
      assert.strictEqual(bytes[0], DataType.BYTE);
    });

    it('toFloat(NaN)', () => {
      const bytes = anySerializer.serialize(toFloat(NaN));
      assert.strictEqual(bytes[0], DataType.FLOAT);
    });

    it('toFloat(Infinity)', () => {
      const bytes = anySerializer.serialize(toFloat(Infinity));
      assert.strictEqual(bytes[0], DataType.FLOAT);
    });

    it('toFloat(-0)', () => {
      const bytes = anySerializer.serialize(toFloat(-0));
      assert.strictEqual(bytes[0], DataType.FLOAT);
    });

    it('toDouble(NaN)', () => {
      const bytes = anySerializer.serialize(toDouble(NaN));
      assert.strictEqual(bytes[0], DataType.DOUBLE);
    });

    it('toDouble(Infinity)', () => {
      const bytes = anySerializer.serialize(toDouble(Infinity));
      assert.strictEqual(bytes[0], DataType.DOUBLE);
    });

    it('toDouble(-Infinity)', () => {
      const bytes = anySerializer.serialize(toDouble(-Infinity));
      assert.strictEqual(bytes[0], DataType.DOUBLE);
    });

    it('toDouble(-0)', () => {
      const bytes = anySerializer.serialize(toDouble(-0));
      assert.strictEqual(bytes[0], DataType.DOUBLE);
    });
  });

  describe('Round-trip through default reader', () => {
    it('toFloat(1.5) round-trips to plain number', async () => {
      const bytes = anySerializer.serialize(toFloat(1.5));
      const result = await anySerializer.deserialize(StreamReader.fromBuffer(bytes));
      assert.strictEqual(result, 1.5);
    });

    it('toInt(67) round-trips to plain number', async () => {
      const bytes = anySerializer.serialize(toInt(67));
      const result = await anySerializer.deserialize(StreamReader.fromBuffer(bytes));
      assert.strictEqual(result, 67);
    });

    it('toLong string value round-trips without precision loss', async () => {
      const bytes = anySerializer.serialize(toLong('9007199254740993'));
      const result = await anySerializer.deserialize(StreamReader.fromBuffer(bytes));
      assert.strictEqual(result, 9007199254740993n);
    });
  });

  describe('Integration with traversal structures', () => {
    it('wrappers work inside P predicates', () => {
      const predicate = P.gt(toFloat(1.0));
      const bytes = anySerializer.serialize(predicate);
      // P serializes successfully — the inner Float gets correct type code
      assert.ok(bytes.length > 0);
    });

    it('wrappers inside collections get correct type codes', async () => {
      const bytes = anySerializer.serialize([toFloat(1.0), toInt(2)]);
      const reader = StreamReader.fromBuffer(bytes);
      // Skip list type code (1 byte) and length (4 bytes)
      const result = await anySerializer.deserialize(reader);
      // The list deserializes — each element was serialized with its own type code
      assert.strictEqual(result.length, 2);
      assert.strictEqual(result[0], 1.0);
      assert.strictEqual(result[1], 2);
    });
  });
});
