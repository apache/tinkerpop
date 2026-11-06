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

import { assert } from 'chai';
import { PrimitivePDT, CompositePDT } from '../../../lib/structure/graph.js';
import { PDTRegistry } from '../../../lib/structure/PDTRegistry.js';
import ioc, { DataType } from '../../../lib/structure/io/binary/GraphBinary.js';
import StreamReader from '../../../lib/structure/io/binary/internals/StreamReader.js';

const { anySerializer, primitivePDTSerializer } = ioc;

async function roundTrip(value) {
  const bytes = anySerializer.serialize(value);
  return anySerializer.deserialize(StreamReader.fromBuffer(bytes));
}

describe('PrimitivePDTSerializer', () => {
  describe('round-trip: simple primitive PDT', () => {
    it('serializes and deserializes a simple PrimitivePDT', async () => {
      const pdt = new PrimitivePDT('Uint32', '42');
      const result = await roundTrip(pdt);
      assert.instanceOf(result, PrimitivePDT);
      assert.strictEqual(result.name, 'Uint32');
      assert.strictEqual(result.value, '42');
    });

    it('uses PRIMITIVEPDT type code', () => {
      const pdt = new PrimitivePDT('Uint32', '123');
      const bytes = anySerializer.serialize(pdt);
      assert.strictEqual(bytes[0], DataType.PRIMITIVEPDT);
    });
  });

  describe('round-trip: opaque string values', () => {
    it('handles leading zeros (preserved as string)', async () => {
      const pdt = new PrimitivePDT('TinkerId', '007');
      const result = await roundTrip(pdt);
      assert.instanceOf(result, PrimitivePDT);
      assert.strictEqual(result.value, '007');
    });

    it('handles large numbers', async () => {
      const pdt = new PrimitivePDT('BigNum', '99999999999999999999999999');
      const result = await roundTrip(pdt);
      assert.strictEqual(result.value, '99999999999999999999999999');
    });

    it('handles non-numeric values', async () => {
      const pdt = new PrimitivePDT('CustomId', 'abc-def-123');
      const result = await roundTrip(pdt);
      assert.strictEqual(result.value, 'abc-def-123');
    });

    it('handles empty string value', async () => {
      const pdt = new PrimitivePDT('Empty', '');
      const result = await roundTrip(pdt);
      assert.instanceOf(result, PrimitivePDT);
      assert.strictEqual(result.name, 'Empty');
      assert.strictEqual(result.value, '');
    });
  });

  describe('empty name rejected', () => {
    it('constructor rejects empty string name', () => {
      assert.throws(() => new PrimitivePDT('', '42'), /name cannot be null or empty/);
    });

    it('constructor rejects null name', () => {
      assert.throws(() => new PrimitivePDT(null, '42'), /name cannot be null or empty/);
    });

    it('constructor rejects undefined name', () => {
      assert.throws(() => new PrimitivePDT(undefined, '42'), /name cannot be null or empty/);
    });

    it('constructor rejects null value', () => {
      assert.throws(() => new PrimitivePDT('Uint32', null), /value cannot be null/);
    });

    it('deserializer rejects null name from wire', async () => {
      const nullString = Buffer.from([DataType.STRING, 0x01]);
      const valueString = Buffer.from([DataType.STRING, 0x00, 0x00, 0x00, 0x00, 0x02, 0x34, 0x32]); // "42"
      const bytes = Buffer.concat([
        Buffer.from([DataType.PRIMITIVEPDT, 0x00]),
        nullString,
        valueString,
      ]);
      try {
        await anySerializer.deserialize(StreamReader.fromBuffer(bytes));
        assert.fail('should have thrown');
      } catch (e) {
        assert.match(e.message, /name cannot be null or empty/);
      }
    });
  });

  describe('canBeUsedFor', () => {
    it('returns true for PrimitivePDT instances', () => {
      assert.isTrue(primitivePDTSerializer.canBeUsedFor(new PrimitivePDT('t', '1')));
    });

    it('returns false for plain objects', () => {
      assert.isFalse(primitivePDTSerializer.canBeUsedFor({ name: 'test', value: '1' }));
    });

    it('returns false for strings', () => {
      assert.isFalse(primitivePDTSerializer.canBeUsedFor('test'));
    });

    it('returns false for composite CompositePDT', () => {
      assert.isFalse(primitivePDTSerializer.canBeUsedFor(new CompositePDT('t', { a: 1 })));
    });
  });

  describe('auto-hydration via pdtRegistry', () => {
    it('auto-hydrates when pdtRegistry is set on the reader', async () => {
      const registry = new PDTRegistry();
      registry.registerPrimitive('Uint32', {
        toValue: (obj) => String(obj),
        fromValue: (value) => parseInt(value, 10),
      });

      const pdt = new PrimitivePDT('Uint32', '42');
      const bytes = anySerializer.serialize(pdt);
      const reader = StreamReader.fromBuffer(bytes);
      reader.pdtRegistry = registry;
      const result = await anySerializer.deserialize(reader);

      assert.notInstanceOf(result, PrimitivePDT);
      assert.strictEqual(result, 42);
    });

    it('returns raw primitive PDT when no pdtRegistry is set', async () => {
      const pdt = new PrimitivePDT('Uint32', '42');
      const bytes = anySerializer.serialize(pdt);
      const result = await anySerializer.deserialize(StreamReader.fromBuffer(bytes));

      assert.instanceOf(result, PrimitivePDT);
      assert.strictEqual(result.name, 'Uint32');
      assert.strictEqual(result.value, '42');
    });

    it('returns raw primitive PDT when no adapter registered for that type', async () => {
      const registry = new PDTRegistry();
      const pdt = new PrimitivePDT('Unknown', 'xyz');
      const bytes = anySerializer.serialize(pdt);
      const reader = StreamReader.fromBuffer(bytes);
      reader.pdtRegistry = registry;
      const result = await anySerializer.deserialize(reader);

      assert.instanceOf(result, PrimitivePDT);
      assert.strictEqual(result.name, 'Unknown');
      assert.strictEqual(result.value, 'xyz');
    });
  });
});
