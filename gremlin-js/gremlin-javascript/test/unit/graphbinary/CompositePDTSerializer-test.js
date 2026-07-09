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
import { CompositePDT } from '../../../lib/structure/graph.js';
import { PDTRegistry } from '../../../lib/structure/PDTRegistry.js';
import ioc, { DataType } from '../../../lib/structure/io/binary/GraphBinary.js';
import StreamReader from '../../../lib/structure/io/binary/internals/StreamReader.js';

const { anySerializer, compositePDTSerializer } = ioc;

async function roundTrip(value) {
  const bytes = anySerializer.serialize(value);
  return anySerializer.deserialize(StreamReader.fromBuffer(bytes));
}

describe('CompositePDTSerializer', () => {
  describe('round-trip: simple PDT', () => {
    it('serializes and deserializes a simple CompositePDT', async () => {
      const pdt = new CompositePDT('myType', { key1: 'value1', key2: 42 });
      const result = await roundTrip(pdt);
      assert.instanceOf(result, CompositePDT);
      assert.strictEqual(result.name, 'myType');
      assert.strictEqual(result.fields.key1, 'value1');
      assert.strictEqual(result.fields.key2, 42);
    });

    it('uses COMPOSITEPDT type code', () => {
      const pdt = new CompositePDT('test', { a: 1 });
      const bytes = anySerializer.serialize(pdt);
      assert.strictEqual(bytes[0], DataType.COMPOSITEPDT);
    });
  });

  describe('round-trip: nested PDT', () => {
    it('serializes and deserializes a PDT with nested PDT in fields', async () => {
      const inner = new CompositePDT('inner', { x: 'hello' });
      const outer = new CompositePDT('outer', { nested: inner, num: 99 });
      const result = await roundTrip(outer);
      assert.instanceOf(result, CompositePDT);
      assert.strictEqual(result.name, 'outer');
      assert.strictEqual(result.fields.num, 99);
      assert.instanceOf(result.fields.nested, CompositePDT);
      assert.strictEqual(result.fields.nested.name, 'inner');
      assert.strictEqual(result.fields.nested.fields.x, 'hello');
    });
  });

  describe('round-trip: null/undefined field value', () => {
    it('handles null field values', async () => {
      const pdt = new CompositePDT('withNull', { present: 'yes', absent: null });
      const result = await roundTrip(pdt);
      assert.instanceOf(result, CompositePDT);
      assert.strictEqual(result.name, 'withNull');
      assert.strictEqual(result.fields.present, 'yes');
      assert.strictEqual(result.fields.absent, null);
    });
  });

  describe('empty name rejected', () => {
    it('constructor rejects empty string name', () => {
      assert.throws(() => new CompositePDT('', { a: 1 }), /name cannot be null or empty/);
    });

    it('constructor rejects null name', () => {
      assert.throws(() => new CompositePDT(null, { a: 1 }), /name cannot be null or empty/);
    });

    it('constructor rejects undefined name', () => {
      assert.throws(() => new CompositePDT(undefined, { a: 1 }), /name cannot be null or empty/);
    });

    it('deserializer rejects null name from wire', async () => {
      // Manually craft bytes: type_code=0xf0, value_flag=0x00, then null string, then empty map
      const nullString = Buffer.from([DataType.STRING, 0x01]); // null string
      const emptyMap = Buffer.from([DataType.MAP, 0x00, 0x00, 0x00, 0x00, 0x00]); // map with 0 entries
      const bytes = Buffer.concat([
        Buffer.from([DataType.COMPOSITEPDT, 0x00]),
        nullString,
        emptyMap,
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
    it('returns true for CompositePDT instances', () => {
      assert.isTrue(compositePDTSerializer.canBeUsedFor(new CompositePDT('t', {})));
    });

    it('returns false for plain objects', () => {
      assert.isFalse(compositePDTSerializer.canBeUsedFor({ name: 'test' }));
    });

    it('returns false for strings', () => {
      assert.isFalse(compositePDTSerializer.canBeUsedFor('test'));
    });
  });

  describe('auto-hydration via pdtRegistry', () => {
    it('auto-hydrates when pdtRegistry is set on the reader', async () => {
      const registry = new PDTRegistry();
      registry.register('myType', {
        serialize: (obj) => obj,
        deserialize: (fields) => ({ hydrated: true, ...fields }),
      });

      const pdt = new CompositePDT('myType', { key1: 'value1', key2: 42 });
      const bytes = anySerializer.serialize(pdt);
      const reader = StreamReader.fromBuffer(bytes);
      reader.pdtRegistry = registry;
      const result = await anySerializer.deserialize(reader);

      assert.notInstanceOf(result, CompositePDT);
      assert.strictEqual(result.hydrated, true);
      assert.strictEqual(result.key1, 'value1');
      assert.strictEqual(result.key2, 42);
    });

    it('returns raw PDT when no pdtRegistry is set', async () => {
      const pdt = new CompositePDT('myType', { key1: 'value1' });
      const bytes = anySerializer.serialize(pdt);
      const result = await anySerializer.deserialize(StreamReader.fromBuffer(bytes));

      assert.instanceOf(result, CompositePDT);
      assert.strictEqual(result.name, 'myType');
    });
  });
});
