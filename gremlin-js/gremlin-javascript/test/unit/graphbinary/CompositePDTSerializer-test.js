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
import { ProviderDefinedType } from '../../../lib/structure/graph.js';
import { ProviderDefinedTypeRegistry } from '../../../lib/structure/ProviderDefinedTypeRegistry.js';
import ioc, { DataType } from '../../../lib/structure/io/binary/GraphBinary.js';
import StreamReader from '../../../lib/structure/io/binary/internals/StreamReader.js';

const { anySerializer, compositePDTSerializer } = ioc;

async function roundTrip(value) {
  const bytes = anySerializer.serialize(value);
  return anySerializer.deserialize(StreamReader.fromBuffer(bytes));
}

describe('CompositePDTSerializer', () => {
  describe('round-trip: simple PDT', () => {
    it('serializes and deserializes a simple ProviderDefinedType', async () => {
      const pdt = new ProviderDefinedType('myType', { key1: 'value1', key2: 42 });
      const result = await roundTrip(pdt);
      assert.instanceOf(result, ProviderDefinedType);
      assert.strictEqual(result.name, 'myType');
      assert.strictEqual(result.properties.key1, 'value1');
      assert.strictEqual(result.properties.key2, 42);
    });

    it('uses COMPOSITEPDT type code', () => {
      const pdt = new ProviderDefinedType('test', { a: 1 });
      const bytes = anySerializer.serialize(pdt);
      assert.strictEqual(bytes[0], DataType.COMPOSITEPDT);
    });
  });

  describe('round-trip: nested PDT', () => {
    it('serializes and deserializes a PDT with nested PDT in properties', async () => {
      const inner = new ProviderDefinedType('inner', { x: 'hello' });
      const outer = new ProviderDefinedType('outer', { nested: inner, num: 99 });
      const result = await roundTrip(outer);
      assert.instanceOf(result, ProviderDefinedType);
      assert.strictEqual(result.name, 'outer');
      assert.strictEqual(result.properties.num, 99);
      assert.instanceOf(result.properties.nested, ProviderDefinedType);
      assert.strictEqual(result.properties.nested.name, 'inner');
      assert.strictEqual(result.properties.nested.properties.x, 'hello');
    });
  });

  describe('round-trip: null/undefined field value', () => {
    it('handles null property values', async () => {
      const pdt = new ProviderDefinedType('withNull', { present: 'yes', absent: null });
      const result = await roundTrip(pdt);
      assert.instanceOf(result, ProviderDefinedType);
      assert.strictEqual(result.name, 'withNull');
      assert.strictEqual(result.properties.present, 'yes');
      assert.strictEqual(result.properties.absent, null);
    });
  });

  describe('empty name rejected', () => {
    it('constructor rejects empty string name', () => {
      assert.throws(() => new ProviderDefinedType('', { a: 1 }), /name cannot be null or empty/);
    });

    it('constructor rejects null name', () => {
      assert.throws(() => new ProviderDefinedType(null, { a: 1 }), /name cannot be null or empty/);
    });

    it('constructor rejects undefined name', () => {
      assert.throws(() => new ProviderDefinedType(undefined, { a: 1 }), /name cannot be null or empty/);
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
    it('returns true for ProviderDefinedType instances', () => {
      assert.isTrue(compositePDTSerializer.canBeUsedFor(new ProviderDefinedType('t', {})));
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
      const registry = new ProviderDefinedTypeRegistry();
      registry.register('myType', {
        serialize: (obj) => obj,
        deserialize: (props) => ({ hydrated: true, ...props }),
      });

      const pdt = new ProviderDefinedType('myType', { key1: 'value1', key2: 42 });
      const bytes = anySerializer.serialize(pdt);
      const reader = StreamReader.fromBuffer(bytes);
      reader.pdtRegistry = registry;
      const result = await anySerializer.deserialize(reader);

      assert.notInstanceOf(result, ProviderDefinedType);
      assert.strictEqual(result.hydrated, true);
      assert.strictEqual(result.key1, 'value1');
      assert.strictEqual(result.key2, 42);
    });

    it('returns raw PDT when no pdtRegistry is set', async () => {
      const pdt = new ProviderDefinedType('myType', { key1: 'value1' });
      const bytes = anySerializer.serialize(pdt);
      const result = await anySerializer.deserialize(StreamReader.fromBuffer(bytes));

      assert.instanceOf(result, ProviderDefinedType);
      assert.strictEqual(result.name, 'myType');
    });
  });
});
