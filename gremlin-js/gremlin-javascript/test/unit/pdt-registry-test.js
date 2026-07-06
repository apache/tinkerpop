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
import { CompositePDT, PrimitivePDT } from '../../lib/structure/graph.js';
import { PDTRegistry } from '../../lib/structure/PDTRegistry.js';
import Client from '../../lib/driver/client.js';
import Connection from '../../lib/driver/connection.js';

describe('PDTRegistry', () => {
  describe('#hydrate()', () => {
    it('should return a typed object when an adapter is registered', () => {
      const registry = new PDTRegistry();
      registry.register('GeoPoint', {
        serialize: (obj) => ({ lat: obj.lat, lon: obj.lon }),
        deserialize: (fields) => ({ type: 'GeoPoint', lat: fields.lat, lon: fields.lon }),
      });

      const pdt = new CompositePDT('GeoPoint', { lat: 37.7749, lon: -122.4194 });
      const result = registry.hydrate(pdt);

      assert.deepStrictEqual(result, { type: 'GeoPoint', lat: 37.7749, lon: -122.4194 });
    });

    it('should return the raw PDT when no adapter is registered', () => {
      const registry = new PDTRegistry();
      const pdt = new CompositePDT('Unknown', { foo: 'bar' });
      const result = registry.hydrate(pdt);

      assert.strictEqual(result, pdt);
      assert.instanceOf(result, CompositePDT);
    });

    it('should fall back gracefully when adapter throws', () => {
      const registry = new PDTRegistry();
      registry.register('Broken', {
        serialize: () => ({}),
        deserialize: () => { throw new Error('adapter error'); },
      });

      const pdt = new CompositePDT('Broken', { x: 1 });
      const warnings = [];
      const origWarn = console.warn;
      console.warn = (msg) => warnings.push(msg);
      try {
        const result = registry.hydrate(pdt);
        assert.strictEqual(result, pdt);
        assert.lengthOf(warnings, 1);
        assert.include(warnings[0], 'adapter error');
        assert.include(warnings[0], 'Broken');
      } finally {
        console.warn = origWarn;
      }
    });

    it('should recursively hydrate nested PDTs', () => {
      const registry = new PDTRegistry();
      registry.register('Address', {
        serialize: (obj) => obj,
        deserialize: (fields) => ({ type: 'Address', city: fields.city, zip: fields.zip }),
      });
      registry.register('Person', {
        serialize: (obj) => obj,
        deserialize: (fields) => ({ type: 'Person', name: fields.name, address: fields.address }),
      });

      const addressPdt = new CompositePDT('Address', { city: 'Portland', zip: '97201' });
      const personPdt = new CompositePDT('Person', { name: 'Alice', address: addressPdt });

      const result = registry.hydrate(personPdt);

      assert.deepStrictEqual(result, {
        type: 'Person',
        name: 'Alice',
        address: { type: 'Address', city: 'Portland', zip: '97201' },
      });
    });

    it('should return non-PDT values unchanged', () => {
      const registry = new PDTRegistry();
      assert.strictEqual(registry.hydrate('hello'), 'hello');
      assert.strictEqual(registry.hydrate(42), 42);
      assert.strictEqual(registry.hydrate(null), null);
    });

    it('should hydrate nested registered PDT inside unregistered outer', () => {
      const registry = new PDTRegistry();
      registry.register('Inner', {
        serialize: (obj) => ({ val: obj.val }),
        deserialize: (fields) => ({ type: 'Inner', val: fields.val }),
      });

      const innerPdt = new CompositePDT('Inner', { val: 42 });
      const outerPdt = new CompositePDT('Outer', { nested: innerPdt, plain: 'hello' });
      const result = registry.hydrate(outerPdt);

      assert.instanceOf(result, CompositePDT);
      assert.strictEqual(result.name, 'Outer');
      assert.deepStrictEqual(result.fields.nested, { type: 'Inner', val: 42 });
      assert.strictEqual(result.fields.plain, 'hello');
    });
  });

  describe('#hasAdapter()', () => {
    it('should return true for registered types', () => {
      const registry = new PDTRegistry();
      registry.register('Foo', { serialize: () => ({}), deserialize: (p) => p });
      assert.isTrue(registry.hasAdapter('Foo'));
      assert.isFalse(registry.hasAdapter('Bar'));
    });
  });

  describe('#getSerializer()', () => {
    it('should return the serialize function for registered types', () => {
      const registry = new PDTRegistry();
      const serFn = (obj) => ({ val: obj.val });
      registry.register('Custom', { serialize: serFn, deserialize: (p) => p });
      assert.strictEqual(registry.getSerializer('Custom'), serFn);
    });

    it('should return null for unregistered types', () => {
      const registry = new PDTRegistry();
      assert.isNull(registry.getSerializer('Missing'));
    });
  });
});

describe('pdtRegistry wiring through Client/Connection', () => {
  it('should set pdtRegistry on the reader when passed via Connection options', () => {
    const registry = new PDTRegistry();
    registry.register('GeoPoint', {
      serialize: (obj) => obj,
      deserialize: (fields) => ({ type: 'GeoPoint', ...fields }),
    });

    const conn = new Connection('http://localhost:8182', { pdtRegistry: registry });
    assert.strictEqual(conn._reader.pdtRegistry, registry);
  });

  it('should set pdtRegistry on the reader when passed via Client options', () => {
    const registry = new PDTRegistry();
    registry.register('GeoPoint', {
      serialize: (obj) => obj,
      deserialize: (fields) => ({ type: 'GeoPoint', ...fields }),
    });

    const client = new Client('http://localhost:8182', { pdtRegistry: registry });
    assert.strictEqual(client._connection._reader.pdtRegistry, registry);
  });

  it('should not leak pdtRegistry between connections', () => {
    const registry = new PDTRegistry();
    const conn1 = new Connection('http://localhost:8182', { pdtRegistry: registry });
    const conn2 = new Connection('http://localhost:8182');
    assert.isNull(conn2._reader.pdtRegistry);
    assert.strictEqual(conn1._reader.pdtRegistry, registry);
  });
});

describe('PDTRegistry - Primitive', () => {
  describe('#hydratePrimitive()', () => {
    it('should return a typed value when a primitive adapter is registered', () => {
      const registry = new PDTRegistry();
      registry.registerPrimitive('Uint32', {
        toValue: (obj) => String(obj),
        fromValue: (value) => parseInt(value, 10),
      });

      const pdt = new PrimitivePDT('Uint32', '42');
      const result = registry.hydratePrimitive(pdt);

      assert.strictEqual(result, 42);
    });

    it('should return the raw primitive PDT when no adapter is registered', () => {
      const registry = new PDTRegistry();
      const pdt = new PrimitivePDT('Unknown', 'xyz');
      const result = registry.hydratePrimitive(pdt);

      assert.strictEqual(result, pdt);
      assert.instanceOf(result, PrimitivePDT);
    });

    it('should fall back gracefully when adapter throws', () => {
      const registry = new PDTRegistry();
      registry.registerPrimitive('Broken', {
        toValue: () => '',
        fromValue: () => { throw new Error('adapter error'); },
      });

      const pdt = new PrimitivePDT('Broken', '1');
      const warnings = [];
      const origWarn = console.warn;
      console.warn = (msg) => warnings.push(msg);
      try {
        const result = registry.hydratePrimitive(pdt);
        assert.strictEqual(result, pdt);
        assert.lengthOf(warnings, 1);
        assert.include(warnings[0], 'adapter error');
        assert.include(warnings[0], 'Broken');
      } finally {
        console.warn = origWarn;
      }
    });

    it('should return non-PrimitivePDT values unchanged', () => {
      const registry = new PDTRegistry();
      assert.strictEqual(registry.hydratePrimitive('hello'), 'hello');
      assert.strictEqual(registry.hydratePrimitive(42), 42);
      assert.strictEqual(registry.hydratePrimitive(null), null);
    });

    it('should preserve leading zeros in opaque string value', () => {
      const registry = new PDTRegistry();
      registry.registerPrimitive('PaddedId', {
        toValue: (obj) => obj.id,
        fromValue: (value) => ({ id: value }),
      });

      const pdt = new PrimitivePDT('PaddedId', '007');
      const result = registry.hydratePrimitive(pdt);
      assert.deepStrictEqual(result, { id: '007' });
    });
  });

  describe('#hasPrimitiveAdapter()', () => {
    it('should return true for registered primitive types', () => {
      const registry = new PDTRegistry();
      registry.registerPrimitive('Uint32', { toValue: () => '', fromValue: (v) => v });
      assert.isTrue(registry.hasPrimitiveAdapter('Uint32'));
      assert.isFalse(registry.hasPrimitiveAdapter('Missing'));
    });
  });

  describe('#getPrimitiveAdapterByClass()', () => {
    it('should return the adapter entry for registered class', () => {
      const registry = new PDTRegistry();
      class Uint32 { constructor(v) { this.v = v; } }
      registry.registerPrimitive('Uint32', {
        toValue: (obj) => String(obj.v),
        fromValue: (value) => new Uint32(parseInt(value, 10)),
      }, Uint32);
      const entry = registry.getPrimitiveAdapterByClass(Uint32);
      assert.isNotNull(entry);
      assert.strictEqual(entry.typeName, 'Uint32');
      assert.strictEqual(entry.toValue(new Uint32(5)), '5');
    });

    it('should return null for unregistered class', () => {
      const registry = new PDTRegistry();
      class Unknown {}
      assert.isNull(registry.getPrimitiveAdapterByClass(Unknown));
    });
  });

  describe('composite hydrate with nested primitive PDT', () => {
    it('should hydrate nested primitive PDT inside composite fields', () => {
      const registry = new PDTRegistry();
      registry.registerPrimitive('Uint32', {
        toValue: (obj) => String(obj),
        fromValue: (value) => parseInt(value, 10),
      });
      registry.register('Measurement', {
        serialize: (obj) => obj,
        deserialize: (fields) => ({ type: 'Measurement', unit: fields.unit, value: fields.value }),
      });

      const primPdt = new PrimitivePDT('Uint32', '99');
      const compPdt = new CompositePDT('Measurement', { unit: 'kg', value: primPdt });
      const result = registry.hydrate(compPdt);

      assert.deepStrictEqual(result, { type: 'Measurement', unit: 'kg', value: 99 });
    });
  });
});
