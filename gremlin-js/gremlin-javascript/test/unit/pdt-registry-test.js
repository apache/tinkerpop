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
import { ProviderDefinedType } from '../../lib/structure/graph.js';
import { ProviderDefinedTypeRegistry } from '../../lib/structure/ProviderDefinedTypeRegistry.js';
import Client from '../../lib/driver/client.js';
import Connection from '../../lib/driver/connection.js';

describe('ProviderDefinedTypeRegistry', () => {
  describe('#hydrate()', () => {
    it('should return a typed object when an adapter is registered', () => {
      const registry = new ProviderDefinedTypeRegistry();
      registry.register('GeoPoint', {
        serialize: (obj) => ({ lat: obj.lat, lon: obj.lon }),
        deserialize: (props) => ({ type: 'GeoPoint', lat: props.lat, lon: props.lon }),
      });

      const pdt = new ProviderDefinedType('GeoPoint', { lat: 37.7749, lon: -122.4194 });
      const result = registry.hydrate(pdt);

      assert.deepStrictEqual(result, { type: 'GeoPoint', lat: 37.7749, lon: -122.4194 });
    });

    it('should return the raw PDT when no adapter is registered', () => {
      const registry = new ProviderDefinedTypeRegistry();
      const pdt = new ProviderDefinedType('Unknown', { foo: 'bar' });
      const result = registry.hydrate(pdt);

      assert.strictEqual(result, pdt);
      assert.instanceOf(result, ProviderDefinedType);
    });

    it('should fall back gracefully when adapter throws', () => {
      const registry = new ProviderDefinedTypeRegistry();
      registry.register('Broken', {
        serialize: () => ({}),
        deserialize: () => { throw new Error('adapter error'); },
      });

      const pdt = new ProviderDefinedType('Broken', { x: 1 });
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
      const registry = new ProviderDefinedTypeRegistry();
      registry.register('Address', {
        serialize: (obj) => obj,
        deserialize: (props) => ({ type: 'Address', city: props.city, zip: props.zip }),
      });
      registry.register('Person', {
        serialize: (obj) => obj,
        deserialize: (props) => ({ type: 'Person', name: props.name, address: props.address }),
      });

      const addressPdt = new ProviderDefinedType('Address', { city: 'Portland', zip: '97201' });
      const personPdt = new ProviderDefinedType('Person', { name: 'Alice', address: addressPdt });

      const result = registry.hydrate(personPdt);

      assert.deepStrictEqual(result, {
        type: 'Person',
        name: 'Alice',
        address: { type: 'Address', city: 'Portland', zip: '97201' },
      });
    });

    it('should return non-PDT values unchanged', () => {
      const registry = new ProviderDefinedTypeRegistry();
      assert.strictEqual(registry.hydrate('hello'), 'hello');
      assert.strictEqual(registry.hydrate(42), 42);
      assert.strictEqual(registry.hydrate(null), null);
    });
  });

  describe('#hasAdapter()', () => {
    it('should return true for registered types', () => {
      const registry = new ProviderDefinedTypeRegistry();
      registry.register('Foo', { serialize: () => ({}), deserialize: (p) => p });
      assert.isTrue(registry.hasAdapter('Foo'));
      assert.isFalse(registry.hasAdapter('Bar'));
    });
  });

  describe('#getSerializer()', () => {
    it('should return the serialize function for registered types', () => {
      const registry = new ProviderDefinedTypeRegistry();
      const serFn = (obj) => ({ val: obj.val });
      registry.register('Custom', { serialize: serFn, deserialize: (p) => p });
      assert.strictEqual(registry.getSerializer('Custom'), serFn);
    });

    it('should return null for unregistered types', () => {
      const registry = new ProviderDefinedTypeRegistry();
      assert.isNull(registry.getSerializer('Missing'));
    });
  });
});

describe('pdtRegistry wiring through Client/Connection', () => {
  it('should set pdtRegistry on the reader when passed via Connection options', () => {
    const registry = new ProviderDefinedTypeRegistry();
    registry.register('GeoPoint', {
      serialize: (obj) => obj,
      deserialize: (props) => ({ type: 'GeoPoint', ...props }),
    });

    const conn = new Connection('http://localhost:8182', { pdtRegistry: registry });
    assert.strictEqual(conn._reader.pdtRegistry, registry);
  });

  it('should set pdtRegistry on the reader when passed via Client options', () => {
    const registry = new ProviderDefinedTypeRegistry();
    registry.register('GeoPoint', {
      serialize: (obj) => obj,
      deserialize: (props) => ({ type: 'GeoPoint', ...props }),
    });

    const client = new Client('http://localhost:8182', { pdtRegistry: registry });
    assert.strictEqual(client._connection._reader.pdtRegistry, registry);
  });

  it('should not leak pdtRegistry between connections', () => {
    const registry = new ProviderDefinedTypeRegistry();
    const conn1 = new Connection('http://localhost:8182', { pdtRegistry: registry });
    const conn2 = new Connection('http://localhost:8182');
    assert.isNull(conn2._reader.pdtRegistry);
    assert.strictEqual(conn1._reader.pdtRegistry, registry);
  });
});
