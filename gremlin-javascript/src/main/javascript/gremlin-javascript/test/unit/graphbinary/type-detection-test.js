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

/*
 * Type detection and dispatch tests for GraphBinaryV4. Verifies that
 * anySerializer routes each JS type to the correct serializer type code,
 * canBeUsedFor rejects wrong types, and round-trip extras not covered
 * by .gbin reference files.
 */

import { assert } from 'chai';
import { Vertex, Edge, Property, VertexProperty, Path } from '../../../lib/structure/graph.js';
import { direction, t, merge } from '../../../lib/process/traversal.js';
import ioc, { DataType } from '../../../lib/structure/io/binary/GraphBinary.js';

const { anySerializer } = ioc;

describe('Type Detection Tests', () => {
  describe('Number routing (NumberSerializationStrategy)', () => {
    it('Integer in Int32 range → INT (0x01)', () => {
      const result = anySerializer.serialize(42);
      assert.strictEqual(result[0], DataType.INT);
    });

    it('Integer at Int32 boundaries → INT (0x01)', () => {
      assert.strictEqual(anySerializer.serialize(2147483647)[0], DataType.INT);
      assert.strictEqual(anySerializer.serialize(-2147483648)[0], DataType.INT);
    });

    it('Integer beyond Int32 but in Int64 → LONG (0x02)', () => {
      const result = anySerializer.serialize(2147483648);
      assert.strictEqual(result[0], DataType.LONG);
    });

    it('Number.MAX_SAFE_INTEGER → LONG (0x02)', () => {
      const result = anySerializer.serialize(Number.MAX_SAFE_INTEGER);
      assert.strictEqual(result[0], DataType.LONG);
    });

    it('Number.MIN_SAFE_INTEGER → LONG (0x02)', () => {
      const result = anySerializer.serialize(Number.MIN_SAFE_INTEGER);
      assert.strictEqual(result[0], DataType.LONG);
    });

    it('Non-integer → DOUBLE (0x07)', () => {
      assert.strictEqual(anySerializer.serialize(3.14)[0], DataType.DOUBLE);
      assert.strictEqual(anySerializer.serialize(0.1)[0], DataType.DOUBLE);
    });

    it('NaN → DOUBLE (0x07)', () => {
      const result = anySerializer.serialize(NaN);
      assert.strictEqual(result[0], DataType.DOUBLE);
    });

    it('±Infinity → DOUBLE (0x07)', () => {
      assert.strictEqual(anySerializer.serialize(Infinity)[0], DataType.DOUBLE);
      assert.strictEqual(anySerializer.serialize(-Infinity)[0], DataType.DOUBLE);
    });

    it('-0 → DOUBLE (0x07) preserving sign', () => {
      const result = anySerializer.serialize(-0);
      assert.strictEqual(result[0], DataType.DOUBLE);
      const deserialized = anySerializer.deserialize(result);
      assert.isTrue(Object.is(deserialized.v, -0));
    });

    it('BigInt within Int64 range → BIGINTEGER (0x23)', () => {
      assert.strictEqual(anySerializer.serialize(123n)[0], DataType.BIGINTEGER);
      assert.strictEqual(anySerializer.serialize(-456n)[0], DataType.BIGINTEGER);
    });

    it('BigInt at Int64 boundaries → BIGINTEGER (0x23)', () => {
      assert.strictEqual(anySerializer.serialize(2n**63n - 1n)[0], DataType.BIGINTEGER);
      assert.strictEqual(anySerializer.serialize(-(2n**63n))[0], DataType.BIGINTEGER);
    });

    it('BigInt beyond Int64 → BIGINTEGER (0x23)', () => {
      assert.strictEqual(anySerializer.serialize(2n**63n)[0], DataType.BIGINTEGER);
      assert.strictEqual(anySerializer.serialize(-(2n**63n) - 1n)[0], DataType.BIGINTEGER);
    });
  });

  describe('Type dispatch', () => {
    it('boolean → BOOLEAN (0x27)', () => {
      assert.strictEqual(anySerializer.serialize(true)[0], DataType.BOOLEAN);
      assert.strictEqual(anySerializer.serialize(false)[0], DataType.BOOLEAN);
    });

    it('string → STRING (0x03)', () => {
      const result = anySerializer.serialize('hello');
      assert.strictEqual(result[0], DataType.STRING);
    });

    it('Date → DATETIME (0x04)', () => {
      const result = anySerializer.serialize(new Date());
      assert.strictEqual(result[0], DataType.DATETIME);
    });

    it('Array → LIST (0x09)', () => {
      const result = anySerializer.serialize([1, 2, 3]);
      assert.strictEqual(result[0], DataType.LIST);
    });

    it('Set → SET (0x0b)', () => {
      const result = anySerializer.serialize(new Set([1, 2, 3]));
      assert.strictEqual(result[0], DataType.SET);
    });

    it('Map → MAP (0x0a)', () => {
      const result = anySerializer.serialize(new Map([['a', 1]]));
      assert.strictEqual(result[0], DataType.MAP);
    });

    it('Plain object → MAP (0x0a)', () => {
      const result = anySerializer.serialize({});
      assert.strictEqual(result[0], DataType.MAP);
    });

    it('Buffer → BINARY (0x25)', () => {
      const result = anySerializer.serialize(Buffer.from('test'));
      assert.strictEqual(result[0], DataType.BINARY);
    });

    it('null → UNSPECIFIED_NULL (0xfe)', () => {
      const result = anySerializer.serialize(null);
      assert.strictEqual(result[0], DataType.UNSPECIFIED_NULL);
    });

    it('undefined → UNSPECIFIED_NULL (0xfe)', () => {
      const result = anySerializer.serialize(undefined);
      assert.strictEqual(result[0], DataType.UNSPECIFIED_NULL);
    });

    it('Vertex → VERTEX (0x11)', () => {
      const vertex = new Vertex(1, 'person');
      const result = anySerializer.serialize(vertex);
      assert.strictEqual(result[0], DataType.VERTEX);
    });

    it('Edge → EDGE (0x0d)', () => {
      const edge = new Edge(1, new Vertex(1, 'person'), 'knows', new Vertex(2, 'person'));
      const result = anySerializer.serialize(edge);
      assert.strictEqual(result[0], DataType.EDGE);
    });

    it('Property → PROPERTY (0x0f)', () => {
      const property = new Property('name', 'marko');
      const result = anySerializer.serialize(property);
      assert.strictEqual(result[0], DataType.PROPERTY);
    });

    it('VertexProperty → VERTEXPROPERTY (0x12)', () => {
      const vp = new VertexProperty(1, 'name', 'marko');
      const result = anySerializer.serialize(vp);
      assert.strictEqual(result[0], DataType.VERTEXPROPERTY);
    });

    it('Path → PATH (0x0e)', () => {
      const path = new Path([new Set()], [1]);
      const result = anySerializer.serialize(path);
      assert.strictEqual(result[0], DataType.PATH);
    });

    it('direction.out → DIRECTION (0x18)', () => {
      const result = anySerializer.serialize(direction.out);
      assert.strictEqual(result[0], DataType.DIRECTION);
    });

    it('t.id → T (0x20)', () => {
      const result = anySerializer.serialize(t.id);
      assert.strictEqual(result[0], DataType.T);
    });

    it('merge.onCreate → MERGE (0x2e)', () => {
      const result = anySerializer.serialize(merge.onCreate);
      assert.strictEqual(result[0], DataType.MERGE);
    });
  });

  describe('canBeUsedFor negative cases', () => {
    it('intSerializer rejects non-numbers', () => {
      assert.isFalse(ioc.intSerializer.canBeUsedFor('string'));
      assert.isFalse(ioc.intSerializer.canBeUsedFor(true));
      assert.isFalse(ioc.intSerializer.canBeUsedFor([]));
    });

    it('stringSerializer rejects non-strings', () => {
      assert.isFalse(ioc.stringSerializer.canBeUsedFor(42));
      assert.isFalse(ioc.stringSerializer.canBeUsedFor(true));
      assert.isFalse(ioc.stringSerializer.canBeUsedFor([]));
    });

    it('booleanSerializer rejects non-booleans', () => {
      assert.isFalse(ioc.booleanSerializer.canBeUsedFor(42));
      assert.isFalse(ioc.booleanSerializer.canBeUsedFor('string'));
      assert.isFalse(ioc.booleanSerializer.canBeUsedFor([]));
    });

    it('vertexSerializer rejects non-vertices', () => {
      assert.isFalse(ioc.vertexSerializer.canBeUsedFor(new Edge(1, new Vertex(1, 'p'), 'knows', new Vertex(2, 'p'))));
      assert.isFalse(ioc.vertexSerializer.canBeUsedFor('string'));
      assert.isFalse(ioc.vertexSerializer.canBeUsedFor(42));
    });

    it('edgeSerializer rejects non-edges', () => {
      assert.isFalse(ioc.edgeSerializer.canBeUsedFor(new Vertex(1, 'person')));
      assert.isFalse(ioc.edgeSerializer.canBeUsedFor('string'));
      assert.isFalse(ioc.edgeSerializer.canBeUsedFor(42));
    });

    it('setSerializer rejects non-sets', () => {
      assert.isFalse(ioc.setSerializer.canBeUsedFor([]));
      assert.isFalse(ioc.setSerializer.canBeUsedFor({}));
      assert.isFalse(ioc.setSerializer.canBeUsedFor('string'));
    });

    it('listSerializer rejects non-arrays', () => {
      assert.isFalse(ioc.listSerializer.canBeUsedFor(new Set()));
      assert.isFalse(ioc.listSerializer.canBeUsedFor({}));
      assert.isFalse(ioc.listSerializer.canBeUsedFor('string'));
    });

    it('dateTimeSerializer rejects non-dates', () => {
      assert.isFalse(ioc.dateTimeSerializer.canBeUsedFor(42));
      assert.isFalse(ioc.dateTimeSerializer.canBeUsedFor('string'));
      assert.isFalse(ioc.dateTimeSerializer.canBeUsedFor([]));
    });

    it('mapSerializer rejects non-maps/non-objects', () => {
      assert.isFalse(ioc.mapSerializer.canBeUsedFor(42));
      assert.isFalse(ioc.mapSerializer.canBeUsedFor('string'));
      assert.isFalse(ioc.mapSerializer.canBeUsedFor([]));
    });

    it('binarySerializer rejects non-buffers', () => {
      assert.isFalse(ioc.binarySerializer.canBeUsedFor('string'));
      assert.isFalse(ioc.binarySerializer.canBeUsedFor(42));
      assert.isFalse(ioc.binarySerializer.canBeUsedFor([]));
    });

    it('pathSerializer rejects non-paths', () => {
      assert.isFalse(ioc.pathSerializer.canBeUsedFor(new Vertex(1, 'person')));
      assert.isFalse(ioc.pathSerializer.canBeUsedFor('string'));
      assert.isFalse(ioc.pathSerializer.canBeUsedFor(42));
    });

    it('propertySerializer rejects non-properties', () => {
      assert.isFalse(ioc.propertySerializer.canBeUsedFor(new Vertex(1, 'person')));
      assert.isFalse(ioc.propertySerializer.canBeUsedFor('string'));
      assert.isFalse(ioc.propertySerializer.canBeUsedFor(42));
    });

    it('vertexPropertySerializer rejects non-vertex-properties', () => {
      assert.isFalse(ioc.vertexPropertySerializer.canBeUsedFor(new Vertex(1, 'person')));
      assert.isFalse(ioc.vertexPropertySerializer.canBeUsedFor('string'));
      assert.isFalse(ioc.vertexPropertySerializer.canBeUsedFor(42));
    });

    it('enumSerializer rejects non-enums', () => {
      assert.isFalse(ioc.enumSerializer.canBeUsedFor('string'));
      assert.isFalse(ioc.enumSerializer.canBeUsedFor(42));
      assert.isFalse(ioc.enumSerializer.canBeUsedFor(new Vertex(1, 'person')));
    });
  });

  describe('Round-trip extras', () => {
    it('nested collections', () => {
      const value = [1, [2, 3], new Map([['a', [4]]])];
      const serialized = anySerializer.serialize(value);
      const deserialized = anySerializer.deserialize(serialized);
      assert.deepStrictEqual(deserialized.v, value);
    });

    it('empty containers', () => {
      const values = [[], new Set(), new Map(), ''];
      values.forEach(value => {
        const serialized = anySerializer.serialize(value);
        const deserialized = anySerializer.deserialize(serialized);
        assert.deepStrictEqual(deserialized.v, value);
      });
    });

    it('JS-specific label construction survives round-trip', () => {
      const vertex = new Vertex(123, 'person');
      const serialized = anySerializer.serialize(vertex);
      const deserialized = anySerializer.deserialize(serialized);
      assert.strictEqual(deserialized.v.label, 'person');
    });

    it('Unicode strings', () => {
      const value = '🚀 Hello 世界';
      const serialized = anySerializer.serialize(value);
      const deserialized = anySerializer.deserialize(serialized);
      assert.strictEqual(deserialized.v, value);
    });

    it('boundary integers', () => {
      const values = [0, -1, 1];
      values.forEach(value => {
        const serialized = anySerializer.serialize(value);
        const deserialized = anySerializer.deserialize(serialized);
        assert.strictEqual(deserialized.v, value);
      });
    });

    it('NaN round-trip', () => {
      const serialized = anySerializer.serialize(NaN);
      const deserialized = anySerializer.deserialize(serialized);
      assert.isTrue(Number.isNaN(deserialized.v));
    });

    it('complex nested structure', () => {
      const value = new Map([
        ['numbers', [1, 2.5, BigInt(123)]],
        ['sets', new Set(['a', 'b'])],
        ['map', new Map([[1, 'one'], [2, 'two']])],
        ['nested', new Map([['inner', [true, false, null]]])]
      ]);
      const serialized = anySerializer.serialize(value);
      const deserialized = anySerializer.deserialize(serialized);
      assert.deepStrictEqual(deserialized.v, value);
    });

    it('mixed type array', () => {
      const value = [1, 'string', true, null, new Date(0), Buffer.from('test')];
      const serialized = anySerializer.serialize(value);
      const deserialized = anySerializer.deserialize(serialized);
      assert.strictEqual(deserialized.v.length, value.length);
      assert.strictEqual(deserialized.v[0], 1);
      assert.strictEqual(deserialized.v[1], 'string');
      assert.strictEqual(deserialized.v[2], true);
      assert.strictEqual(deserialized.v[3], null);
      assert.deepStrictEqual(deserialized.v[4], new Date(0));
      assert.isTrue(Buffer.isBuffer(deserialized.v[5]));
      assert.isTrue(deserialized.v[5].equals(Buffer.from('test')));
    });

    it('graph elements with properties', () => {
      const vertex = new Vertex(1, ['person'], [new VertexProperty(1, ['name'], 'marko')]);
      const serialized = anySerializer.serialize(vertex);
      const deserialized = anySerializer.deserialize(serialized);
      assert.strictEqual(deserialized.v.id, 1);
      assert.strictEqual(deserialized.v.label, 'person');
    });

    it('path with multiple labels', () => {
      const path = new Path([new Set(['a']), new Set(['b', 'c'])], [1, 2]);
      const serialized = anySerializer.serialize(path);
      const deserialized = anySerializer.deserialize(serialized);
      assert.deepStrictEqual(deserialized.v.labels, path.labels);
      assert.deepStrictEqual(deserialized.v.objects, path.objects);
    });

    it('large numbers', () => {
      const values = [Number.MAX_SAFE_INTEGER, Number.MIN_SAFE_INTEGER, 2n**100n];
      values.forEach(value => {
        const serialized = anySerializer.serialize(value);
        const deserialized = anySerializer.deserialize(serialized);
        assert.strictEqual(deserialized.v, value);
      });
    });

    it('special float values', () => {
      const values = [Infinity, -Infinity];
      values.forEach(value => {
        const serialized = anySerializer.serialize(value);
        const deserialized = anySerializer.deserialize(serialized);
        assert.strictEqual(deserialized.v, value);
      });
    });

    it('enum values', () => {
      const values = [direction.out, direction.in, direction.both, t.id, t.key, t.label, t.value, merge.onCreate, merge.onMatch, merge.outV, merge.inV];
      values.forEach(value => {
        const serialized = anySerializer.serialize(value);
        const deserialized = anySerializer.deserialize(serialized);
        assert.strictEqual(deserialized.v, value);
      });
    });
  });
});