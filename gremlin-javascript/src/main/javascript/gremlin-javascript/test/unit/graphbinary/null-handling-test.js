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

/*
 * Null and undefined handling tests for GraphBinaryV4. Validates
 * fully-qualified and bare null serialization/deserialization for
 * every supported type.
 */

import { assert } from 'chai';
import ioc from '../../../lib/structure/io/binary/GraphBinary.js';
const { anySerializer, DataType } = ioc;

describe('GraphBinary v4 Null Handling Tests', () => {
  describe('anySerializer level', () => {
    it('serialize null', () => {
      const result = anySerializer.serialize(null);
      assert.deepStrictEqual(result, Buffer.from([0xFE, 0x01]));
    });

    it('serialize undefined', () => {
      const result = anySerializer.serialize(undefined);
      assert.deepStrictEqual(result, Buffer.from([0xFE, 0x01]));
    });

    it('deserialize unspecified null', () => {
      const result = anySerializer.deserialize(Buffer.from([0xFE, 0x01]));
      assert.strictEqual(result.v, null);
      assert.strictEqual(result.len, 2);
    });
  });

  describe('per-type fully-qualified null', () => {
    const types = [
      { name: 'INT', code: DataType.INT, serializer: 'intSerializer' },
      { name: 'LONG', code: DataType.LONG, serializer: 'longSerializer' },
      { name: 'STRING', code: DataType.STRING, serializer: 'stringSerializer' },
      { name: 'DOUBLE', code: DataType.DOUBLE, serializer: 'doubleSerializer' },
      { name: 'FLOAT', code: DataType.FLOAT, serializer: 'floatSerializer' },
      { name: 'BOOLEAN', code: DataType.BOOLEAN, serializer: 'booleanSerializer' },
      { name: 'SHORT', code: DataType.SHORT, serializer: 'shortSerializer' },
      { name: 'BYTE', code: DataType.BYTE, serializer: 'byteSerializer' },
      { name: 'UUID', code: DataType.UUID, serializer: 'uuidSerializer' },
      { name: 'BIGINTEGER', code: DataType.BIGINTEGER, serializer: 'bigIntegerSerializer' },
      { name: 'DATETIME', code: DataType.DATETIME, serializer: 'dateTimeSerializer' },
      { name: 'BINARY', code: DataType.BINARY, serializer: 'binarySerializer' },
      { name: 'LIST', code: DataType.LIST, serializer: 'listSerializer' },
      { name: 'SET', code: DataType.SET, serializer: 'setSerializer' },
      { name: 'MAP', code: DataType.MAP, serializer: 'mapSerializer' },
      { name: 'VERTEX', code: DataType.VERTEX, serializer: 'vertexSerializer' },
      { name: 'EDGE', code: DataType.EDGE, serializer: 'edgeSerializer' },
      { name: 'PROPERTY', code: DataType.PROPERTY, serializer: 'propertySerializer' },
      { name: 'VERTEXPROPERTY', code: DataType.VERTEXPROPERTY, serializer: 'vertexPropertySerializer' },
      { name: 'PATH', code: DataType.PATH, serializer: 'pathSerializer' }
    ];

    types.forEach(({ name, code, serializer }) => {
      describe(name, () => {
        it('serialize null fully-qualified', () => {
          const result = ioc[serializer].serialize(null, true);
          assert.deepStrictEqual(result, Buffer.from([code, 0x01]));
        });

        it('serialize undefined fully-qualified', () => {
          const result = ioc[serializer].serialize(undefined, true);
          assert.deepStrictEqual(result, Buffer.from([code, 0x01]));
        });

        it('deserialize fully-qualified null', () => {
          const result = ioc[serializer].deserialize(Buffer.from([code, 0x01]), true);
          assert.strictEqual(result.v, null);
          assert.strictEqual(result.len, 2);
        });
      });
    });

    // EnumSerializer has special null handling - it doesn't support null/undefined directly
    describe('DIRECTION', () => {
      it('deserialize fully-qualified null', () => {
        const result = ioc.enumSerializer.deserialize(Buffer.from([DataType.DIRECTION, 0x01]), true);
        assert.strictEqual(result.v, null);
        assert.strictEqual(result.len, 2);
      });
    });

    describe('T', () => {
      it('deserialize fully-qualified null', () => {
        const result = ioc.enumSerializer.deserialize(Buffer.from([DataType.T, 0x01]), true);
        assert.strictEqual(result.v, null);
        assert.strictEqual(result.len, 2);
      });
    });
  });

  describe('per-type bare null', () => {
    const bareNullTests = [
      { name: 'INT', serializer: 'intSerializer', expected: Buffer.from([0x00, 0x00, 0x00, 0x00]), defaultValue: 0 },
      { name: 'LONG', serializer: 'longSerializer', expected: Buffer.from([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]), defaultValue: 0 },
      { name: 'STRING', serializer: 'stringSerializer', expected: Buffer.from([0x00, 0x00, 0x00, 0x00]), defaultValue: '' },
      { name: 'DOUBLE', serializer: 'doubleSerializer', expected: Buffer.from([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]), defaultValue: 0.0 },
      { name: 'FLOAT', serializer: 'floatSerializer', expected: Buffer.from([0x00, 0x00, 0x00, 0x00]), defaultValue: 0.0 },
      { name: 'BOOLEAN', serializer: 'booleanSerializer', expected: Buffer.from([0x00]), defaultValue: false },
      { name: 'SHORT', serializer: 'shortSerializer', expected: Buffer.from([0x00, 0x00]), defaultValue: 0 },
      { name: 'BYTE', serializer: 'byteSerializer', expected: Buffer.from([0x00]), defaultValue: 0 },
      { name: 'UUID', serializer: 'uuidSerializer', expected: Buffer.from([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]), defaultValue: '00000000-0000-0000-0000-000000000000' },
      { name: 'BIGINTEGER', serializer: 'bigIntegerSerializer', expected: Buffer.from([0x00, 0x00, 0x00, 0x01, 0x00]), defaultValue: BigInt(0) },
      { name: 'DATETIME', serializer: 'dateTimeSerializer', expected: Buffer.alloc(18), defaultValue: null },
      { name: 'BINARY', serializer: 'binarySerializer', expected: Buffer.from([0x00, 0x00, 0x00, 0x00]), defaultValue: Buffer.alloc(0) }
    ];

    bareNullTests.forEach(({ name, serializer, expected, defaultValue }) => {
      describe(name, () => {
        it('serialize null bare', () => {
          const result = ioc[serializer].serialize(null, false);
          assert.deepStrictEqual(result, expected);
        });

        it('serialize undefined bare', () => {
          const result = ioc[serializer].serialize(undefined, false);
          assert.deepStrictEqual(result, expected);
        });

        it('deserialize bare null', () => {
          const result = ioc[serializer].deserialize(expected, false);
          if (defaultValue === null) {
            // DATETIME: 18 zero bytes produce a valid Date object but with implementation-defined value
            assert.instanceOf(result.v, Date);
          } else if (typeof defaultValue === 'bigint') {
            assert.strictEqual(result.v, defaultValue);
          } else if (Buffer.isBuffer(defaultValue)) {
            assert.isTrue(Buffer.isBuffer(result.v));
            assert.isTrue(result.v.equals(defaultValue));
          } else if (defaultValue instanceof Date) {
            assert.instanceOf(result.v, Date);
          } else {
            assert.strictEqual(result.v, defaultValue);
          }
          assert.strictEqual(result.len, expected.length);
        });
      });
    });

    // Collection types have different bare null behavior
    const collectionTests = [
      { name: 'LIST', serializer: 'listSerializer', expected: Buffer.from([0x00, 0x00, 0x00, 0x00]), defaultValue: [] },
      { name: 'SET', serializer: 'setSerializer', expected: Buffer.from([0x00, 0x00, 0x00, 0x00]), defaultValue: new Set() },
      { name: 'MAP', serializer: 'mapSerializer', expected: Buffer.from([0x00, 0x00, 0x00, 0x00]), defaultValue: new Map() }
    ];

    collectionTests.forEach(({ name, serializer, expected, defaultValue }) => {
      describe(name, () => {
        it('serialize null bare', () => {
          const result = ioc[serializer].serialize(null, false);
          assert.deepStrictEqual(result, expected);
        });

        it('serialize undefined bare', () => {
          const result = ioc[serializer].serialize(undefined, false);
          assert.deepStrictEqual(result, expected);
        });

        it('deserialize bare null', () => {
          const result = ioc[serializer].deserialize(expected, false);
          if (defaultValue instanceof Set) {
            assert.instanceOf(result.v, Set);
            assert.strictEqual(result.v.size, 0);
          } else if (defaultValue instanceof Map) {
            assert.instanceOf(result.v, Map);
            assert.strictEqual(result.v.size, 0);
          } else if (Array.isArray(defaultValue)) {
            assert.isArray(result.v);
            assert.strictEqual(result.v.length, 0);
          }
          assert.strictEqual(result.len, expected.length);
        });
      });
    });

    // Graph types serialize to empty structures
    const graphTests = [
      { name: 'VERTEX', serializer: 'vertexSerializer' },
      { name: 'EDGE', serializer: 'edgeSerializer' },
      { name: 'PROPERTY', serializer: 'propertySerializer' },
      { name: 'VERTEXPROPERTY', serializer: 'vertexPropertySerializer' },
      { name: 'PATH', serializer: 'pathSerializer' }
    ];

    graphTests.forEach(({ name, serializer }) => {
      describe(name, () => {
        it('serialize null bare produces non-empty bytes', () => {
          const result = ioc[serializer].serialize(null, false);
          assert.isTrue(result.length > 0);
        });

        it('serialize undefined bare produces non-empty bytes', () => {
          const result = ioc[serializer].serialize(undefined, false);
          assert.isTrue(result.length > 0);
        });

        it('deserialize bare null produces default object', () => {
          const nullBytes = ioc[serializer].serialize(null, false);
          const result = ioc[serializer].deserialize(nullBytes, false);
          assert.isNotNull(result.v);
          assert.strictEqual(result.len, nullBytes.length);
        });
      });
    });
  });
});