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
 * Null and undefined handling tests for GraphBinaryV4. Validates
 * fully-qualified and bare null serialization/deserialization for
 * every supported type.
 */

import { assert } from 'chai';
import ioc, {enumSerializer} from '../../../lib/structure/io/binary/GraphBinary.js';
import StreamReader from '../../../lib/structure/io/binary/internals/StreamReader.js';
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

    it('deserialize unspecified null', async () => {
      const result = await anySerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0xFE, 0x01])));
      assert.strictEqual(result, null);
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

        it('deserialize fully-qualified null', async () => {
          const result = await ioc[serializer].deserialize(StreamReader.fromBuffer(Buffer.from([code, 0x01])));
          assert.strictEqual(result, null);
        });
      });
    });

    // EnumSerializer has special null handling - it doesn't support null/undefined directly
    describe('DIRECTION', () => {
      it('deserialize fully-qualified null', async () => {
        const result = await ioc.enumSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([DataType.DIRECTION, 0x01])));
        assert.strictEqual(result, null);
      });
    });

    describe('T', () => {
      it('deserialize fully-qualified null', async () => {
        const result = await ioc.enumSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([DataType.T, 0x01])));
        assert.strictEqual(result, null);
      });
    });

    describe('MERGE', () => {
      it('deserialize fully-qualified null', async () => {
        const result = await ioc.enumSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([DataType.MERGE, 0x01])));
        assert.strictEqual(result, null);
      });
    });
  });

  describe('per-type bare null', () => {
    const bareNullTests = [
      { name: 'INT', code: DataType.INT, serializer: 'intSerializer', expected: Buffer.from([0x00, 0x00, 0x00, 0x00]), defaultValue: 0 },
      { name: 'LONG', code: DataType.LONG, serializer: 'longSerializer', expected: Buffer.from([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]), defaultValue: 0 },
      { name: 'STRING', code: DataType.STRING, serializer: 'stringSerializer', expected: Buffer.from([0x00, 0x00, 0x00, 0x00]), defaultValue: '' },
      { name: 'DOUBLE', code: DataType.DOUBLE, serializer: 'doubleSerializer', expected: Buffer.from([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]), defaultValue: 0.0 },
      { name: 'FLOAT', code: DataType.FLOAT, serializer: 'floatSerializer', expected: Buffer.from([0x00, 0x00, 0x00, 0x00]), defaultValue: 0.0 },
      { name: 'BOOLEAN', code: DataType.BOOLEAN, serializer: 'booleanSerializer', expected: Buffer.from([0x00]), defaultValue: false },
      { name: 'SHORT', code: DataType.SHORT, serializer: 'shortSerializer', expected: Buffer.from([0x00, 0x00]), defaultValue: 0 },
      { name: 'BYTE', code: DataType.BYTE, serializer: 'byteSerializer', expected: Buffer.from([0x00]), defaultValue: 0 },
      { name: 'UUID', code: DataType.UUID, serializer: 'uuidSerializer', expected: Buffer.from([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]), defaultValue: '00000000-0000-0000-0000-000000000000' },
      { name: 'BIGINTEGER', code: DataType.BIGINTEGER, serializer: 'bigIntegerSerializer', expected: Buffer.from([0x00, 0x00, 0x00, 0x01, 0x00]), defaultValue: BigInt(0) },
      { name: 'DATETIME', code: DataType.DATETIME, serializer: 'dateTimeSerializer', expected: Buffer.alloc(18), defaultValue: null },
      { name: 'BINARY', code: DataType.BINARY, serializer: 'binarySerializer', expected: Buffer.from([0x00, 0x00, 0x00, 0x00]), defaultValue: Buffer.alloc(0) }
    ];

    bareNullTests.forEach(({ name, code, serializer, expected, defaultValue }) => {
      describe(name, () => {
        it('serialize null bare', () => {
          const result = ioc[serializer].serialize(null, false);
          assert.deepStrictEqual(result, expected);
        });

        it('serialize undefined bare', () => {
          const result = ioc[serializer].serialize(undefined, false);
          assert.deepStrictEqual(result, expected);
        });

        it('deserialize bare null', async () => {
          const reader = StreamReader.fromBuffer(expected);
          const result = await ioc[serializer].deserializeValue(reader, 0x00, code);
          if (defaultValue === null) {
            // DATETIME: 18 zero bytes produce a valid Date object but with implementation-defined value
            assert.instanceOf(result, Date);
          } else if (typeof defaultValue === 'bigint') {
            assert.strictEqual(result, defaultValue);
          } else if (Buffer.isBuffer(defaultValue)) {
            assert.isTrue(Buffer.isBuffer(result));
            assert.isTrue(result.equals(defaultValue));
          } else if (defaultValue instanceof Date) {
            assert.instanceOf(result, Date);
          } else {
            assert.strictEqual(result, defaultValue);
          }
          assert.strictEqual(reader.position, expected.length);
        });
      });
    });

    // Collection types have different bare null behavior
    const collectionTests = [
      { name: 'LIST', code: DataType.LIST, serializer: 'listSerializer', expected: Buffer.from([0x00, 0x00, 0x00, 0x00]), defaultValue: [] },
      { name: 'SET', code: DataType.SET, serializer: 'setSerializer', expected: Buffer.from([0x00, 0x00, 0x00, 0x00]), defaultValue: new Set() },
      { name: 'MAP', code: DataType.MAP, serializer: 'mapSerializer', expected: Buffer.from([0x00, 0x00, 0x00, 0x00]), defaultValue: new Map() }
    ];

    collectionTests.forEach(({ name, code, serializer, expected, defaultValue }) => {
      describe(name, () => {
        it('serialize null bare', () => {
          const result = ioc[serializer].serialize(null, false);
          assert.deepStrictEqual(result, expected);
        });

        it('serialize undefined bare', () => {
          const result = ioc[serializer].serialize(undefined, false);
          assert.deepStrictEqual(result, expected);
        });

        it('deserialize bare null', async () => {
          const reader = StreamReader.fromBuffer(expected);
          const result = await ioc[serializer].deserializeValue(reader, 0x00, code);
          if (defaultValue instanceof Set) {
            assert.instanceOf(result, Set);
            assert.strictEqual(result.size, 0);
          } else if (defaultValue instanceof Map) {
            assert.instanceOf(result, Map);
            assert.strictEqual(result.size, 0);
          } else if (Array.isArray(defaultValue)) {
            assert.isArray(result);
            assert.strictEqual(result.length, 0);
          }
          assert.strictEqual(reader.position, expected.length);
        });
      });
    });

    // Graph types serialize to empty structures
    const graphTests = [
      { name: 'VERTEX', code: DataType.VERTEX, serializer: 'vertexSerializer' },
      { name: 'EDGE', code: DataType.EDGE, serializer: 'edgeSerializer' },
      { name: 'PROPERTY', code: DataType.PROPERTY, serializer: 'propertySerializer' },
      { name: 'VERTEXPROPERTY', code: DataType.VERTEXPROPERTY, serializer: 'vertexPropertySerializer' },
      { name: 'PATH', code: DataType.PATH, serializer: 'pathSerializer' }
    ];

    graphTests.forEach(({ name, code, serializer }) => {
      describe(name, () => {
        it('serialize null bare produces non-empty bytes', () => {
          const result = ioc[serializer].serialize(null, false);
          assert.isTrue(result.length > 0);
        });

        it('serialize undefined bare produces non-empty bytes', () => {
          const result = ioc[serializer].serialize(undefined, false);
          assert.isTrue(result.length > 0);
        });

        it('deserialize bare null produces default object', async () => {
          const nullBytes = ioc[serializer].serialize(null, false);
          const reader = StreamReader.fromBuffer(nullBytes);
          const result = await ioc[serializer].deserializeValue(reader, 0x00, code);
          assert.isNotNull(result);
          assert.strictEqual(reader.position, nullBytes.length);
        });
      });
    });
  });
});