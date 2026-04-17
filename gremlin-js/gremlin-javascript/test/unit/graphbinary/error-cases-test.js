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
 * Error handling tests for GraphBinaryV4 serializers. Validates behavior for
 * malformed input: invalid buffers, unknown type codes, bad value flags,
 * truncated data, negative lengths, and unsupported types.
 */

import { assert } from 'chai';
import { Buffer } from 'buffer';
import ioc from '../../../lib/structure/io/binary/GraphBinary.js';
import StreamReader from '../../../lib/structure/io/binary/internals/StreamReader.js';
import { P, TextP, Traverser } from '../../../lib/process/traversal.js';
import { OptionsStrategy } from '../../../lib/process/traversal-strategy.js';

const { anySerializer, intSerializer, longSerializer, stringSerializer, listSerializer, mapSerializer, uuidSerializer, dateTimeSerializer, floatSerializer, shortSerializer, byteSerializer, bigIntegerSerializer, binarySerializer, setSerializer, enumSerializer, edgeSerializer, vertexSerializer, vertexPropertySerializer, propertySerializer, pathSerializer, markerSerializer } = ioc;

/** Helper: assert that an async call rejects with a message matching the pattern */
async function assertRejects(fn, pattern) {
  try {
    await fn();
    assert.fail('Expected an error to be thrown');
  } catch (e) {
    if (pattern) assert.match(e.message, pattern);
  }
}

describe('GraphBinary v4 Error Cases', () => {
  describe('Buffer validation', () => {
    it('undefined buffer throws error', async () => {
      await assertRejects(() => anySerializer.deserialize(StreamReader.fromBuffer(undefined)));
    });

    it('null buffer throws error', async () => {
      await assertRejects(() => anySerializer.deserialize(StreamReader.fromBuffer(null)));
    });

    it('non-Buffer object throws error', async () => {
      await assertRejects(() => anySerializer.deserialize(StreamReader.fromBuffer('not a buffer')));
    });

    it('empty buffer throws error', async () => {
      await assertRejects(() => anySerializer.deserialize(StreamReader.fromBuffer(Buffer.alloc(0))), /Unexpected end of buffer/);
    });

    it('buffer with only type_code, no value_flag throws error', async () => {
      await assertRejects(() => anySerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x01]))), /Unexpected end of buffer/);
    });

    it('individual serializer with undefined buffer throws error', async () => {
      await assertRejects(() => intSerializer.deserialize(StreamReader.fromBuffer(undefined)));
    });

    it('individual serializer with null buffer throws error', async () => {
      await assertRejects(() => intSerializer.deserialize(StreamReader.fromBuffer(null)));
    });

    it('individual serializer with empty buffer throws error', async () => {
      await assertRejects(() => intSerializer.deserialize(StreamReader.fromBuffer(Buffer.alloc(0))), /Unexpected end of buffer/);
    });
  });

  describe('Type code errors', () => {
    it('unknown type code throws error', async () => {
      await assertRejects(() => anySerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x99, 0x00]))), /unknown.*type_code/);
    });

    it('wrong type code for INT throws error', async () => {
      await assertRejects(() => intSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x02, 0x00, 0x00, 0x00, 0x00, 0x00]))), /unexpected.*type_code/);
    });

    it('wrong type code for LONG throws error', async () => {
      await assertRejects(() => longSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x01, 0x00, 0x00, 0x00, 0x00, 0x00]))), /unexpected.*type_code/);
    });

    it('wrong type code for STRING throws error', async () => {
      await assertRejects(() => stringSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x01, 0x00, 0x00, 0x00, 0x00, 0x00]))), /unexpected.*type_code/);
    });

    it('wrong type code for DOUBLE throws error', async () => {
      await assertRejects(() => ioc.doubleSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x01, 0x00]))), /unexpected.*type_code/);
    });

    it('wrong type code for BOOLEAN throws error', async () => {
      await assertRejects(() => ioc.booleanSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x01, 0x00]))), /unexpected.*type_code/);
    });

    it('wrong type code for UUID throws error', async () => {
      await assertRejects(() => uuidSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x01, 0x00]))), /unexpected.*type_code/);
    });

    it('wrong type code for LIST throws error', async () => {
      await assertRejects(() => listSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x01, 0x00]))), /unexpected.*type_code/);
    });

    it('wrong type code for MAP throws error', async () => {
      await assertRejects(() => mapSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x01, 0x00]))), /unexpected.*type_code/);
    });

    it('wrong type code for FLOAT throws error', async () => {
      await assertRejects(() => floatSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x09, 0x00]))), /unexpected.*type_code/);
    });

    it('wrong type code for SHORT throws error', async () => {
      await assertRejects(() => shortSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x27, 0x00]))), /unexpected.*type_code/);
    });

    it('wrong type code for BYTE throws error', async () => {
      await assertRejects(() => byteSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x25, 0x00]))), /unexpected.*type_code/);
    });

    it('wrong type code for BIGINTEGER throws error', async () => {
      await assertRejects(() => bigIntegerSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x24, 0x00]))), /unexpected.*type_code/);
    });

    it('wrong type code for BINARY throws error', async () => {
      await assertRejects(() => binarySerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x26, 0x00]))), /unexpected.*type_code/);
    });

    it('wrong type code for SET throws error', async () => {
      await assertRejects(() => setSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x0C, 0x00]))), /unexpected.*type_code/);
    });

    it('wrong type code for DATETIME throws error', async () => {
      await assertRejects(() => dateTimeSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x05, 0x00]))), /unexpected.*type_code/);
    });

    it('wrong type code for ENUM throws error', async () => {
      await assertRejects(() => enumSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x19, 0x00]))), /unexpected.*type_code/);
    });

    it('wrong type code for EDGE throws error', async () => {
      await assertRejects(() => edgeSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x01, 0x00]))), /unexpected.*type_code/);
    });

    it('wrong type code for VERTEX throws error', async () => {
      await assertRejects(() => vertexSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x01, 0x00]))), /unexpected.*type_code/);
    });

    it('wrong type code for VERTEXPROPERTY throws error', async () => {
      await assertRejects(() => vertexPropertySerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x01, 0x00]))), /unexpected.*type_code/);
    });

    it('wrong type code for PROPERTY throws error', async () => {
      await assertRejects(() => propertySerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x01, 0x00]))), /unexpected.*type_code/);
    });

    it('wrong type code for PATH throws error', async () => {
      await assertRejects(() => pathSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x01, 0x00]))), /unexpected.*type_code/);
    });

    it('wrong type code for MARKER throws error', async () => {
      await assertRejects(() => markerSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x01, 0x00]))), /unexpected.*type_code/);
    });
  });

  describe('Value flag errors', () => {
    it('invalid value_flag 0x03 for INT throws error', async () => {
      await assertRejects(() => intSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x01, 0x03]))), /unexpected.*value_flag/);
    });

    it('invalid value_flag 0x0F for STRING throws error', async () => {
      await assertRejects(() => stringSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x03, 0x0F]))), /unexpected.*value_flag/);
    });

    it('invalid value_flag 0xFF for LIST throws error', async () => {
      await assertRejects(() => listSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x09, 0xFF]))), /unexpected.*value_flag/);
    });

    it('invalid value_flag 0x05 for MAP throws error', async () => {
      await assertRejects(() => mapSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x0A, 0x05]))), /unexpected.*value_flag/);
    });

    it('invalid value_flag 0x10 for DOUBLE throws error', async () => {
      await assertRejects(() => ioc.doubleSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x07, 0x10]))), /unexpected.*value_flag/);
    });

    it('invalid value_flag 0x04 for FLOAT throws error', async () => {
      await assertRejects(() => floatSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x08, 0x04]))), /unexpected.*value_flag/);
    });

    it('invalid value_flag 0x06 for SHORT throws error', async () => {
      await assertRejects(() => shortSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x26, 0x06]))), /unexpected.*value_flag/);
    });

    it('invalid value_flag 0x07 for BYTE throws error', async () => {
      await assertRejects(() => byteSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x24, 0x07]))), /unexpected.*value_flag/);
    });

    it('invalid value_flag 0x08 for BIGINTEGER throws error', async () => {
      await assertRejects(() => bigIntegerSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x23, 0x08]))), /unexpected.*value_flag/);
    });

    it('invalid value_flag 0x09 for BINARY throws error', async () => {
      await assertRejects(() => binarySerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x25, 0x09]))), /unexpected.*value_flag/);
    });

    it('invalid value_flag 0x0A for SET throws error', async () => {
      await assertRejects(() => setSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x0B, 0x0A]))), /unexpected.*value_flag/);
    });

    it('invalid value_flag 0x0B for DATETIME throws error', async () => {
      await assertRejects(() => dateTimeSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x04, 0x0B]))), /unexpected.*value_flag/);
    });

    it('invalid value_flag 0x0C for UUID throws error', async () => {
      await assertRejects(() => uuidSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x0C, 0x0C]))), /unexpected.*value_flag/);
    });

    it('invalid value_flag 0x0D for ENUM throws error', async () => {
      await assertRejects(() => enumSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x18, 0x0D]))), /unexpected.*value_flag/);
    });

    it('invalid value_flag for EDGE throws error', async () => {
      await assertRejects(() => edgeSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x0D, 0x03]))), /unexpected.*value_flag/);
    });

    it('invalid value_flag for VERTEX throws error', async () => {
      await assertRejects(() => vertexSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x11, 0x03]))), /unexpected.*value_flag/);
    });

    it('invalid value_flag for VERTEXPROPERTY throws error', async () => {
      await assertRejects(() => vertexPropertySerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x12, 0x03]))), /unexpected.*value_flag/);
    });

    it('invalid value_flag for PROPERTY throws error', async () => {
      await assertRejects(() => propertySerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x0F, 0x03]))), /unexpected.*value_flag/);
    });

    it('invalid value_flag for PATH throws error', async () => {
      await assertRejects(() => pathSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x0E, 0x03]))), /unexpected.*value_flag/);
    });

    it('invalid value_flag for MARKER throws error', async () => {
      await assertRejects(() => markerSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0xFD, 0x03]))), /unexpected.*value_flag/);
    });

    it('invalid value_flag for BOOLEAN throws error', async () => {
      await assertRejects(() => ioc.booleanSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x27, 0x03]))), /unexpected.*value_flag/);
    });

    it('invalid value_flag for LONG throws error', async () => {
      await assertRejects(() => longSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x02, 0x03]))), /unexpected.*value_flag/);
    });

    it('invalid value_flag dispatched via anySerializer throws error', async () => {
      await assertRejects(() => anySerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x01, 0x0D]))), /unexpected.*value_flag/);
    });

    it('missing value_flag for INT throws error', async () => {
      await assertRejects(() => intSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x01]))), /Unexpected end of buffer/);
    });

    it('missing value_flag for STRING throws error', async () => {
      await assertRejects(() => stringSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x03]))), /Unexpected end of buffer/);
    });

    it('missing value_flag for LIST throws error', async () => {
      await assertRejects(() => listSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x09]))), /Unexpected end of buffer/);
    });

    it('missing value_flag for MAP throws error', async () => {
      await assertRejects(() => mapSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x0A]))), /Unexpected end of buffer/);
    });
  });

  describe('Truncated data', () => {
    it('INT with only 2 of 4 value bytes throws error', async () => {
      await assertRejects(() => intSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x01, 0x00, 0x00, 0x01]))), /Unexpected end of buffer/);
    });

    it('LONG with only 4 of 8 value bytes throws error', async () => {
      await assertRejects(() => longSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x02, 0x00, 0x00, 0x00, 0x00, 0x01]))), /Unexpected end of buffer/);
    });

    it('DOUBLE with only 4 of 8 value bytes throws error', async () => {
      await assertRejects(() => ioc.doubleSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x07, 0x00, 0x00, 0x00, 0x00, 0x01]))), /Unexpected end of buffer/);
    });

    it('FLOAT with only 2 of 4 value bytes throws error', async () => {
      await assertRejects(() => ioc.floatSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x08, 0x00, 0x00, 0x01]))), /Unexpected end of buffer/);
    });

    it('SHORT with only 1 of 2 value bytes throws error', async () => {
      await assertRejects(() => ioc.shortSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x26, 0x00, 0x01]))), /Unexpected end of buffer/);
    });

    it('STRING with length 10 but only 3 bytes throws error', async () => {
      const buffer = Buffer.concat([
        Buffer.from([0x03, 0x00]), // STRING, value_flag=0x00
        Buffer.from([0x00, 0x00, 0x00, 0x0A]), // length=10
        Buffer.from([0x41, 0x42, 0x43]) // only 3 bytes: "ABC"
      ]);
      await assertRejects(() => stringSerializer.deserialize(StreamReader.fromBuffer(buffer)), /Unexpected end of buffer/);
    });

    it('LIST with length 5 but only 1 item throws error', async () => {
      const buffer = Buffer.concat([
        Buffer.from([0x09, 0x00]), // LIST, value_flag=0x00
        Buffer.from([0x00, 0x00, 0x00, 0x05]), // length=5
        Buffer.from([0x01, 0x00, 0x00, 0x00, 0x00, 0x01]) // only 1 INT item
      ]);
      await assertRejects(() => listSerializer.deserialize(StreamReader.fromBuffer(buffer)), /Unexpected end of buffer/);
    });

    it('MAP with length 3 but only 1 entry throws error', async () => {
      const buffer = Buffer.concat([
        Buffer.from([0x0A, 0x00]), // MAP, value_flag=0x00
        Buffer.from([0x00, 0x00, 0x00, 0x03]), // length=3
        Buffer.from([0x03, 0x00, 0x00, 0x00, 0x00, 0x03, 0x6B, 0x65, 0x79]), // key: "key"
        Buffer.from([0x01, 0x00, 0x00, 0x00, 0x00, 0x01]) // value: 1
      ]);
      await assertRejects(() => mapSerializer.deserialize(StreamReader.fromBuffer(buffer)), /Unexpected end of buffer/);
    });

    it('UUID with only 8 of 16 bytes throws error', async () => {
      const buffer = Buffer.concat([
        Buffer.from([0x0C, 0x00]), // UUID, value_flag=0x00
        Buffer.from([0x41, 0xD2, 0xE2, 0x8A, 0x20, 0x13, 0x4E, 0x35]) // only 8 bytes
      ]);
      await assertRejects(() => uuidSerializer.deserialize(StreamReader.fromBuffer(buffer)), /Unexpected end of buffer/);
    });

    it('DATETIME with only 10 of 18 bytes throws error', async () => {
      const buffer = Buffer.concat([
        Buffer.from([0x04, 0x00]), // DATETIME, value_flag=0x00
        Buffer.from([0x07, 0xB2, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]) // only 10 bytes
      ]);
      await assertRejects(() => dateTimeSerializer.deserialize(StreamReader.fromBuffer(buffer)), /Unexpected end of buffer/);
    });

    it('BOOLEAN with no value byte throws error', async () => {
      await assertRejects(() => ioc.booleanSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x27, 0x00]))), /Unexpected end of buffer/);
    });

    it('BYTE with no value byte throws error', async () => {
      await assertRejects(() => ioc.byteSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x24, 0x00]))), /Unexpected end of buffer/);
    });

    it('BIGINTEGER with truncated length throws error', async () => {
      const buffer = Buffer.concat([
        Buffer.from([0x23, 0x00]), // BIGINTEGER, value_flag=0x00
        Buffer.from([0x00, 0x00]) // only 2 bytes of length instead of 4
      ]);
      await assertRejects(() => bigIntegerSerializer.deserialize(StreamReader.fromBuffer(buffer)), /Unexpected end of buffer/);
    });

    it('BIGINTEGER with length=0 throws error', async () => {
      const buffer = Buffer.concat([
        Buffer.from([0x23, 0x00]), // BIGINTEGER, value_flag=0x00
        Buffer.from([0x00, 0x00, 0x00, 0x00]) // length=0
      ]);
      await assertRejects(() => bigIntegerSerializer.deserialize(StreamReader.fromBuffer(buffer)), /length.*less than one|length.*0/);
    });

    it('BINARY with truncated value data throws error', async () => {
      const buffer = Buffer.concat([
        Buffer.from([0x25, 0x00]), // BINARY, value_flag=0x00
        Buffer.from([0x00, 0x00, 0x00, 0x05]), // length=5
        Buffer.from([0x01, 0x02]) // only 2 bytes instead of 5
      ]);
      await assertRejects(() => binarySerializer.deserialize(StreamReader.fromBuffer(buffer)), /Unexpected end of buffer/);
    });

    it('SET with length 5 but only 1 item throws error', async () => {
      const buffer = Buffer.concat([
        Buffer.from([0x0B, 0x00]), // SET, value_flag=0x00
        Buffer.from([0x00, 0x00, 0x00, 0x05]), // length=5
        Buffer.from([0x01, 0x00, 0x00, 0x00, 0x00, 0x01]) // only 1 INT item
      ]);
      await assertRejects(() => setSerializer.deserialize(StreamReader.fromBuffer(buffer)), /Unexpected end of buffer/);
    });

    it('ENUM with truncated elementName throws error', async () => {
      const buffer = Buffer.concat([
        Buffer.from([0x18, 0x00]), // DIRECTION, value_flag=0x00
        Buffer.from([0x03, 0x00, 0x00, 0x00, 0x00, 0x05]) // STRING with length=5 but no data
      ]);
      await assertRejects(() => enumSerializer.deserialize(StreamReader.fromBuffer(buffer)), /Unexpected end of buffer/);
    });
  });

  describe('Negative lengths', () => {
    it('STRING with negative length throws error', async () => {
      const buffer = Buffer.concat([
        Buffer.from([0x03, 0x00]), // STRING, value_flag=0x00
        Buffer.from([0xFF, 0xFF, 0xFF, 0xFF]) // length=-1
      ]);
      await assertRejects(() => stringSerializer.deserialize(StreamReader.fromBuffer(buffer)), /length.*less than zero/);
    });

    it('LIST with negative length throws error', async () => {
      const buffer = Buffer.concat([
        Buffer.from([0x09, 0x00]), // LIST, value_flag=0x00
        Buffer.from([0xFF, 0xFF, 0xFF, 0xFF]) // length=-1
      ]);
      await assertRejects(() => listSerializer.deserialize(StreamReader.fromBuffer(buffer)), /length.*less than zero/);
    });

    it('MAP with negative length throws error', async () => {
      const buffer = Buffer.concat([
        Buffer.from([0x0A, 0x00]), // MAP, value_flag=0x00
        Buffer.from([0xFF, 0xFF, 0xFF, 0xFF]) // length=-1
      ]);
      await assertRejects(() => mapSerializer.deserialize(StreamReader.fromBuffer(buffer)), /length.*less than zero/);
    });

    it('SET with negative length throws error', async () => {
      const buffer = Buffer.concat([
        Buffer.from([0x0B, 0x00]), // SET, value_flag=0x00
        Buffer.from([0xFF, 0xFF, 0xFF, 0xFF]) // length=-1
      ]);
      await assertRejects(() => setSerializer.deserialize(StreamReader.fromBuffer(buffer)), /length.*less than zero/);
    });

    it('BINARY with negative length throws error', async () => {
      const buffer = Buffer.concat([
        Buffer.from([0x25, 0x00]), // BINARY, value_flag=0x00
        Buffer.from([0xFF, 0xFF, 0xFF, 0xFF]) // length=-1
      ]);
      await assertRejects(() => binarySerializer.deserialize(StreamReader.fromBuffer(buffer)), /length.*less than zero/);
    });
  });

  describe('Bulk flag errors', () => {
    it('LIST with bulk flag but missing bulk count data throws error', async () => {
      const buffer = Buffer.concat([
        Buffer.from([0x09, 0x02]), // LIST, value_flag=0x02 (bulk)
        Buffer.from([0x00, 0x00, 0x00, 0x01]), // length=1
        Buffer.from([0x01, 0x00, 0x00, 0x00, 0x00, 0x01]) // 1 INT item but no bulk count
      ]);
      await assertRejects(() => listSerializer.deserialize(StreamReader.fromBuffer(buffer)), /Unexpected end of buffer/);
    });

    it('SET with bulk flag but missing bulk count data throws error', async () => {
      const buffer = Buffer.concat([
        Buffer.from([0x0B, 0x02]), // SET, value_flag=0x02 (bulk)
        Buffer.from([0x00, 0x00, 0x00, 0x01]), // length=1
        Buffer.from([0x01, 0x00, 0x00, 0x00, 0x00, 0x01]) // 1 INT item but no bulk count
      ]);
      await assertRejects(() => setSerializer.deserialize(StreamReader.fromBuffer(buffer)), /Unexpected end of buffer/);
    });
  });

  describe('UnspecifiedNull errors', () => {
    it('UnspecifiedNull with value_flag=0x00 throws error', async () => {
      // type_code=0xFE (UNSPECIFIED_NULL), value_flag=0x00 (non-null) — invalid
      await assertRejects(
        () => anySerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0xFE, 0x00]))),
        /UnspecifiedNull should always have value_flag=0x01/,
      );
    });
  });

  describe('Boolean value errors', () => {
    it('unexpected boolean byte 0x02 throws error', async () => {
      await assertRejects(
        () => ioc.booleanSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0x27, 0x00, 0x02]))),
        /unexpected boolean byte/,
      );
    });
  });

  describe('Marker value errors', () => {
    it('unexpected marker value throws error', async () => {
      // type_code=0xFD (MARKER), value_flag=0x00, value=0x01 (should be 0x00)
      await assertRejects(
        () => markerSerializer.deserialize(StreamReader.fromBuffer(Buffer.from([0xFD, 0x00, 0x01]))),
        /unexpected marker value/,
      );
    });
  });

  describe('Serialization rejection for unsupported types', () => {
    it('Symbol throws error', () => {
      assert.throws(() => anySerializer.serialize(Symbol('test')), /No serializer found to support item/);
    });

    it('function throws error', () => {
      assert.throws(() => anySerializer.serialize(() => {}), /No serializer found to support item/);
    });

    it('arrow function throws error', () => {
      assert.throws(() => anySerializer.serialize((x) => x + 1), /No serializer found to support item/);
    });

    it('async function throws error', () => {
      assert.throws(() => anySerializer.serialize(async () => {}), /No serializer found to support item/);
    });

    it('generator function throws error', () => {
      assert.throws(() => anySerializer.serialize(function* () {}), /No serializer found to support item/);
    });

    it('class constructor throws error', () => {
      class TestClass {}
      assert.throws(() => anySerializer.serialize(TestClass), /No serializer found to support item/);
    });
  });

  describe('v1-removed types serialization behavior', () => {
    it('P.eq("cole") serializes as Map (v1 types fall through to MapSerializer)', () => {
      // v1 P types no longer have dedicated serializers, so they serialize as Maps
      const result = anySerializer.serialize(P.eq('cole'));
      assert.ok(result.length > 0, 'Should serialize successfully as Map');
    });

    it('TextP.containing("x") serializes as Map (v1 types fall through to MapSerializer)', () => {
      // v1 TextP types no longer have dedicated serializers, so they serialize as Maps
      const result = anySerializer.serialize(TextP.containing('x'));
      assert.ok(result.length > 0, 'Should serialize successfully as Map');
    });

    it('new Traverser("cole", 1) serializes as Map (v1 types fall through to MapSerializer)', () => {
      // v1 Traverser types no longer have dedicated serializers, so they serialize as Maps
      const result = anySerializer.serialize(new Traverser('cole', 1));
      assert.ok(result.length > 0, 'Should serialize successfully as Map');
    });

    it('new OptionsStrategy({}) serializes as Map (v1 types fall through to MapSerializer)', () => {
      // v1 OptionsStrategy types no longer have dedicated serializers, so they serialize as Maps
      const result = anySerializer.serialize(new OptionsStrategy({}));
      assert.ok(result.length > 0, 'Should serialize successfully as Map');
    });
  });
});