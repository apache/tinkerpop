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
 * Error handling tests for GraphBinaryV4 serializers. Validates behavior for
 * malformed input: invalid buffers, unknown type codes, bad value flags,
 * truncated data, negative lengths, and unsupported types.
 */

import { assert } from 'chai';
import { Buffer } from 'buffer';
import ioc from '../../../lib/structure/io/binary/GraphBinary.js';
import { P, TextP, Traverser } from '../../../lib/process/traversal.js';
import { OptionsStrategy } from '../../../lib/process/traversal-strategy.js';

const { anySerializer, intSerializer, longSerializer, stringSerializer, listSerializer, mapSerializer, uuidSerializer, dateTimeSerializer, floatSerializer, shortSerializer, byteSerializer, bigIntegerSerializer, binarySerializer, setSerializer, enumSerializer } = ioc;

describe('GraphBinary v4 Error Cases', () => {
  describe('Buffer validation', () => {
    it('undefined buffer throws error', () => {
      assert.throws(() => anySerializer.deserialize(undefined), /buffer is missing/);
    });

    it('null buffer throws error', () => {
      assert.throws(() => anySerializer.deserialize(null), /buffer is missing/);
    });

    it('non-Buffer object throws error', () => {
      assert.throws(() => anySerializer.deserialize('not a buffer'), /buffer is missing/);
    });

    it('empty buffer throws error', () => {
      assert.throws(() => anySerializer.deserialize(Buffer.alloc(0)), /buffer is empty/);
    });

    it('buffer with only type_code, no value_flag throws error', () => {
      assert.throws(() => anySerializer.deserialize(Buffer.from([0x01])), /value_flag.*missing/);
    });

    it('individual serializer with undefined buffer throws error', () => {
      assert.throws(() => intSerializer.deserialize(undefined), /buffer is missing/);
    });

    it('individual serializer with null buffer throws error', () => {
      assert.throws(() => intSerializer.deserialize(null), /buffer is missing/);
    });

    it('individual serializer with empty buffer throws error', () => {
      assert.throws(() => intSerializer.deserialize(Buffer.alloc(0)), /buffer is empty/);
    });
  });

  describe('Type code errors', () => {
    it('unknown type code throws error', () => {
      assert.throws(() => anySerializer.deserialize(Buffer.from([0x99, 0x00])), /unknown.*type_code/);
    });

    it('wrong type code for INT throws error', () => {
      assert.throws(() => intSerializer.deserialize(Buffer.from([0x02, 0x00, 0x00, 0x00, 0x00, 0x00])), /unexpected.*type_code/);
    });

    it('wrong type code for LONG throws error', () => {
      assert.throws(() => longSerializer.deserialize(Buffer.from([0x01, 0x00, 0x00, 0x00, 0x00, 0x00])), /unexpected.*type_code/);
    });

    it('wrong type code for STRING throws error', () => {
      assert.throws(() => stringSerializer.deserialize(Buffer.from([0x01, 0x00, 0x00, 0x00, 0x00, 0x00])), /unexpected.*type_code/);
    });

    it('wrong type code for DOUBLE throws error', () => {
      assert.throws(() => ioc.doubleSerializer.deserialize(Buffer.from([0x01, 0x00])), /unexpected.*type_code/);
    });

    it('wrong type code for BOOLEAN throws error', () => {
      assert.throws(() => ioc.booleanSerializer.deserialize(Buffer.from([0x01, 0x00])), /unexpected.*type_code/);
    });

    it('wrong type code for UUID throws error', () => {
      assert.throws(() => uuidSerializer.deserialize(Buffer.from([0x01, 0x00])), /unexpected.*type_code/);
    });

    it('wrong type code for LIST throws error', () => {
      assert.throws(() => listSerializer.deserialize(Buffer.from([0x01, 0x00])), /unexpected.*type_code/);
    });

    it('wrong type code for MAP throws error', () => {
      assert.throws(() => mapSerializer.deserialize(Buffer.from([0x01, 0x00])), /unexpected.*type_code/);
    });

    it('wrong type code for FLOAT throws error', () => {
      assert.throws(() => floatSerializer.deserialize(Buffer.from([0x09, 0x00])), /unexpected.*type_code/);
    });

    it('wrong type code for SHORT throws error', () => {
      assert.throws(() => shortSerializer.deserialize(Buffer.from([0x27, 0x00])), /unexpected.*type_code/);
    });

    it('wrong type code for BYTE throws error', () => {
      assert.throws(() => byteSerializer.deserialize(Buffer.from([0x25, 0x00])), /unexpected.*type_code/);
    });

    it('wrong type code for BIGINTEGER throws error', () => {
      assert.throws(() => bigIntegerSerializer.deserialize(Buffer.from([0x24, 0x00])), /unexpected.*type_code/);
    });

    it('wrong type code for BINARY throws error', () => {
      assert.throws(() => binarySerializer.deserialize(Buffer.from([0x26, 0x00])), /unexpected.*type_code/);
    });

    it('wrong type code for SET throws error', () => {
      assert.throws(() => setSerializer.deserialize(Buffer.from([0x0C, 0x00])), /unexpected.*type_code/);
    });

    it('wrong type code for DATETIME throws error', () => {
      assert.throws(() => dateTimeSerializer.deserialize(Buffer.from([0x05, 0x00])), /unexpected.*type_code/);
    });

    it('wrong type code for ENUM throws error', () => {
      assert.throws(() => enumSerializer.deserialize(Buffer.from([0x19, 0x00])), /unexpected.*type_code/);
    });
  });

  describe('Value flag errors', () => {
    it('invalid value_flag 0x03 for INT throws error', () => {
      assert.throws(() => intSerializer.deserialize(Buffer.from([0x01, 0x03])), /unexpected.*value_flag/);
    });

    it('invalid value_flag 0x0F for STRING throws error', () => {
      assert.throws(() => stringSerializer.deserialize(Buffer.from([0x03, 0x0F])), /unexpected.*value_flag/);
    });

    it('invalid value_flag 0xFF for LIST throws error', () => {
      assert.throws(() => listSerializer.deserialize(Buffer.from([0x09, 0xFF])), /unexpected.*value_flag/);
    });

    it('invalid value_flag 0x05 for MAP throws error', () => {
      assert.throws(() => mapSerializer.deserialize(Buffer.from([0x0A, 0x05])), /unexpected.*value_flag/);
    });

    it('invalid value_flag 0x10 for DOUBLE throws error', () => {
      assert.throws(() => ioc.doubleSerializer.deserialize(Buffer.from([0x07, 0x10])), /unexpected.*value_flag/);
    });

    it('invalid value_flag 0x04 for FLOAT throws error', () => {
      assert.throws(() => floatSerializer.deserialize(Buffer.from([0x08, 0x04])), /unexpected.*value_flag/);
    });

    it('invalid value_flag 0x06 for SHORT throws error', () => {
      assert.throws(() => shortSerializer.deserialize(Buffer.from([0x26, 0x06])), /unexpected.*value_flag/);
    });

    it('invalid value_flag 0x07 for BYTE throws error', () => {
      assert.throws(() => byteSerializer.deserialize(Buffer.from([0x24, 0x07])), /unexpected.*value_flag/);
    });

    it('invalid value_flag 0x08 for BIGINTEGER throws error', () => {
      assert.throws(() => bigIntegerSerializer.deserialize(Buffer.from([0x23, 0x08])), /unexpected.*value_flag/);
    });

    it('invalid value_flag 0x09 for BINARY throws error', () => {
      assert.throws(() => binarySerializer.deserialize(Buffer.from([0x25, 0x09])), /unexpected.*value_flag/);
    });

    it('invalid value_flag 0x0A for SET throws error', () => {
      assert.throws(() => setSerializer.deserialize(Buffer.from([0x0B, 0x0A])), /unexpected.*value_flag/);
    });

    it('invalid value_flag 0x0B for DATETIME throws error', () => {
      assert.throws(() => dateTimeSerializer.deserialize(Buffer.from([0x04, 0x0B])), /unexpected.*value_flag/);
    });

    it('invalid value_flag 0x0C for UUID throws error', () => {
      assert.throws(() => uuidSerializer.deserialize(Buffer.from([0x0C, 0x0C])), /unexpected.*value_flag/);
    });

    it('invalid value_flag 0x0D for ENUM throws error', () => {
      assert.throws(() => enumSerializer.deserialize(Buffer.from([0x18, 0x0D])), /unexpected.*value_flag/);
    });

    it('missing value_flag for INT throws error', () => {
      assert.throws(() => intSerializer.deserialize(Buffer.from([0x01])), /value_flag.*missing/);
    });

    it('missing value_flag for STRING throws error', () => {
      assert.throws(() => stringSerializer.deserialize(Buffer.from([0x03])), /value_flag.*missing/);
    });

    it('missing value_flag for LIST throws error', () => {
      assert.throws(() => listSerializer.deserialize(Buffer.from([0x09])), /value_flag.*missing/);
    });

    it('missing value_flag for MAP throws error', () => {
      assert.throws(() => mapSerializer.deserialize(Buffer.from([0x0A])), /value_flag.*missing/);
    });
  });

  describe('Truncated data', () => {
    it('INT with only 2 of 4 value bytes throws error', () => {
      assert.throws(() => intSerializer.deserialize(Buffer.from([0x01, 0x00, 0x00, 0x01])), /unexpected.*value.*length/);
    });

    it('LONG with only 4 of 8 value bytes throws error', () => {
      assert.throws(() => longSerializer.deserialize(Buffer.from([0x02, 0x00, 0x00, 0x00, 0x00, 0x01])), /unexpected.*value.*length/);
    });

    it('DOUBLE with only 4 of 8 value bytes throws error', () => {
      assert.throws(() => ioc.doubleSerializer.deserialize(Buffer.from([0x07, 0x00, 0x00, 0x00, 0x00, 0x01])), /unexpected.*value.*length/);
    });

    it('FLOAT with only 2 of 4 value bytes throws error', () => {
      assert.throws(() => ioc.floatSerializer.deserialize(Buffer.from([0x08, 0x00, 0x00, 0x01])), /unexpected.*value.*length/);
    });

    it('SHORT with only 1 of 2 value bytes throws error', () => {
      assert.throws(() => ioc.shortSerializer.deserialize(Buffer.from([0x26, 0x00, 0x01])), /unexpected.*value.*length/);
    });

    it('STRING with length 10 but only 3 bytes throws error', () => {
      const buffer = Buffer.concat([
        Buffer.from([0x03, 0x00]), // STRING, value_flag=0x00
        Buffer.from([0x00, 0x00, 0x00, 0x0A]), // length=10
        Buffer.from([0x41, 0x42, 0x43]) // only 3 bytes: "ABC"
      ]);
      assert.throws(() => stringSerializer.deserialize(buffer), /unexpected.*text_value.*length/);
    });

    it('LIST with length 5 but only 1 item throws error', () => {
      const buffer = Buffer.concat([
        Buffer.from([0x09, 0x00]), // LIST, value_flag=0x00
        Buffer.from([0x00, 0x00, 0x00, 0x05]), // length=5
        Buffer.from([0x01, 0x00, 0x00, 0x00, 0x00, 0x01]) // only 1 INT item
      ]);
      assert.throws(() => listSerializer.deserialize(buffer), /item_1.*buffer is empty/);
    });

    it('MAP with length 3 but only 1 entry throws error', () => {
      const buffer = Buffer.concat([
        Buffer.from([0x0A, 0x00]), // MAP, value_flag=0x00
        Buffer.from([0x00, 0x00, 0x00, 0x03]), // length=3
        Buffer.from([0x03, 0x00, 0x00, 0x00, 0x00, 0x03, 0x6B, 0x65, 0x79]), // key: "key"
        Buffer.from([0x01, 0x00, 0x00, 0x00, 0x00, 0x01]) // value: 1
      ]);
      assert.throws(() => mapSerializer.deserialize(buffer), /{item_1}.*buffer is empty/);
    });

    it('UUID with only 8 of 16 bytes throws error', () => {
      const buffer = Buffer.concat([
        Buffer.from([0x0C, 0x00]), // UUID, value_flag=0x00
        Buffer.from([0x41, 0xD2, 0xE2, 0x8A, 0x20, 0x13, 0x4E, 0x35]) // only 8 bytes
      ]);
      assert.throws(() => uuidSerializer.deserialize(buffer), /unexpected.*value.*length/);
    });

    it('DATETIME with only 10 of 18 bytes throws error', () => {
      const buffer = Buffer.concat([
        Buffer.from([0x04, 0x00]), // DATETIME, value_flag=0x00
        Buffer.from([0x07, 0xB2, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]) // only 10 bytes
      ]);
      assert.throws(() => dateTimeSerializer.deserialize(buffer), /unexpected.*value.*length/);
    });

    it('BOOLEAN with no value byte throws error', () => {
      assert.throws(() => ioc.booleanSerializer.deserialize(Buffer.from([0x27, 0x00])), /unexpected.*value.*length/);
    });

    it('BYTE with no value byte throws error', () => {
      assert.throws(() => ioc.byteSerializer.deserialize(Buffer.from([0x24, 0x00])), /unexpected.*value.*length/);
    });

    it('BIGINTEGER with truncated length throws error', () => {
      const buffer = Buffer.concat([
        Buffer.from([0x23, 0x00]), // BIGINTEGER, value_flag=0x00
        Buffer.from([0x00, 0x00]) // only 2 bytes of length instead of 4
      ]);
      assert.throws(() => bigIntegerSerializer.deserialize(buffer), /{length}.*unexpected.*value.*length/);
    });

    it('BIGINTEGER with length=0 throws error', () => {
      const buffer = Buffer.concat([
        Buffer.from([0x23, 0x00]), // BIGINTEGER, value_flag=0x00
        Buffer.from([0x00, 0x00, 0x00, 0x00]) // length=0
      ]);
      assert.throws(() => bigIntegerSerializer.deserialize(buffer), /{length}=0 is less than one/);
    });

    it('BINARY with truncated value data throws error', () => {
      const buffer = Buffer.concat([
        Buffer.from([0x25, 0x00]), // BINARY, value_flag=0x00
        Buffer.from([0x00, 0x00, 0x00, 0x05]), // length=5
        Buffer.from([0x01, 0x02]) // only 2 bytes instead of 5
      ]);
      assert.throws(() => binarySerializer.deserialize(buffer), /{value}.*unexpected.*value.*length/);
    });

    it('SET with length 5 but only 1 item throws error', () => {
      const buffer = Buffer.concat([
        Buffer.from([0x0B, 0x00]), // SET, value_flag=0x00
        Buffer.from([0x00, 0x00, 0x00, 0x05]), // length=5
        Buffer.from([0x01, 0x00, 0x00, 0x00, 0x00, 0x01]) // only 1 INT item
      ]);
      assert.throws(() => setSerializer.deserialize(buffer), /{item_1}.*buffer is empty/);
    });

    it('ENUM with truncated elementName throws error', () => {
      const buffer = Buffer.concat([
        Buffer.from([0x18, 0x00]), // DIRECTION, value_flag=0x00
        Buffer.from([0x03, 0x00, 0x00, 0x00, 0x00, 0x05]) // STRING with length=5 but no data
      ]);
      assert.throws(() => enumSerializer.deserialize(buffer), /elementName.*unexpected.*text_value.*length/);
    });
  });

  describe('Negative lengths', () => {
    it('STRING with negative length throws error', () => {
      const buffer = Buffer.concat([
        Buffer.from([0x03, 0x00]), // STRING, value_flag=0x00
        Buffer.from([0xFF, 0xFF, 0xFF, 0xFF]) // length=-1
      ]);
      assert.throws(() => stringSerializer.deserialize(buffer), /length.*less than zero/);
    });

    it('LIST with negative length throws error', () => {
      const buffer = Buffer.concat([
        Buffer.from([0x09, 0x00]), // LIST, value_flag=0x00
        Buffer.from([0xFF, 0xFF, 0xFF, 0xFF]) // length=-1
      ]);
      assert.throws(() => listSerializer.deserialize(buffer), /length.*less than zero/);
    });

    it('MAP with negative length throws error', () => {
      const buffer = Buffer.concat([
        Buffer.from([0x0A, 0x00]), // MAP, value_flag=0x00
        Buffer.from([0xFF, 0xFF, 0xFF, 0xFF]) // length=-1
      ]);
      assert.throws(() => mapSerializer.deserialize(buffer), /length.*less than zero/);
    });

    it('SET with negative length throws error', () => {
      const buffer = Buffer.concat([
        Buffer.from([0x0B, 0x00]), // SET, value_flag=0x00
        Buffer.from([0xFF, 0xFF, 0xFF, 0xFF]) // length=-1
      ]);
      assert.throws(() => ioc.setSerializer.deserialize(buffer), /length.*less than zero/);
    });

    it('BINARY with negative length throws error', () => {
      const buffer = Buffer.concat([
        Buffer.from([0x25, 0x00]), // BINARY, value_flag=0x00
        Buffer.from([0xFF, 0xFF, 0xFF, 0xFF]) // length=-1
      ]);
      assert.throws(() => ioc.binarySerializer.deserialize(buffer), /length.*less than zero/);
    });
  });

  describe('Bulk flag errors', () => {
    it('LIST with bulk flag but missing bulk count data throws error', () => {
      const buffer = Buffer.concat([
        Buffer.from([0x09, 0x02]), // LIST, value_flag=0x02 (bulk)
        Buffer.from([0x00, 0x00, 0x00, 0x01]), // length=1
        Buffer.from([0x01, 0x00, 0x00, 0x00, 0x00, 0x01]) // 1 INT item but no bulk count
      ]);
      assert.throws(() => listSerializer.deserialize(buffer), /{item_0}.*bulk count is missing/);
    });

    it('SET with bulk flag but missing bulk count data throws error', () => {
      const buffer = Buffer.concat([
        Buffer.from([0x0B, 0x02]), // SET, value_flag=0x02 (bulk)
        Buffer.from([0x00, 0x00, 0x00, 0x01]), // length=1
        Buffer.from([0x01, 0x00, 0x00, 0x00, 0x00, 0x01]) // 1 INT item but no bulk count
      ]);
      assert.throws(() => setSerializer.deserialize(buffer), /{item_0}.*bulk count is missing/);
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