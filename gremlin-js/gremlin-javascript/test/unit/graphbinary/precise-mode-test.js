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

import assert from 'assert';
import ioc, { createPreciseReader, DataType } from '../../../lib/structure/io/binary/GraphBinary.js';
import StreamReader from '../../../lib/structure/io/binary/internals/StreamReader.js';
import { Float, Double, Int, Long, Short, Byte, toFloat, toDouble, toInt, toLong, toShort, toByte, unwrap } from '../../../lib/utils.js';
import Connection from '../../../lib/driver/connection.js';
import { Path } from '../../../lib/structure/graph.js';

const { anySerializer, graphBinaryReader } = ioc;

describe('Precise Mode Tests', () => {
  let preciseReader;

  before(() => {
    preciseReader = createPreciseReader();
  });

  async function deserializeWithPrecise(buf) {
    return preciseReader.ioc.anySerializer.deserialize(StreamReader.fromBuffer(buf));
  }

  async function deserializeWithDefault(buf) {
    return anySerializer.deserialize(StreamReader.fromBuffer(buf));
  }

  describe('Basic deserialization', () => {
    it('FLOAT bytes → Float instance', async () => {
      const buf = anySerializer.serialize(toFloat(1.5));
      const result = await deserializeWithPrecise(buf);
      assert.ok(result instanceof Float);
      assert.strictEqual(result.value, 1.5);
      assert.strictEqual(result.type, 'float');
    });

    it('DOUBLE bytes → Double instance', async () => {
      const buf = anySerializer.serialize(toDouble(3.14));
      const result = await deserializeWithPrecise(buf);
      assert.ok(result instanceof Double);
      assert.strictEqual(result.value, 3.14);
      assert.strictEqual(result.type, 'double');
    });

    it('INT bytes → Int instance', async () => {
      const buf = anySerializer.serialize(toInt(42));
      const result = await deserializeWithPrecise(buf);
      assert.ok(result instanceof Int);
      assert.strictEqual(result.value, 42);
      assert.strictEqual(result.type, 'int');
    });

    it('LONG bytes (safe range) → Long instance with number value', async () => {
      const buf = anySerializer.serialize(toLong(42));
      const result = await deserializeWithPrecise(buf);
      assert.ok(result instanceof Long);
      assert.strictEqual(result.value, 42);
      assert.strictEqual(result.type, 'long');
    });

    it('LONG bytes (unsafe range) → Long instance with bigint value', async () => {
      const buf = anySerializer.serialize(toLong(9007199254740993n));
      const result = await deserializeWithPrecise(buf);
      assert.ok(result instanceof Long);
      assert.strictEqual(result.value, 9007199254740993n);
    });

    it('SHORT bytes → Short instance', async () => {
      const buf = anySerializer.serialize(toShort(5));
      const result = await deserializeWithPrecise(buf);
      assert.ok(result instanceof Short);
      assert.strictEqual(result.value, 5);
      assert.strictEqual(result.type, 'short');
    });

    it('BYTE bytes → Byte instance', async () => {
      const buf = anySerializer.serialize(toByte(127));
      const result = await deserializeWithPrecise(buf);
      assert.ok(result instanceof Byte);
      assert.strictEqual(result.value, 127);
      assert.strictEqual(result.type, 'byte');
    });
  });

  describe('Nested structures', () => {
    it('MAP containing Float values → Float wrappers inside Map', async () => {
      const map = new Map([['x', toFloat(1.5)]]);
      const buf = anySerializer.serialize(map);
      const result = await deserializeWithPrecise(buf);
      assert.ok(result.get('x') instanceof Float);
      assert.strictEqual(result.get('x').value, 1.5);
    });

    it('LIST with mixed numeric types → each wrapped correctly', async () => {
      const list = [toFloat(1.5), toInt(2), toDouble(3.14)];
      const buf = anySerializer.serialize(list);
      const result = await deserializeWithPrecise(buf);
      assert.ok(result[0] instanceof Float);
      assert.ok(result[1] instanceof Int);
      assert.ok(result[2] instanceof Double);
    });

    it('PATH with numeric vertex IDs → wrapped correctly', async () => {
      const path = new Path([['a'], ['b']], [toInt(1), toInt(2)]);
      const buf = anySerializer.serialize(path);
      const result = await deserializeWithPrecise(buf);
      assert.ok(result instanceof Path);
      assert.ok(result.objects[0] instanceof Int);
      assert.strictEqual(result.objects[0].value, 1);
      assert.ok(result.objects[1] instanceof Int);
      assert.strictEqual(result.objects[1].value, 2);
    });

    it('LIST with Short, Byte, Long → each wrapped correctly', async () => {
      const list = [toShort(5), toByte(127), toLong(99)];
      const buf = anySerializer.serialize(list);
      const result = await deserializeWithPrecise(buf);
      assert.ok(result[0] instanceof Short);
      assert.strictEqual(result[0].value, 5);
      assert.ok(result[1] instanceof Byte);
      assert.strictEqual(result[1].value, 127);
      assert.ok(result[2] instanceof Long);
      assert.strictEqual(result[2].value, 99);
    });

    it('Non-numeric types still deserialize correctly', async () => {
      const list = ['hello', true, 'world'];
      const buf = anySerializer.serialize(list);
      const result = await deserializeWithPrecise(buf);
      assert.deepStrictEqual(result, ['hello', true, 'world']);
    });
  });

  describe('Precise reader error paths', () => {
    it('returns null for a null-flagged numeric value', async () => {
      const buf = Buffer.from([DataType.INT, 0x01]);
      const result = await deserializeWithPrecise(buf);
      assert.strictEqual(result, null);
    });

    it('returns null for a null-flagged LONG value', async () => {
      const buf = Buffer.from([DataType.LONG, 0x01]);
      assert.strictEqual(await deserializeWithPrecise(buf), null);
    });

    it('returns null for a null-flagged FLOAT value', async () => {
      const buf = Buffer.from([DataType.FLOAT, 0x01]);
      assert.strictEqual(await deserializeWithPrecise(buf), null);
    });

    it('returns null for a null-flagged DOUBLE value', async () => {
      const buf = Buffer.from([DataType.DOUBLE, 0x01]);
      assert.strictEqual(await deserializeWithPrecise(buf), null);
    });

    it('returns null for a null-flagged SHORT value', async () => {
      const buf = Buffer.from([DataType.SHORT, 0x01]);
      assert.strictEqual(await deserializeWithPrecise(buf), null);
    });

    it('returns null for a null-flagged BYTE value', async () => {
      const buf = Buffer.from([DataType.BYTE, 0x01]);
      assert.strictEqual(await deserializeWithPrecise(buf), null);
    });

    it('throws on invalid value_flag', async () => {
      const buf = Buffer.from([DataType.INT, 0xFF, 0, 0, 0, 0]);
      await assert.rejects(() => deserializeWithPrecise(buf), /AnySerializer: unexpected \{value_flag}=0x/);
    });

    it('throws on unknown type_code', async () => {
      const buf = Buffer.from([0xEE, 0x00]);
      await assert.rejects(() => deserializeWithPrecise(buf), /AnySerializer: unknown \{type_code}=0xee/);
    });

    it('deserializes value_flag 0x02 as non-null', async () => {
      const buf = Buffer.from([DataType.INT, 0x02, 0x00, 0x00, 0x00, 0x2A]);
      const result = await deserializeWithPrecise(buf);
      assert.ok(result instanceof Int);
      assert.strictEqual(result.value, 42);
    });

    it('wraps deserializeValue errors with position info', async () => {
      const buf = Buffer.from([DataType.INT, 0x00]);
      await assert.rejects(() => deserializeWithPrecise(buf), /IntSerializer\.deserializeValue\(\) at position \d+/);
    });
  });

  describe('Wrapper behavior', () => {
    it('Float valueOf works in arithmetic', () => {
      assert.strictEqual(new Float(1.5) + 1, 2.5);
    });

    it('Int valueOf works in arithmetic', () => {
      assert.strictEqual(new Int(42) + 0, 42);
    });

    it('Double valueOf works in arithmetic', () => {
      assert.strictEqual(new Double(3.14) + 0, 3.14);
    });

    it('Short valueOf works in arithmetic', () => {
      assert.strictEqual(new Short(5) + 1, 6);
    });

    it('Byte valueOf works in arithmetic', () => {
      assert.strictEqual(new Byte(127) + 0, 127);
    });

    it('Long with unsafe bigint throws RangeError on arithmetic', () => {
      assert.throws(() => new Long(9007199254740993n) + 1, RangeError);
    });

    it('Long with unsafe string throws RangeError on arithmetic', () => {
      assert.throws(() => new Long('9007199254740993') + 1, RangeError);
    });

    it('Long with safe bigint works in arithmetic', () => {
      assert.strictEqual(new Long(42n) + 1, 43);
    });

    it('Long toPrimitive string hint works for unsafe values', () => {
      assert.strictEqual(`${new Long(9007199254740993n)}`, '9007199254740993');
    });

    it('Float toPrimitive string hint', () => {
      assert.strictEqual(`${new Float(1.5)}`, '1.5');
    });

    it('Double toPrimitive string hint', () => {
      assert.strictEqual(`${new Double(3.14)}`, '3.14');
    });

    it('Int toPrimitive string hint', () => {
      assert.strictEqual(`${new Int(67)}`, '67');
    });

    it('Short toPrimitive string hint', () => {
      assert.strictEqual(`${new Short(5)}`, '5');
    });

    it('Byte toPrimitive string hint', () => {
      assert.strictEqual(`${new Byte(127)}`, '127');
    });

    it('JSON.stringify Float', () => {
      assert.strictEqual(JSON.stringify(new Float(1.5)), '1.5');
    });

    it('JSON.stringify Long with bigint', () => {
      assert.strictEqual(JSON.stringify(new Long(9007199254740993n)), '"9007199254740993"');
    });

    it('Long valueOf with safe string returns number', () => {
      assert.strictEqual(new Long('42').valueOf(), 42);
    });

    it('Long valueOf with unsafe string throws RangeError', () => {
      assert.throws(() => new Long('9007199254740993').valueOf(), RangeError);
    });

    it('JSON.stringify Long with string value preserves string', () => {
      assert.strictEqual(JSON.stringify(new Long('9007199254740993')), '"9007199254740993"');
    });

    it('JSON.stringify Long with number value is a number', () => {
      assert.strictEqual(JSON.stringify(new Long(42)), '42');
    });

    it('JSON.stringify Double', () => {
      assert.strictEqual(JSON.stringify(new Double(3.14)), '3.14');
    });

    it('JSON.stringify Int', () => {
      assert.strictEqual(JSON.stringify(new Int(67)), '67');
    });

    it('JSON.stringify Short', () => {
      assert.strictEqual(JSON.stringify(new Short(5)), '5');
    });

    it('JSON.stringify Byte', () => {
      assert.strictEqual(JSON.stringify(new Byte(127)), '127');
    });

    it('unwrap Float', () => {
      assert.strictEqual(unwrap(new Float(1.5)), 1.5);
    });

    it('unwrap Long with bigint', () => {
      assert.strictEqual(unwrap(new Long(42n)), 42n);
    });

    it('unwrap Long with string', () => {
      assert.strictEqual(unwrap(new Long('123')), '123');
    });

    it('unwrap Long with number', () => {
      assert.strictEqual(unwrap(new Long(42)), 42);
    });

    it('unwrap Int', () => {
      assert.strictEqual(unwrap(new Int(42)), 42);
    });

    it('unwrap Double', () => {
      assert.strictEqual(unwrap(new Double(3.14)), 3.14);
    });

    it('unwrap Short', () => {
      assert.strictEqual(unwrap(new Short(5)), 5);
    });

    it('unwrap Byte', () => {
      assert.strictEqual(unwrap(new Byte(127)), 127);
    });

    it('unwrap plain number passthrough', () => {
      assert.strictEqual(unwrap(42), 42);
    });

    it('unwrap null passthrough', () => {
      assert.strictEqual(unwrap(null), null);
    });

    it('unwrap undefined passthrough', () => {
      assert.strictEqual(unwrap(undefined), undefined);
    });
  });

  describe('Long constructor validation', () => {
    it('rejects non-numeric string', () => {
      assert.throws(() => new Long('abc'), TypeError);
    });

    it('rejects injection attempt', () => {
      assert.throws(() => new Long('1L).drop()'), TypeError);
    });

    it('rejects empty string', () => {
      assert.throws(() => new Long(''), TypeError);
    });

    it('rejects non-integer number', () => {
      assert.throws(() => new Long(1.5), TypeError);
    });

    it('accepts exact int64 max (bigint)', () => {
      const l = new Long(9223372036854775807n);
      assert.strictEqual(l.value, 9223372036854775807n);
    });

    it('accepts exact int64 min (bigint)', () => {
      const l = new Long(-9223372036854775808n);
      assert.strictEqual(l.value, -9223372036854775808n);
    });

    it('rejects one above int64 max (bigint)', () => {
      assert.throws(() => new Long(9223372036854775808n), RangeError);
    });

    it('rejects one below int64 min (bigint)', () => {
      assert.throws(() => new Long(-9223372036854775809n), RangeError);
    });

    it('accepts exact int64 max (string)', () => {
      const l = new Long('9223372036854775807');
      assert.strictEqual(l.value, '9223372036854775807');
    });

    it('rejects one above int64 max (string)', () => {
      assert.throws(() => new Long('9223372036854775808'), RangeError);
    });

    it('rejects negative zero string', () => {
      assert.throws(() => new Long('-0'), TypeError);
    });

    it('accepts negative zero number', () => {
      const l = new Long(-0);
      assert.strictEqual(l.valueOf(), -0);
    });

    it('rejects leading zeros in string', () => {
      assert.throws(() => new Long('0042'), TypeError);
    });
  });

  describe('Float constructor validation', () => {
    it('rejects non-number argument', () => {
      assert.throws(() => new Float('1.5'), TypeError);
    });
  });

  describe('Double constructor validation', () => {
    it('rejects non-number argument', () => {
      assert.throws(() => new Double('3.14'), TypeError);
    });
  });

  describe('Int constructor validation', () => {
    it('accepts exact int32 max', () => {
      assert.strictEqual(new Int(2147483647).value, 2147483647);
    });

    it('accepts exact int32 min', () => {
      assert.strictEqual(new Int(-2147483648).value, -2147483648);
    });

    it('rejects above int32 max', () => {
      assert.throws(() => new Int(2147483648), RangeError);
    });

    it('rejects below int32 min', () => {
      assert.throws(() => new Int(-2147483649), RangeError);
    });

    it('rejects non-integer', () => {
      assert.throws(() => new Int(1.5), TypeError);
    });

    it('rejects non-number argument', () => {
      assert.throws(() => new Int('5'), TypeError);
    });
  });

  describe('Short constructor validation', () => {
    it('accepts exact int16 max', () => {
      assert.strictEqual(new Short(32767).value, 32767);
    });

    it('accepts exact int16 min', () => {
      assert.strictEqual(new Short(-32768).value, -32768);
    });

    it('rejects above int16 max', () => {
      assert.throws(() => new Short(32768), RangeError);
    });

    it('rejects below int16 min', () => {
      assert.throws(() => new Short(-32769), RangeError);
    });

    it('rejects non-integer', () => {
      assert.throws(() => new Short(1.5), TypeError);
    });

    it('rejects non-number argument', () => {
      assert.throws(() => new Short('5'), TypeError);
    });
  });

  describe('Byte constructor validation', () => {
    it('accepts exact int8 max', () => {
      assert.strictEqual(new Byte(127).value, 127);
    });

    it('accepts exact int8 min', () => {
      assert.strictEqual(new Byte(-128).value, -128);
    });

    it('rejects above int8 max', () => {
      assert.throws(() => new Byte(128), RangeError);
    });

    it('rejects below int8 min', () => {
      assert.throws(() => new Byte(-129), RangeError);
    });

    it('rejects non-integer', () => {
      assert.throws(() => new Byte(1.5), TypeError);
    });

    it('rejects non-number argument', () => {
      assert.throws(() => new Byte('127'), TypeError);
    });
  });

  describe('Backward compatibility', () => {
    it('default graphBinaryReader still returns plain numbers after createPreciseReader()', async () => {
      const buf = anySerializer.serialize(toFloat(1.5));
      const result = await deserializeWithDefault(buf);
      assert.strictEqual(result, 1.5);
      assert.ok(!(result instanceof Float));
    });
  });

  describe('Connection option wiring', () => {
    it('preciseNumbers: true uses a precise reader', () => {
      const conn = new Connection('http://localhost:8182', { preciseNumbers: true });
      assert.ok(conn._reader !== graphBinaryReader);
    });

    it('explicit reader takes precedence over preciseNumbers', () => {
      const customReader = { custom: true };
      const conn = new Connection('http://localhost:8182', { reader: customReader, preciseNumbers: true });
      assert.strictEqual(conn._reader, customReader);
    });

    it('default uses the default reader', () => {
      const conn = new Connection('http://localhost:8182', {});
      assert.ok(conn._reader instanceof graphBinaryReader.constructor);
    });
  });
});
