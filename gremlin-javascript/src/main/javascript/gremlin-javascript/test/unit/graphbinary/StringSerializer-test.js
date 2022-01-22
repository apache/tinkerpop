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

/**
 * @author Igor Ostapenko
 */
'use strict';

const assert = require('assert');
const { stringSerializer } = require('../../../lib/structure/io/binary/GraphBinary');

const { from, concat } = Buffer;

describe('GraphBinary.StringSerializer', () => {

  const type_code =  from([0x03]);
  const value_flag = from([0x00]);

  const cases = [
    {        v:undefined,                      fq:1,       b:[0x03,0x01], av:null },
    {        v:undefined,                      fq:0,       b:[0x00,0x00,0x00,0x00], av:'' },
    {        v:null,                           fq:1,       b:[0x03,0x01] },
    {        v:null,                           fq:0,       b:[0x00,0x00,0x00,0x00], av:'' },
    { des:1, v:null,                           fq:0, na:1, b:[0x01] },

    { v:'',                                                b:[0x00,0x00,0x00,0x00] },
    { v:'Sun',                                             b:[0x00,0x00,0x00,0x03, 0x53,0x75,0x6E] },
    { v:'ήλιος',                                           b:[0x00,0x00,0x00,0x0A, 0xCE,0xAE, 0xCE,0xBB, 0xCE,0xB9, 0xCE,0xBF, 0xCF,0x82] },

    // TODO: Should we complain on wrong UTF-8 deserialization?
    // It could be additional latency if we explicitly check entire string for correct encoding (if we avoid custom parser impl).
    // And, probably, it's better to provide something instead of throwing an error and failing entire response.
    // For now invalid byte sequences are stubbed with 0xFFFD according to https://nodejs.org/dist/latest/docs/api/buffer.html#buftostringencoding-start-end

    { des:1, err:/buffer is missing/,          fq:1,       b:undefined },
    { des:1, err:/buffer is missing/,          fq:0,       b:undefined },
    { des:1, err:/buffer is missing/,          fq:1,       b:null },
    { des:1, err:/buffer is missing/,          fq:0,       b:null },
    { des:1, err:/buffer is empty/,            fq:1,       b:[] },
    { des:1, err:/buffer is empty/,            fq:0,       b:[] },
    { des:1, err:/buffer is empty/,            fq:0, na:1, b:[] },

    { des:1, err:/unexpected {type_code}/,     fq:1,       b:[0x00] },
    { des:1, err:/unexpected {type_code}/,     fq:1,       b:[0x02] },
    { des:1, err:/unexpected {type_code}/,     fq:1,       b:[0x04] },
    { des:1, err:/unexpected {type_code}/,     fq:1,       b:[0x30] },
    { des:1, err:/unexpected {type_code}/,     fq:1,       b:[0x83] },
    { des:1, err:/unexpected {type_code}/,     fq:1,       b:[0xFF] },

    { des:1, err:/{value_flag} is missing/,    fq:1,       b:[0x03] },
    { des:1, err:/unexpected {value_flag}/,    fq:1,       b:[0x03,0x10] },
    { des:1, err:/unexpected {value_flag}/,    fq:0, na:1, b:[0x10] },
    { des:1, err:/unexpected {value_flag}/,    fq:1,       b:[0x03,0x02] },
    { des:1, err:/unexpected {value_flag}/,    fq:0, na:1, b:[0x02] },
    { des:1, err:/unexpected {value_flag}/,    fq:1,       b:[0x03,0x80] },
    { des:1, err:/unexpected {value_flag}/,    fq:0, na:1, b:[0x80] },
    { des:1, err:/unexpected {value_flag}/,    fq:1,       b:[0x03,0xFF] },
    { des:1, err:/unexpected {value_flag}/,    fq:0, na:1, b:[0xFF] },

    { des:1, err:/unexpected {length} length/, fq:1,       b:[0x03,0x00] },
    { des:1, err:/unexpected {length} length/,             b:[0x11] },
    { des:1, err:/unexpected {length} length/,             b:[0x11,0x22,0x33] },
    { des:1, err:/{length} is less than zero/,             b:[0xFF,0xFF,0xFF,0xFF] },
    { des:1, err:/{length} is less than zero/,             b:[0x80,0x00,0x00,0x00] },

    { des:1, err:/unexpected {text_value} length/,         b:[0x00,0x00,0x00,0x01] },
    { des:1, err:/unexpected {text_value} length/,         b:[0x00,0x00,0x00,0x02, 0x41] },
    { des:1, err:/unexpected {text_value} length/,         b:[0x00,0x00,0x00,0x03, 0x41,0x42] },
  ];

  describe('serialize', () =>
    cases.forEach(({ des, v, fq, b }, i) => it(`should be able to handle case #${i}`, () => {
      // deserialize case only
      if (des)
        return; // keep it like passed test not to mess with case index

      b = from(b);

      // when fq is under control
      if (fq !== undefined) {
        assert.deepEqual( stringSerializer.serialize(v, fq), b );
        return;
      }

      // generic case
      assert.deepEqual( stringSerializer.serialize(v, true),  concat([type_code, value_flag, b]) );
      assert.deepEqual( stringSerializer.serialize(v, false), concat([                       b]) );
    }))
  );

  describe('deserialize', () =>
    cases.forEach(({ v, fq, na, b, av, err }, i) => it(`should be able to handle case #${i}`, () => {
      if (Array.isArray(b))
        b = from(b);

      // wrong binary
      if (err !== undefined) {
        if (fq !== undefined)
          assert.throws(() => stringSerializer.deserialize(b, fq, na), { message: err });
        else {
          assert.throws(() => stringSerializer.deserialize(concat([type_code, value_flag, b]), true),         { message: err });
          assert.throws(() => stringSerializer.deserialize(concat([           value_flag, b]), false, true),  { message: err });
          assert.throws(() => stringSerializer.deserialize(concat([                       b]), false, false), { message: err });
        }
        return;
      }

      if (av !== undefined)
        v = av;
      const len = b.length;

      // when fq is under control
      if (fq !== undefined) {
        assert.deepStrictEqual( stringSerializer.deserialize(from(b), fq, na), {v,len} );
        return;
      }

      // generic case
      assert.deepStrictEqual( stringSerializer.deserialize(concat([type_code, value_flag, b]), true,  false), {v,len:len+2} );
      assert.deepStrictEqual( stringSerializer.deserialize(concat([type_code, value_flag, b]), true,  true),  {v,len:len+2} );
      assert.deepStrictEqual( stringSerializer.deserialize(concat([           value_flag, b]), false, true),  {v,len:len+1} );
      assert.deepStrictEqual( stringSerializer.deserialize(concat([                       b]), false, false), {v,len:len+0} );
    }))
  );

  describe('canBeUsedFor', () =>
    [
      { v: 'some string',    e: true },
      { v: '',               e: true },
      { v: 'Z',              e: true },
      { v: 'Україна',        e: true },
      { v: true,             e: false },
      { v: false,            e: false },
      { v: null,             e: false },
      { v: undefined,        e: false },
      { v: Number.MAX_VALUE, e: false },
      { v: 42,               e: false },
      { v: 0,                e: false },
      { v: Number.MIN_VALUE, e: false },
      { v: NaN,              e: false },
      { v: +Infinity,        e: false },
      { v: -Infinity,        e: false },
      //{ v: Symbol(''), e: false },
    ].forEach(({ v, e }, i) => it(`should return ${e} if value is '${v}', case #${i}`, () => assert.strictEqual(
      stringSerializer.canBeUsedFor(v),
      e,
    )))
  );

});
