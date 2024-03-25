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

import { ser_title, des_title, cbuf_title } from './utils.js';
import assert from 'assert';
import { doubleSerializer } from '../../../lib/structure/io/binary/GraphBinary.js';

const { from, concat } = Buffer;

describe('GraphBinary.DoubleSerializer', () => {

  const type_code =  from([0x07]);
  const value_flag = from([0x00]);

  const cases = [
    { v:undefined,                            fq:1, b:[0x07,0x01],           av:null },
    { v:undefined,                            fq:0, b:[0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00], av:0 },
    { v:null,                                 fq:1, b:[0x07,0x01] },
    { v:null,                                 fq:0, b:[0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00], av:0 },

    { v:1,                                          b:[0x3F,0xF0,0x00,0x00,0x00,0x00,0x00,0x00] },
    { v:0.1,                                        b:[0x3F,0xB9,0x99,0x99,0x99,0x99,0x99,0x9A] },
    { v:0.375,                                      b:[0x3F,0xD8,0x00,0x00,0x00,0x00,0x00,0x00] },
    { v:0.00390625,                                 b:[0x3F,0x70,0x00,0x00,0x00,0x00,0x00,0x00] },
    { v:Infinity,                                   b:[0x7F,0xF0,0x00,0x00,0x00,0x00,0x00,0x00] },
    { v:-Infinity,                                  b:[0xFF,0xF0,0x00,0x00,0x00,0x00,0x00,0x00] },
    { v:NaN,                                        b:[0x7F,0xF8,0x00,0x00,0x00,0x00,0x00,0x00] },

    { des:1, err:/buffer is missing/,         fq:1, b:undefined },
    { des:1, err:/buffer is missing/,         fq:0, b:undefined },
    { des:1, err:/buffer is missing/,         fq:1, b:null },
    { des:1, err:/buffer is missing/,         fq:0, b:null },
    { des:1, err:/buffer is empty/,           fq:1, b:[] },
    { des:1, err:/buffer is empty/,           fq:0, b:[] },

    { des:1, err:/unexpected {type_code}/,    fq:1, b:[0x00] },
    { des:1, err:/unexpected {type_code}/,    fq:1, b:[0x06] },
    { des:1, err:/unexpected {type_code}/,    fq:1, b:[0x08] },
    { des:1, err:/unexpected {type_code}/,    fq:1, b:[0x70] },
    { des:1, err:/unexpected {type_code}/,    fq:1, b:[0x77] },
    { des:1, err:/unexpected {type_code}/,    fq:1, b:[0xFF] },

    { des:1, err:/{value_flag} is missing/,   fq:1, b:[0x07] },
    { des:1, err:/unexpected {value_flag}/,   fq:1, b:[0x07,0x10] },
    { des:1, err:/unexpected {value_flag}/,   fq:1, b:[0x07,0x02] },
    { des:1, err:/unexpected {value_flag}/,   fq:1, b:[0x07,0x0F] },
    { des:1, err:/unexpected {value_flag}/,   fq:1, b:[0x07,0xFF] },

    { des:1, err:/unexpected {value} length/, fq:1, b:[0x07,0x00] },
    { des:1, err:/unexpected {value} length/,       b:[0x11] },
    { des:1, err:/unexpected {value} length/,       b:[0x11,0x22,0x33,0x44,0x55,0x66,0x77] },
  ];

  describe('#serialize', () =>
    cases
    .filter(({des}) => !des)
    .forEach(({ v, fq, b }, i) => it(ser_title({i,v}), () => {
      b = from(b);

      // when fq is under control
      if (fq !== undefined) {
        assert.deepEqual( doubleSerializer.serialize(v, fq), b );
        return;
      }

      // generic case
      assert.deepEqual( doubleSerializer.serialize(v, true),  concat([type_code, value_flag, b]) );
      assert.deepEqual( doubleSerializer.serialize(v, false), concat([                       b]) );
    }))
  );

  describe('#deserialize', () =>
    cases.forEach(({ v, fq, b, av, err }, i) => it(des_title({i,b}), () => {
      if (Array.isArray(b))
        b = from(b);

      // wrong binary
      if (err !== undefined) {
        if (fq !== undefined)
          assert.throws(() => doubleSerializer.deserialize(b, fq), { message: err });
        else {
          assert.throws(() => doubleSerializer.deserialize(concat([type_code, value_flag, b]), true),  { message: err });
          assert.throws(() => doubleSerializer.deserialize(concat([                       b]), false), { message: err });
        }
        return;
      }

      if (av !== undefined)
        v = av;
      const len = b.length;

      // when fq is under control
      if (fq !== undefined) {
        assert.deepStrictEqual( doubleSerializer.deserialize(b, fq), {v,len} );
        return;
      }

      // generic case
      assert.deepStrictEqual( doubleSerializer.deserialize(concat([type_code, value_flag, b]), true),  {v,len:len+2} );
      assert.deepStrictEqual( doubleSerializer.deserialize(concat([                       b]), false), {v,len:len+0} );
    }))
  );

  describe('#canBeUsedFor', () =>
    // most of the cases are implicitly tested via AnySerializer.serialize() tests
    [
      { v: null,              e: false },
      { v: undefined,         e: false },
      { v: {},                e: false },
      { v: [],                e: false },
      { v: [0],               e: false },
      { v: [undefined],       e: false },
      { v: 0n,                e: false },
      { v: BigInt(1),         e: false },
      { v: 0,                 e: false },
      { v: 1,                 e: false },
      { v: -1,                e: false },
      { v: 2147483647,        e: false },
      { v: 2147483648,        e: false },
      { v: -2147483648,       e: false },
      { v: -2147483649,       e: false },
      { v: 0.1,               e: true  },
      { v: Infinity,          e: true  },
      { v: -Infinity,         e: true  },
      { v: NaN,               e: true  },
    ].forEach(({ v, e }, i) => it(cbuf_title({i,v}), () =>
      assert.strictEqual( doubleSerializer.canBeUsedFor(v), e )
    ))
  );

});
