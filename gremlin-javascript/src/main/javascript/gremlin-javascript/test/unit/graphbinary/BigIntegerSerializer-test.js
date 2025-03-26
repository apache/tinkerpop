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
import { bigIntegerSerializer } from '../../../lib/structure/io/binary/GraphBinary.js';

const { from, concat } = Buffer;

describe('GraphBinary.BigIntegerSerializer', () => {

  const type_code =  from([0x23]);
  const value_flag = from([0x00]);

  const cases = [
    { v:undefined,               fq:1, b:[0x23,0x01],                 av:null },
    { v:undefined,               fq:0, b:[0x00,0x00,0x00,0x01, 0x00], av:0n },
    { v:null,                    fq:1, b:[0x23,0x01] },
    { v:null,                    fq:0, b:[0x00,0x00,0x00,0x01, 0x00], av:0n },

    { v:0n,                            b:[0x00,0x00,0x00,0x01,                                                   0x00] },
    { v:1n,                            b:[0x00,0x00,0x00,0x01,                                                   0x01] },
    { v:127n,                          b:[0x00,0x00,0x00,0x01,                                                   0x7F] },
    { v:128n,                          b:[0x00,0x00,0x00,0x02,                                              0x00,0x80] },
    { v:160n,                          b:[0x00,0x00,0x00,0x02,                                              0x00,0xA0] },
    { v:32767n,                        b:[0x00,0x00,0x00,0x02,                                              0x7F,0xFF] },
    { v:32768n,                        b:[0x00,0x00,0x00,0x03,                                         0x00,0x80,0x00] },
    { v:8388607n,                      b:[0x00,0x00,0x00,0x03,                                         0x7F,0xFF,0xFF] },
    { v:8388608n,                      b:[0x00,0x00,0x00,0x04,                                    0x00,0x80,0x00,0x00] },
    { v:2147483647n,                   b:[0x00,0x00,0x00,0x04,                                    0x7F,0xFF,0xFF,0xFF] },
    { v:2147483648n,                   b:[0x00,0x00,0x00,0x05,                               0x00,0x80,0x00,0x00,0x00] },
    { v:549755813887n,                 b:[0x00,0x00,0x00,0x05,                               0x7F,0xFF,0xFF,0xFF,0xFF] },
    { v:549755813888n,                 b:[0x00,0x00,0x00,0x06,                          0x00,0x80,0x00,0x00,0x00,0x00] },
    { v:140737488355327n,              b:[0x00,0x00,0x00,0x06,                          0x7F,0xFF,0xFF,0xFF,0xFF,0xFF] },
    { v:140737488355328n,              b:[0x00,0x00,0x00,0x07,                     0x00,0x80,0x00,0x00,0x00,0x00,0x00] },
    { v:36028797018963967n,            b:[0x00,0x00,0x00,0x07,                     0x7F,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF] },
    { v:36028797018963968n,            b:[0x00,0x00,0x00,0x08,                0x00,0x80,0x00,0x00,0x00,0x00,0x00,0x00] },
    { v:9223372036854775807n,          b:[0x00,0x00,0x00,0x08,                0x7F,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF] },
    { v:9223372036854775808n,          b:[0x00,0x00,0x00,0x09,           0x00,0x80,0x00,0x00,0x00,0x00,0x00,0x00,0x00] },
    { v:2361183241434822606847n,       b:[0x00,0x00,0x00,0x09,           0x7F,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF] },
    { v:2361183241434822606848n,       b:[0x00,0x00,0x00,0x0A,      0x00,0x80,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00] },
    { v:604462909807314587353087n,     b:[0x00,0x00,0x00,0x0A,      0x7F,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF] },

    { v:-1n,                           b:[0x00,0x00,0x00,0x01,                                                   0xFF] },
    { v:-128n,                         b:[0x00,0x00,0x00,0x01,                                                   0x80] },
    { v:-129n,                         b:[0x00,0x00,0x00,0x02,                                              0xFF,0x7F] },
    { v:-1n, des:1,                    b:[0x00,0x00,0x00,0x02,                                              0xFF,0xFF] },
    { v:-32768n,                       b:[0x00,0x00,0x00,0x02,                                              0x80,0x00] },
    { v:-32769n,                       b:[0x00,0x00,0x00,0x03,                                         0xFF,0x7F,0xFF] },
    { v:-1n, des:1,                    b:[0x00,0x00,0x00,0x03,                                         0xFF,0xFF,0xFF] },
    { v:-8388608n,                     b:[0x00,0x00,0x00,0x03,                                         0x80,0x00,0x00] },
    { v:-8388609n,                     b:[0x00,0x00,0x00,0x04,                                    0xFF,0x7F,0xFF,0xFF] },
    { v:-1n, des:1,                    b:[0x00,0x00,0x00,0x04,                                    0xFF,0xFF,0xFF,0xFF] },
    { v:-2147483648n,                  b:[0x00,0x00,0x00,0x04,                                    0x80,0x00,0x00,0x00] },
    { v:-2147483649n,                  b:[0x00,0x00,0x00,0x05,                               0xFF,0x7F,0xFF,0xFF,0xFF] },
    { v:-1n, des:1,                    b:[0x00,0x00,0x00,0x05,                               0xFF,0xFF,0xFF,0xFF,0xFF] },
    { v:-549755813888n,                b:[0x00,0x00,0x00,0x05,                               0x80,0x00,0x00,0x00,0x00] },
    { v:-549755813889n,                b:[0x00,0x00,0x00,0x06,                          0xFF,0x7F,0xFF,0xFF,0xFF,0xFF] },
    { v:-1n, des:1,                    b:[0x00,0x00,0x00,0x06,                          0xFF,0xFF,0xFF,0xFF,0xFF,0xFF] },
    { v:-140737488355328n,             b:[0x00,0x00,0x00,0x06,                          0x80,0x00,0x00,0x00,0x00,0x00] },
    { v:-140737488355329n,             b:[0x00,0x00,0x00,0x07,                     0xFF,0x7F,0xFF,0xFF,0xFF,0xFF,0xFF] },
    { v:-1n, des:1,                    b:[0x00,0x00,0x00,0x07,                     0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF] },
    { v:-36028797018963968n,           b:[0x00,0x00,0x00,0x07,                     0x80,0x00,0x00,0x00,0x00,0x00,0x00] },
    { v:-36028797018963969n,           b:[0x00,0x00,0x00,0x08,                0xFF,0x7F,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF] },
    { v:-1n, des:1,                    b:[0x00,0x00,0x00,0x08,                0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF] },
    { v:-9223372036854775808n,         b:[0x00,0x00,0x00,0x08,                0x80,0x00,0x00,0x00,0x00,0x00,0x00,0x00] },
    { v:-9223372036854775809n,         b:[0x00,0x00,0x00,0x09,           0xFF,0x7F,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF] },
    { v:-1n, des:1,                    b:[0x00,0x00,0x00,0x09,           0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF] },
    { v:-2361183241434822606848n,      b:[0x00,0x00,0x00,0x09,           0x80,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00] },
    { v:-2361183241434822606849n,      b:[0x00,0x00,0x00,0x0A,      0xFF,0x7F,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF] },
    { v:-1n, des:1,                    b:[0x00,0x00,0x00,0x0A,      0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF] },
    { v:-604462909807314587353088n,    b:[0x00,0x00,0x00,0x0A,      0x80,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00] },
    { v:-604462909807314587353089n,    b:[0x00,0x00,0x00,0x0B, 0xFF,0x7F,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF] },

    { des:1, err:/buffer is missing/,         fq:1, b:undefined },
    { des:1, err:/buffer is missing/,         fq:0, b:undefined },
    { des:1, err:/buffer is missing/,         fq:1, b:null },
    { des:1, err:/buffer is missing/,         fq:0, b:null },
    { des:1, err:/buffer is empty/,           fq:1, b:[] },
    { des:1, err:/buffer is empty/,           fq:0, b:[] },

    { des:1, err:/unexpected {type_code}/,    fq:1, b:[0x00] },
    { des:1, err:/unexpected {type_code}/,    fq:1, b:[0x01] },
    { des:1, err:/unexpected {type_code}/,    fq:1, b:[0x03] },
    { des:1, err:/unexpected {type_code}/,    fq:1, b:[0x32] },
    { des:1, err:/unexpected {type_code}/,    fq:1, b:[0x22] },
    { des:1, err:/unexpected {type_code}/,    fq:1, b:[0x24] },
    { des:1, err:/unexpected {type_code}/,    fq:1, b:[0xFF] },

    { des:1, err:/{value_flag} is missing/,   fq:1, b:[0x23] },
    { des:1, err:/unexpected {value_flag}/,   fq:1, b:[0x23,0x10] },
    { des:1, err:/unexpected {value_flag}/,   fq:1, b:[0x23,0x02] },
    { des:1, err:/unexpected {value_flag}/,   fq:1, b:[0x23,0x0F] },
    { des:1, err:/unexpected {value_flag}/,   fq:1, b:[0x23,0xFF] },

    { des:1, err:/{length}=.* is less than one/,    b:[0x00,0x00,0x00,0x00] },
    { des:1, err:/{length}=.* is less than one/,    b:[0xFF,0xFF,0xFF,0xFF] },
    { des:1, err:/{length}=.* is less than one/,    b:[0xFF,0xFF,0xFF,0xFE] },
    { des:1, err:/{length}=.* is less than one/,    b:[0x80,0x00,0x00,0x00] },
  ];

  describe('#serialize', () =>
    cases
    .filter(({des}) => !des)
    .forEach(({ v, fq, b }, i) => it(ser_title({i,v}), () => {
      b = from(b);

      // when fq is under control
      if (fq !== undefined) {
        assert.deepEqual( bigIntegerSerializer.serialize(v, fq), b );
        return;
      }

      // generic case
      assert.deepEqual( bigIntegerSerializer.serialize(v, true),  concat([type_code, value_flag, b]) );
      assert.deepEqual( bigIntegerSerializer.serialize(v, false), concat([                       b]) );
    }))
  );

  describe('#deserialize', () =>
    cases.forEach(({ v, fq, b, av, err }, i) => it(des_title({i,b}), () => {
      if (Array.isArray(b))
        b = from(b);

      // wrong binary
      if (err !== undefined) {
        if (fq !== undefined)
          assert.throws(() => bigIntegerSerializer.deserialize(b, fq), { message: err });
        else {
          assert.throws(() => bigIntegerSerializer.deserialize(concat([type_code, value_flag, b]), true),  { message: err });
          assert.throws(() => bigIntegerSerializer.deserialize(concat([                       b]), false), { message: err });
        }
        return;
      }

      if (av !== undefined)
        v = av;
      const len = b.length;

      // when fq is under control
      if (fq !== undefined) {
        assert.deepStrictEqual( bigIntegerSerializer.deserialize(b, fq), {v,len} );
        return;
      }

      // generic case
      assert.deepStrictEqual( bigIntegerSerializer.deserialize(concat([type_code, value_flag, b]), true),  {v,len:len+2} );
      assert.deepStrictEqual( bigIntegerSerializer.deserialize(concat([                       b]), false), {v,len:len+0} );
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
      { v: 0n,                e: true  },
      { v: BigInt(1),         e: true  },
      { v: 0,                 e: false },
      { v: 1,                 e: false },
      { v: -1,                e: false },
      { v: 2147483647,        e: false },
      { v: 2147483648,        e: false },
      { v: -2147483648,       e: false },
      { v: -2147483649,       e: false },
      { v: 0.1,               e: false },
      { v: Infinity,          e: false },
      { v: -Infinity,         e: false },
      { v: NaN,               e: false },
    ].forEach(({ v, e }, i) => it(cbuf_title({i,v}), () =>
      assert.strictEqual( bigIntegerSerializer.canBeUsedFor(v), e )
    ))
  );

});
