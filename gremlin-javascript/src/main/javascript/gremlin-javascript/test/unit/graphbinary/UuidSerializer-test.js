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

import { ser_title, des_title } from './utils.js';
import assert from 'assert';
import { uuidSerializer } from '../../../lib/structure/io/binary/GraphBinary.js';

const { from, concat } = Buffer;

describe('GraphBinary.UuidSerializer', () => {

  const type_code =  from([0x0C]);
  const value_flag = from([0x00]);

  const cases = [
    {        v:undefined,                     fq:1,       b:[0x0C,0x01],                                                                          av:null },
    {        v:undefined,                     fq:0,       b:[0x00,0x00,0x00,0x00, 0x00,0x00,0x00,0x00, 0x00,0x00,0x00,0x00, 0x00,0x00,0x00,0x00], av:'00000000-0000-0000-0000-000000000000' },
    {        v:null,                          fq:1,       b:[0x0C,0x01] },
    {        v:null,                          fq:0,       b:[0x00,0x00,0x00,0x00, 0x00,0x00,0x00,0x00, 0x00,0x00,0x00,0x00, 0x00,0x00,0x00,0x00], av:'00000000-0000-0000-0000-000000000000' },

    { v:'00000000-0000-0000-0000-000000000000',           b:[0x00,0x00,0x00,0x00, 0x00,0x00,0x00,0x00, 0x00,0x00,0x00,0x00, 0x00,0x00,0x00,0x00] },
    { v:'00010203-0405-0607-0809-0A0B0C0D0E0F',           b:[0x00,0x01,0x02,0x03, 0x04,0x05,0x06,0x07, 0x08,0x09,0x0A,0x0B, 0x0C,0x0D,0x0E,0x0F] },
    { v:'AaBbCcDd-EeFf-1122-3344-556677889900',           b:[0xAA,0xBB,0xCC,0xDD, 0xEE,0xFF,0x11,0x22, 0x33,0x44,0x55,0x66, 0x77,0x88,0x99,0x00] },

    // string presentation formats
    { v:'000102030405060708090A0B0C0D0E0F',               b:[0x00,0x01,0x02,0x03, 0x04,0x05,0x06,0x07, 0x08,0x09,0x0A,0x0B, 0x0C,0x0D,0x0E,0x0F], av:'00010203-0405-0607-0809-0A0B0C0D0E0F' },
    { v:'00010203-0405-0607-0809-0A0B0C0D0E0F',           b:[0x00,0x01,0x02,0x03, 0x04,0x05,0x06,0x07, 0x08,0x09,0x0A,0x0B, 0x0C,0x0D,0x0E,0x0F] },
    { v:'{00010203-0405-0607-0809-0A0B0C0D0E0F}',         b:[0x00,0x01,0x02,0x03, 0x04,0x05,0x06,0x07, 0x08,0x09,0x0A,0x0B, 0x0C,0x0D,0x0E,0x0F], av:'00010203-0405-0607-0809-0A0B0C0D0E0F' },
    { v:'urn:uuid:00010203-0405-0607-0809-0A0B0C0D0E0F',  b:[0x00,0x01,0x02,0x03, 0x04,0x05,0x06,0x07, 0x08,0x09,0x0A,0x0B, 0x0C,0x0D,0x0E,0x0F], av:'00010203-0405-0607-0809-0A0B0C0D0E0F' },

    // wrong string presentation
    { v:'AaBbCcDd-EeFf-1122-3344-556677889900FFFF',       b:[0xAA,0xBB,0xCC,0xDD, 0xEE,0xFF,0x11,0x22, 0x33,0x44,0x55,0x66, 0x77,0x88,0x99,0x00], av:'AaBbCcDd-EeFf-1122-3344-556677889900' },
    { v:'GHIJKLMN-OPQR-STUV-WXYZ-!@#$%^&*()_+',           b:[0x00,0x00,0x00,0x00, 0x00,0x00,0x00,0x00, 0x00,0x00,0x00,0x00, 0x00,0x00,0x00,0x00], av:'00000000-0000-0000-0000-000000000000' },

    { des:1, err:/buffer is missing/,         fq:1,       b:undefined },
    { des:1, err:/buffer is missing/,         fq:0,       b:undefined },
    { des:1, err:/buffer is missing/,         fq:1,       b:null },
    { des:1, err:/buffer is missing/,         fq:0,       b:null },
    { des:1, err:/buffer is empty/,           fq:1,       b:[] },
    { des:1, err:/buffer is empty/,           fq:0,       b:[] },
    { des:1, err:/buffer is empty/,           fq:0, na:1, b:[] },

    { des:1, err:/unexpected {type_code}/,    fq:1,       b:[0x01] },
    { des:1, err:/unexpected {type_code}/,    fq:1,       b:[0x00] },
    { des:1, err:/unexpected {type_code}/,    fq:1,       b:[0x0D] },
    { des:1, err:/unexpected {type_code}/,    fq:1,       b:[0x0B] },
    { des:1, err:/unexpected {type_code}/,    fq:1,       b:[0x8C] },
    { des:1, err:/unexpected {type_code}/,    fq:1,       b:[0xFC] },

    { des:1, err:/{value_flag} is missing/,   fq:1,       b:[0x0C] },
    { des:1, err:/unexpected {value_flag}/,   fq:1,       b:[0x0C,0x10] },
    { des:1, err:/unexpected {value_flag}/,   fq:0, na:1, b:[0x10] },
    { des:1, err:/unexpected {value_flag}/,   fq:1,       b:[0x0C,0x02] },
    { des:1, err:/unexpected {value_flag}/,   fq:0, na:1, b:[0x02] },
    { des:1, err:/unexpected {value_flag}/,   fq:1,       b:[0x0C,0x0F] },
    { des:1, err:/unexpected {value_flag}/,   fq:0, na:1, b:[0x0F] },
    { des:1, err:/unexpected {value_flag}/,   fq:1,       b:[0x0C,0xFF] },
    { des:1, err:/unexpected {value_flag}/,   fq:0, na:1, b:[0xFF] },

    { des:1, err:/unexpected {value} length/, fq:1,       b:[0x0C,0x00] },
    { des:1, err:/unexpected {value} length/, fq:0, na:1, b:[0x00] },
    { des:1, err:/unexpected {value} length/,             b:[0x00] },
    { des:1, err:/unexpected {value} length/,             b:[0x00,0x01,0x02,0x03,0x04,0x05,0x06,0x07,0x08,0x09,0x0A,0x0B,0x0C,0x0D,0x0E] },

    { des:1, v:null,                          fq:0, na:1, b:[0x01] },
  ];

  describe('#serialize', () =>
    cases
    .filter(({des}) => !des)
    .forEach(({ v, fq, b }, i) => it(ser_title({i,v}), () => {
      b = from(b);

      // when fq is under control
      if (fq !== undefined) {
        assert.deepEqual( uuidSerializer.serialize(v, fq), b );
        return;
      }

      // generic case
      assert.deepEqual( uuidSerializer.serialize(v, true),  concat([type_code, value_flag, b]) );
      assert.deepEqual( uuidSerializer.serialize(v, false), concat([                       b]) );
    }))
  );

  describe('#deserialize', () =>
    cases.forEach(({ v, fq, na, b, av, err }, i) => it(des_title({i,b}), () => {
      if (Array.isArray(b))
        b = from(b);

      // wrong binary
      if (err !== undefined) {
        if (fq !== undefined)
          assert.throws(() => uuidSerializer.deserialize(b, fq, na), { message: err });
        else {
          assert.throws(() => uuidSerializer.deserialize(concat([type_code, value_flag, b]), true),         { message: err });
          assert.throws(() => uuidSerializer.deserialize(concat([           value_flag, b]), false, true),  { message: err });
          assert.throws(() => uuidSerializer.deserialize(concat([                       b]), false, false), { message: err });
        }
        return;
      }

      if (av !== undefined)
        v = av;
      if (typeof v === 'string')
        v = v.toLowerCase();
      const len = b.length;

      // when fq is under control
      if (fq !== undefined) {
        assert.deepStrictEqual( uuidSerializer.deserialize(b, fq, na), {v,len} );
        return;
      }

      // generic case
      assert.deepStrictEqual( uuidSerializer.deserialize(concat([type_code, value_flag, b]), true,  false), {v,len:len+2} );
      assert.deepStrictEqual( uuidSerializer.deserialize(concat([type_code, value_flag, b]), true,  true),  {v,len:len+2} );
      assert.deepStrictEqual( uuidSerializer.deserialize(concat([           value_flag, b]), false, true),  {v,len:len+1} );
      assert.deepStrictEqual( uuidSerializer.deserialize(concat([                       b]), false, false), {v,len:len+0} );
    }))
  );

  describe('#canBeUsedFor', () =>
    it.skip('')
  );

});
