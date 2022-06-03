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

const utils = require('./utils');
const assert = require('assert');
const { shortSerializer } = require('../../../lib/structure/io/binary/GraphBinary');

const { from, concat } = Buffer;

describe('GraphBinary.ShortSerializer', () => {

  const type_code =  from([0x26]);
  const value_flag = from([0x00]);

  const cases = [
    { v:undefined,                            fq:1, b:[0x26,0x01], av:null },
    { v:undefined,                            fq:0, b:[0x00,0x00], av:0 },
    { v:null,                                 fq:1, b:[0x26,0x01] },
    { v:null,                                 fq:0, b:[0x00,0x00], av:0 },
    { v:0,                                          b:[0x00,0x00] },
    { v:1,                                          b:[0x00,0x01] },
    { v:256,                                        b:[0x01,0x00] },
    { v:32767,                                      b:[0x7F,0xFF] },
    { v:-1,                                         b:[0xFF,0xFF] },
    { v:-32768,                                     b:[0x80,0x00] },

    { des:1, err:/buffer is missing/,         fq:1, b:undefined },
    { des:1, err:/buffer is missing/,         fq:0, b:undefined },
    { des:1, err:/buffer is missing/,         fq:1, b:null },
    { des:1, err:/buffer is missing/,         fq:0, b:null },
    { des:1, err:/buffer is empty/,           fq:1, b:[] },
    { des:1, err:/buffer is empty/,           fq:0, b:[] },

    { des:1, err:/unexpected {type_code}/,    fq:1, b:[0x00] },
    { des:1, err:/unexpected {type_code}/,    fq:1, b:[0x02] },
    { des:1, err:/unexpected {type_code}/,    fq:1, b:[0x62] },
    { des:1, err:/unexpected {type_code}/,    fq:1, b:[0x25] },
    { des:1, err:/unexpected {type_code}/,    fq:1, b:[0x27] },
    { des:1, err:/unexpected {type_code}/,    fq:1, b:[0xFF] },

    { des:1, err:/{value_flag} is missing/,   fq:1, b:[0x26] },
    { des:1, err:/unexpected {value_flag}/,   fq:1, b:[0x26,0x10] },
    { des:1, err:/unexpected {value_flag}/,   fq:1, b:[0x26,0x02] },
    { des:1, err:/unexpected {value_flag}/,   fq:1, b:[0x26,0x0F] },
    { des:1, err:/unexpected {value_flag}/,   fq:1, b:[0x26,0xFF] },

    { des:1, err:/unexpected {value} length/, fq:1, b:[0x26,0x00] },
    { des:1, err:/unexpected {value} length/,       b:[0x11] },
  ];

  describe('#serialize', () =>
    cases
    .filter(({des}) => !des)
    .forEach(({ v, fq, b }, i) => it(utils.ser_title({i,v}), () => {
      b = from(b);

      // when fq is under control
      if (fq !== undefined) {
        assert.deepEqual( shortSerializer.serialize(v, fq), b );
        return;
      }

      // generic case
      assert.deepEqual( shortSerializer.serialize(v, true),  concat([type_code, value_flag, b]) );
      assert.deepEqual( shortSerializer.serialize(v, false), concat([                       b]) );
    }))
  );

  describe('#deserialize', () =>
    cases.forEach(({ v, fq, b, av, err }, i) => it(utils.des_title({i,b}), () => {
      if (Array.isArray(b))
        b = from(b);

      // wrong binary
      if (err !== undefined) {
        if (fq !== undefined)
          assert.throws(() => shortSerializer.deserialize(b, fq), { message: err });
        else {
          assert.throws(() => shortSerializer.deserialize(concat([type_code, value_flag, b]), true),  { message: err });
          assert.throws(() => shortSerializer.deserialize(concat([                       b]), false), { message: err });
        }
        return;
      }

      if (av !== undefined)
        v = av;
      const len = b.length;

      // when fq is under control
      if (fq !== undefined) {
        assert.deepStrictEqual( shortSerializer.deserialize(b, fq), {v,len} );
        return;
      }

      // generic case
      assert.deepStrictEqual( shortSerializer.deserialize(concat([type_code, value_flag, b]), true),  {v,len:len+2} );
      assert.deepStrictEqual( shortSerializer.deserialize(concat([                       b]), false), {v,len:len+0} );
    }))
  );

});
