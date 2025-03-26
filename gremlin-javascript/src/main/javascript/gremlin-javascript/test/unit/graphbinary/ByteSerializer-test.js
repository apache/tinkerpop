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
import { byteSerializer } from '../../../lib/structure/io/binary/GraphBinary.js';

const { from, concat } = Buffer;

describe('GraphBinary.ByteSerializer', () => {

  const type_code =  from([0x24]);
  const value_flag = from([0x00]);

  const cases = [
    { v:undefined,                            fq:1, b:[0x24,0x01], av:null },
    { v:undefined,                            fq:0, b:[0x00],      av:0 },
    { v:null,                                 fq:1, b:[0x24,0x01] },
    { v:null,                                 fq:0, b:[0x00],      av:0 },
    { v:0,                                          b:[0x00] },
    { v:1,                                          b:[0x01] },
    { v:16,                                         b:[0x10] },

    { des:1, err:/buffer is missing/,         fq:1, b:undefined },
    { des:1, err:/buffer is missing/,         fq:0, b:undefined },
    { des:1, err:/buffer is missing/,         fq:1, b:null },
    { des:1, err:/buffer is missing/,         fq:0, b:null },
    { des:1, err:/buffer is empty/,           fq:1, b:[] },
    { des:1, err:/buffer is empty/,           fq:0, b:[] },

    { des:1, err:/unexpected {type_code}/,    fq:1, b:[0x00] },
    { des:1, err:/unexpected {type_code}/,    fq:1, b:[0x02] },
    { des:1, err:/unexpected {type_code}/,    fq:1, b:[0x23] },
    { des:1, err:/unexpected {type_code}/,    fq:1, b:[0x25] },
    { des:1, err:/unexpected {type_code}/,    fq:1, b:[0x42] },
    { des:1, err:/unexpected {type_code}/,    fq:1, b:[0xFF] },

    { des:1, err:/{value_flag} is missing/,   fq:1, b:[0x24] },
    { des:1, err:/unexpected {value_flag}/,   fq:1, b:[0x24,0x10] },
    { des:1, err:/unexpected {value_flag}/,   fq:1, b:[0x24,0x02] },
    { des:1, err:/unexpected {value_flag}/,   fq:1, b:[0x24,0x0F] },
    { des:1, err:/unexpected {value_flag}/,   fq:1, b:[0x24,0xFF] },

    { des:1, err:/unexpected {value} length/, fq:1, b:[0x24,0x00] },
  ];

  describe('#serialize', () =>
    cases
    .filter(({des}) => !des)
    .forEach(({ v, fq, b }, i) => it(ser_title({i,v}), () => {
      b = from(b);

      // when fq is under control
      if (fq !== undefined) {
        assert.deepEqual( byteSerializer.serialize(v, fq), b );
        return;
      }

      // generic case
      assert.deepEqual( byteSerializer.serialize(v, true),  concat([type_code, value_flag, b]) );
      assert.deepEqual( byteSerializer.serialize(v, false), concat([                       b]) );
    }))
  );

  describe('#deserialize', () =>
    cases.forEach(({ v, fq, b, av, err }, i) => it(des_title({i,b}), () => {
      if (Array.isArray(b))
        b = from(b);

      // wrong binary
      if (err !== undefined) {
        if (fq !== undefined)
          assert.throws(() => byteSerializer.deserialize(b, fq), { message: err });
        else {
          assert.throws(() => byteSerializer.deserialize(concat([type_code, value_flag, b]), true),  { message: err });
          assert.throws(() => byteSerializer.deserialize(concat([                       b]), false), { message: err });
        }
        return;
      }

      if (av !== undefined)
        v = av;
      const len = b.length;

      // when fq is under control
      if (fq !== undefined) {
        assert.deepStrictEqual( byteSerializer.deserialize(b, fq), {v,len} );
        return;
      }

      // generic case
      assert.deepStrictEqual( byteSerializer.deserialize(concat([type_code, value_flag, b]), true),  {v,len:len+2} );
      assert.deepStrictEqual( byteSerializer.deserialize(concat([                       b]), false), {v,len:len+0} );
    }))
  );

});
