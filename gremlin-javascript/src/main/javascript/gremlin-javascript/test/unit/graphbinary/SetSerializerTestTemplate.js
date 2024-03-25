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

import { des_title, cbuf_title } from './utils.js';
import assert from 'assert';
import { serializers } from '../../../lib/structure/io/binary/GraphBinary.js';
import { Traverser } from '../../../lib/process/traversal.js';

const { from, concat } = Buffer;

export default ({ ID, name }) => {

describe(`GraphBinary.${name}Serializer`, () => {

  const type_code =  from([ID]);
  const value_flag = from([0x00]);

  const serializer = serializers[ID];

  const cases = [
    { v:undefined,                          fq:1, b:[ID,0x01],             av:null },
    { v:undefined,                          fq:0, b:[0x00,0x00,0x00,0x00], av:new Set() },
    { v:null,                               fq:1, b:[ID,0x01] },
    { v:null,                               fq:0, b:[0x00,0x00,0x00,0x00], av:new Set() },

    { v:new Set(),                              b:[0x00,0x00,0x00,0x00] },
    { v:new Set([2147483647]),                    b:[0x00,0x00,0x00,0x01, 0x01,0x00,0x7F,0xFF,0xFF,0xFF] },
    { v:new Set([-1,'A']),                        b:[0x00,0x00,0x00,0x02, 0x01,0x00,0xFF,0xFF,0xFF,0xFF, 0x03,0x00,0x00,0x00,0x00,0x01,0x41] },

    { des:1, err:/buffer is missing/,       fq:1, b:undefined },
    { des:1, err:/buffer is missing/,       fq:0, b:undefined },
    { des:1, err:/buffer is missing/,       fq:1, b:null },
    { des:1, err:/buffer is missing/,       fq:0, b:null },
    { des:1, err:/buffer is empty/,         fq:1, b:[] },
    { des:1, err:/buffer is empty/,         fq:0, b:[] },

    { des:1, err:/unexpected {type_code}/,  fq:1, b:[0x00] },
    { des:1, err:/unexpected {type_code}/,  fq:1, b:[0x01] },
    { des:1, err:/unexpected {type_code}/,  fq:1, b:[ID-1] },
    { des:1, err:/unexpected {type_code}/,  fq:1, b:[ID+1] },
    { des:1, err:/unexpected {type_code}/,  fq:1, b:[0x89] },
    { des:1, err:/unexpected {type_code}/,  fq:1, b:[0x19] },
    { des:1, err:/unexpected {type_code}/,  fq:1, b:[0x90] },

    { des:1, err:/{value_flag} is missing/, fq:1, b:[ID] },
    { des:1, err:/unexpected {value_flag}/, fq:1, b:[ID,0x10] },
    { des:1, err:/unexpected {value_flag}/, fq:1, b:[ID,0x02] },
    { des:1, err:/unexpected {value_flag}/, fq:1, b:[ID,0x0F] },
    { des:1, err:/unexpected {value_flag}/, fq:1, b:[ID,0xFF] },

    { des:1, err:/{length} is less than zero/,    b:[0xFF,0xFF,0xFF,0xFF] },
    { des:1, err:/{length} is less than zero/,    b:[0x80,0x00,0x00,0x00] },
  ];

  describe('#deserialize', () =>
    cases.forEach(({ v, fq, b, av, err }, i) => it(des_title({i,b}), () => {
      if (Array.isArray(b))
        b = from(b);

      // wrong binary
      if (err !== undefined) {
        if (fq !== undefined)
          assert.throws(() => serializer.deserialize(b, fq), { message: err });
        else {
          assert.throws(() => serializer.deserialize(concat([type_code, value_flag, b]), true),  { message: err });
          assert.throws(() => serializer.deserialize(concat([                       b]), false), { message: err });
        }
        return;
      }

      if (av !== undefined)
        v = av;
      const len = b.length;

      // when fq is under control
      if (fq !== undefined) {
        assert.deepStrictEqual( serializer.deserialize(b, fq), {v,len} );
        return;
      }

      // generic case
      assert.deepStrictEqual( serializer.deserialize(concat([type_code, value_flag, b]), true),  {v,len:len+2} );
      assert.deepStrictEqual( serializer.deserialize(concat([                       b]), false), {v,len:len+0} );
    }))
  );

  describe('#canBeUsedFor', () =>
    // most of the cases are implicitly tested via AnySerializer.serialize() tests
    [
      { v: null,                  e: false },
      { v: undefined,             e: false },
      { v: {},                    e: false },
      { v: new Traverser(),     e: false },
      { v: [],                    e: false },
      { v: [0],                   e: false },
      { v: [undefined],           e: false },
      { v: new Set(),             e: true },
      { v: new Set([0]),          e: true },
      { v: new Set([undefined]),  e: true },
    ].forEach(({ v, e }, i) => it(cbuf_title({i,v}), () =>
      assert.strictEqual( serializer.canBeUsedFor(v), e )
    ))
  );

});

};
