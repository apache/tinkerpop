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
const { listSerializer, intSerializer } = require('../../../lib/structure/io/binary/GraphBinary');
const t = require('../../../lib/process/traversal');

const { from, concat } = Buffer;

describe('GraphBinary.ListSerializer', () => {

  const type_code =  from([0x09]);
  const value_flag = from([0x00]);

  const cases = [
    { v:undefined,                                     fq:1, b:[0x09, 0x01],          av:null },
    { v:undefined,                                     fq:0, b:[0x00,0x00,0x00,0x00], av:[] },
    { v:null,                                          fq:1, b:[0x09, 0x01] },
    { v:null,                                          fq:0, b:[0x00,0x00,0x00,0x00], av:[] },

    { v:[],                                                  b:[0x00,0x00,0x00,0x00] },
    { v:[2147483647],                                        b:[0x00,0x00,0x00,0x01, 0x01,0x00,0x7F,0xFF,0xFF,0xFF] },
    { v:[-1,'A'],                                            b:[0x00,0x00,0x00,0x02, 0x01,0x00,0xFF,0xFF,0xFF,0xFF, 0x03,0x00,0x00,0x00,0x00,0x01,0x41] },

    { des:1, err:/buffer is missing/,                  fq:1, b:undefined },
    { des:1, err:/buffer is missing/,                  fq:0, b:undefined },
    { des:1, err:/buffer is missing/,                  fq:1, b:null },
    { des:1, err:/buffer is missing/,                  fq:0, b:null },
    { des:1, err:/buffer is empty/,                    fq:1, b:[] },
    { des:1, err:/buffer is empty/,                    fq:0, b:[] },

    { des:1, err:/unexpected {type_code}/,             fq:1, b:[0x00] },
    { des:1, err:/unexpected {type_code}/,             fq:1, b:[0x01] },
    { des:1, err:/unexpected {type_code}/,             fq:1, b:[0x08] },
    { des:1, err:/unexpected {type_code}/,             fq:1, b:[0x0A] },
    { des:1, err:/unexpected {type_code}/,             fq:1, b:[0x89] },
    { des:1, err:/unexpected {type_code}/,             fq:1, b:[0x19] },
    { des:1, err:/unexpected {type_code}/,             fq:1, b:[0x90] },

    { des:1, err:/{value_flag} is missing/,            fq:1, b:[0x09] },
    { des:1, err:/unexpected {value_flag}/,            fq:1, b:[0x09,0x10] },
    { des:1, err:/unexpected {value_flag}/,            fq:1, b:[0x09,0x02] },
    { des:1, err:/unexpected {value_flag}/,            fq:1, b:[0x09,0x0F] },
    { des:1, err:/unexpected {value_flag}/,            fq:1, b:[0x09,0xFF] },

    { des:1, err:/{length} is less than zero/,               b:[0xFF,0xFF,0xFF,0xFF] },
    { des:1, err:/{length} is less than zero/,               b:[0x80,0x00,0x00,0x00] },
  ];

  describe('serialize', () => {
    cases.forEach(({ des, v, fq, b }, i) => it(`should be able to handle case #${i}`, () => {
      // deserialize case only
      if (des)
        return; // keep it like passed test not to mess with case index

      b = from(b);

      // when fq is under control
      if (fq !== undefined) {
        assert.deepEqual( listSerializer.serialize(v, fq), b );
        return;
      }

      // generic case
      assert.deepEqual( listSerializer.serialize(v, true),  concat([type_code, value_flag, b]) );
      assert.deepEqual( listSerializer.serialize(v, false), concat([                       b]) );
    }));

    it.skip('should not error if array length is INT32_MAX'); // TODO: resource heavy

    it('should error if array length is greater than INT32_MAX', () => assert.throws(
      () => listSerializer.serialize(new Array(intSerializer.INT32_MAX+1)),
      { message: new RegExp(`List length=${intSerializer.INT32_MAX+1} is greater than supported max_length=${intSerializer.INT32_MAX}`) }
    ));
  });

  describe('deserialize', () =>
    cases.forEach(({ v, fq, b, av, err }, i) => it(`should be able to handle case #${i}`, () => {
      if (Array.isArray(b))
        b = from(b);

      // wrong binary
      if (err !== undefined) {
        if (fq !== undefined)
          assert.throws(() => listSerializer.deserialize(b, fq), { message: err });
        else {
          assert.throws(() => listSerializer.deserialize(concat([type_code, value_flag, b]), true),  { message: err });
          assert.throws(() => listSerializer.deserialize(concat([                       b]), false), { message: err });
        }
        return;
      }

      if (av !== undefined)
        v = av;
      const len = b.length;

      // when fq is under control
      if (fq !== undefined) {
        assert.deepStrictEqual( listSerializer.deserialize(b, fq), {v,len} );
        return;
      }

      // generic case
      assert.deepStrictEqual( listSerializer.deserialize(concat([type_code, value_flag, b]), true),  {v,len:len+2} );
      assert.deepStrictEqual( listSerializer.deserialize(concat([                       b]), false), {v,len:len+0} );
    }))
  );

  describe('canBeUsedFor', () =>
    // most of the cases are implicitly tested via AnySerializer.serialize() tests
    [
      { v: null,              e: false },
      { v: undefined,         e: false },
      { v: {},                e: false },
      { v: new t.Traverser(), e: false },
      { v: [],                e: true },
      { v: [0],               e: true },
      { v: [undefined],       e: true },
    ].forEach(({ v, e }, i) => it(`should be able to handle case #${i}`, () =>
      assert.strictEqual( listSerializer.canBeUsedFor(v), e )
    ))
  );

});
