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

'use strict';

const utils = require('./utils');
const assert = require('assert');
const ioc = require('../../../lib/structure/io/binary/GraphBinary');

const { from, concat } = Buffer;

const ID = 0x88; // OFFSETDATETIME

describe('GraphBinary.OffsetDateTimeSerializer', () => {

  const type_code = from([ID]);
  const value_flag = from([0x00]);

  const serializer = ioc.serializers[ID];

  const cases = [
    { v: undefined, fq: 1, b: [ID, 0x01], av: null },
    { v: null,      fq: 1, b: [ID, 0x01] },

    // year=2022(0x07e6), month=5, day=1, ns=0x000042c277bd8e00, offset=0
    { v: new Date(1651436603000),
      b: [0x00,0x00,0x07,0xe6, 0x05, 0x01, 0x00,0x00,0x42,0xc2,0x77,0xbd,0x8e,0x00, 0x00,0x00,0x00,0x00] },

    { des: 1, err: /buffer is missing/,         fq: 1, b: undefined },
    { des: 1, err: /buffer is empty/,           fq: 1, b: [] },
    { des: 1, err: /unexpected {type_code}/,    fq: 1, b: [ID - 1] },
    { des: 1, err: /{value_flag} is missing/,   fq: 1, b: [ID] },
    { des: 1, err: /unexpected {value_flag}/,   fq: 1, b: [ID, 0x02] },
    { des: 1, err: /unexpected {value} length/, fq: 1, b: [ID, 0x00] },

    // Boundary values that fall outside the JavaScript Date range (year=999999999=0x3B9AC9FF)
    // must be rejected rather than silently deserialized into an invalid Date. See TINKERPOP-3276.
    { des: 1, err: /outside the range supported by JavaScript Date/, fq: 0,
      b: [0x3B,0x9A,0xC9,0xFF, 0x01, 0x01, 0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00, 0x00,0x00,0x00,0x00] },
    // year=-999999999=0xC4653601
    { des: 1, err: /outside the range supported by JavaScript Date/, fq: 0,
      b: [0xC4,0x65,0x36,0x01, 0x01, 0x01, 0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00, 0x00,0x00,0x00,0x00] },
  ];

  describe('#serialize', () =>
    cases
    .filter(({ des }) => !des)
    .forEach(({ v, fq, b }, i) => it(utils.ser_title({ i, v }), () => {
      b = from(b);

      if (fq !== undefined) {
        assert.deepEqual(serializer.serialize(v, fq), b);
        return;
      }

      assert.deepEqual(serializer.serialize(v, true),  concat([type_code, value_flag, b]));
      assert.deepEqual(serializer.serialize(v, false), concat([b]));
    }))
  );

  describe('#deserialize', () =>
    cases.forEach(({ v, fq, b, av, err }, i) => it(utils.des_title({ i, b }), () => {
      if (Array.isArray(b))
        b = from(b);

      if (err !== undefined) {
        if (fq !== undefined)
          assert.throws(() => serializer.deserialize(b, fq), { message: err });
        else {
          assert.throws(() => serializer.deserialize(concat([type_code, value_flag, b]), true),  { message: err });
          assert.throws(() => serializer.deserialize(concat([b]), false), { message: err });
        }
        return;
      }

      if (av !== undefined)
        v = av;
      const len = b.length;

      if (fq !== undefined) {
        assert.deepStrictEqual(serializer.deserialize(b, fq), { v, len });
        return;
      }

      assert.deepStrictEqual(serializer.deserialize(concat([type_code, value_flag, b]), true),  { v, len: len + 2 });
      assert.deepStrictEqual(serializer.deserialize(concat([b]), false), { v, len: len + 0 });
    }))
  );

  describe('#canBeUsedFor', () =>
    [
      { v: null,       e: false },
      { v: undefined,  e: false },
      { v: {},         e: false },
      { v: [],         e: false },
      { v: new Date(), e: true  },
    ].forEach(({ v, e }, i) => it(utils.cbuf_title({ i, v }), () =>
      assert.strictEqual(serializer.canBeUsedFor(v), e)
    ))
  );

});
