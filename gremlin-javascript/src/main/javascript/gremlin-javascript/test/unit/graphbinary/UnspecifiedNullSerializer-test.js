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
const { unspecifiedNullSerializer } = require('../../../lib/structure/io/binary/GraphBinary');

const { from, concat } = Buffer;

describe('GraphBinary.UnspecifiedNullSerializer', () => {

  const cases = [
    { v:undefined,                          b:[0xFE,0x01], av:null },
    { v:null,                               b:[0xFE,0x01] },

    { des:1, err:/buffer is missing/,       b:undefined },
    { des:1, err:/buffer is missing/,       b:null },
    { des:1, err:/buffer is empty/,         b:[] },

    { des:1, err:/unexpected {type_code}/,  b:[0x00] },
    { des:1, err:/unexpected {type_code}/,  b:[0xEF] },
    { des:1, err:/unexpected {type_code}/,  b:[0x0E] },
    { des:1, err:/unexpected {type_code}/,  b:[0x81] },
    { des:1, err:/unexpected {type_code}/,  b:[0xFD] },
    { des:1, err:/unexpected {type_code}/,  b:[0xFF] },

    { des:1, err:/{value_flag} is missing/, b:[0xFE] },
    { des:1, err:/unexpected {value_flag}/, b:[0xFE,0x10] },
    { des:1, err:/unexpected {value_flag}/, b:[0xFE,0x00] },
    { des:1, err:/unexpected {value_flag}/, b:[0xFE,0x02] },
    { des:1, err:/unexpected {value_flag}/, b:[0xFE,0x0F] },
    { des:1, err:/unexpected {value_flag}/, b:[0xFE,0xFF] },
  ];

  describe('serialize', () =>
    cases.forEach(({ des, v, b }, i) => it(`should be able to handle case #${i}`, () => {
      // deserialize case only
      if (des)
        return; // keep it like passed test not to mess with case index

      b = from(b);
      assert.deepEqual( unspecifiedNullSerializer.serialize(v), b );
    }))
  );

  describe('deserialize', () =>
    cases.forEach(({ v, b, av, err }, i) => it(`should be able to handle case #${i}`, () => {
      if (Array.isArray(b))
        b = from(b);

      // wrong binary
      if (err !== undefined) {
        assert.throws(() => unspecifiedNullSerializer.deserialize(b), { message: err });
        return;
      }

      if (av !== undefined)
        v = av;
      const len = b.length;

      // generic case
      assert.deepStrictEqual( unspecifiedNullSerializer.deserialize(b), {v,len} );
    }))
  );

  describe('canBeUsedFor', () =>
    it.skip('')
  );

});
