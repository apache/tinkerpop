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
import { pSerializer } from '../../../lib/structure/io/binary/GraphBinary.js';
import { P, Traverser } from '../../../lib/process/traversal.js';

const { from, concat } = Buffer;

describe('GraphBinary.PSerializer', () => {

  const type_code =  from([0x1E]);
  const value_flag = from([0x00]);

  const cases = [
    { v:undefined, fq:1, b:[0x1E, 0x01],                               av:null },
    { v:undefined, fq:0, b:[0x00,0x00,0x00,0x00, 0x00,0x00,0x00,0x00], av:new P('') },
    { v:null,      fq:1, b:[0x1E, 0x01] },
    { v:null,      fq:0, b:[0x00,0x00,0x00,0x00, 0x00,0x00,0x00,0x00], av:new P('') },

    // TODO: and

    // TODO: or

    // within(...[])
    { v: P.within('A', 'b', '3'),
      b: [
        0x00,0x00,0x00,0x06, ...from('within'),
        0x00,0x00,0x00,0x03, // {values_length}
        0x03,0x00, 0x00,0x00,0x00,0x01, 0x41, // 'A'
        0x03,0x00, 0x00,0x00,0x00,0x01, 0x62, // 'b'
        0x03,0x00, 0x00,0x00,0x00,0x01, 0x33, // '3'
      ]
    },
    // within([...])
    { v: P.within([ 'B', 'a', '0' ]),
      b: [
        0x00,0x00,0x00,0x06, ...from('within'),
        0x00,0x00,0x00,0x03, // {values_length}
        0x03,0x00, 0x00,0x00,0x00,0x01, 0x42, // 'B'
        0x03,0x00, 0x00,0x00,0x00,0x01, 0x61, // 'a'
        0x03,0x00, 0x00,0x00,0x00,0x01, 0x30, // '0'
      ]
    },

    // without(...[])
    { v: P.without('A', 'b', '3'),
      b: [
        0x00,0x00,0x00,0x07, ...from('without'),
        0x00,0x00,0x00,0x03, // {values_length}
        0x03,0x00, 0x00,0x00,0x00,0x01, 0x41, // 'A'
        0x03,0x00, 0x00,0x00,0x00,0x01, 0x62, // 'b'
        0x03,0x00, 0x00,0x00,0x00,0x01, 0x33, // '3'
      ]
    },
    // without([...])
    { v: P.without([ 'B', 'a', '0' ]),
      b: [
        0x00,0x00,0x00,0x07, ...from('without'),
        0x00,0x00,0x00,0x03, // {values_length}
        0x03,0x00, 0x00,0x00,0x00,0x01, 0x42, // 'B'
        0x03,0x00, 0x00,0x00,0x00,0x01, 0x61, // 'a'
        0x03,0x00, 0x00,0x00,0x00,0x01, 0x30, // '0'
      ]
    },

    ...(['between','eq','gt','gte','inside','lt','lte','neq','not','outside'].map(operator => ({
      v: P[operator]('ValuE', 'OtheR'),
      b: [
        0x00,0x00,0x00,operator.length, ...from(operator),
        0x00,0x00,0x00,0x02, // {values_length}
        0x03,0x00, 0x00,0x00,0x00,0x05, ...from('ValuE'),
        0x03,0x00, 0x00,0x00,0x00,0x05, ...from('OtheR'),
      ]
    }))),

    // test
    { v: P.test(1, 2, 3),
      b: [
        0x00,0x00,0x00,0x04, ...from('test'),
        0x00,0x00,0x00,0x02, // {values_length}
        0x01,0x00, 0x00,0x00,0x00,0x01, // 1
        0x01,0x00, 0x00,0x00,0x00,0x02, // 2
      ],
    },

    { des:1, err:/buffer is missing/,                  fq:1, b:undefined },
    { des:1, err:/buffer is missing/,                  fq:0, b:undefined },
    { des:1, err:/buffer is missing/,                  fq:1, b:null },
    { des:1, err:/buffer is missing/,                  fq:0, b:null },
    { des:1, err:/buffer is empty/,                    fq:1, b:[] },
    { des:1, err:/buffer is empty/,                    fq:0, b:[] },

    { des:1, err:/unexpected {type_code}/,             fq:1, b:[0x00] },
    { des:1, err:/unexpected {type_code}/,             fq:1, b:[0x01] },
    { des:1, err:/unexpected {type_code}/,             fq:1, b:[0x1D] },
    { des:1, err:/unexpected {type_code}/,             fq:1, b:[0x1F] },
    { des:1, err:/unexpected {type_code}/,             fq:1, b:[0xE1] },
    { des:1, err:/unexpected {type_code}/,             fq:1, b:[0x0E] },
    { des:1, err:/unexpected {type_code}/,             fq:1, b:[0xFF] },

    { des:1, err:/{value_flag} is missing/,            fq:1, b:[0x1E] },
    { des:1, err:/unexpected {value_flag}/,            fq:1, b:[0x1E,0x10] },
    { des:1, err:/unexpected {value_flag}/,            fq:1, b:[0x1E,0x02] },
    { des:1, err:/unexpected {value_flag}/,            fq:1, b:[0x1E,0x0F] },
    { des:1, err:/unexpected {value_flag}/,            fq:1, b:[0x1E,0xFF] },
  ];

  describe('#serialize', () =>
    cases
    .filter(({des}) => !des)
    .forEach(({ v, fq, b }, i) => it(ser_title({i,v}), () => {
      b = from(b);

      // when fq is under control
      if (fq !== undefined) {
        assert.deepEqual( pSerializer.serialize(v, fq), b );
        return;
      }

      // generic case
      assert.deepEqual( pSerializer.serialize(v, true),  concat([type_code, value_flag, b]) );
      assert.deepEqual( pSerializer.serialize(v, false), concat([                       b]) );
    }))
  );

  describe('#deserialize', () =>
    cases.forEach(({ v, fq, b, av, err }, i) => it(des_title({i,b}), () => {
      if (Array.isArray(b))
        b = from(b);

      // wrong binary
      if (err !== undefined) {
        if (fq !== undefined)
          assert.throws(() => pSerializer.deserialize(b, fq), { message: err });
        else {
          assert.throws(() => pSerializer.deserialize(concat([type_code, value_flag, b]), true),  { message: err });
          assert.throws(() => pSerializer.deserialize(concat([                       b]), false), { message: err });
        }
        return;
      }

      if (av !== undefined)
        v = av;
      const len = b.length;

      // when fq is under control
      if (fq !== undefined) {
        assert.deepStrictEqual( pSerializer.deserialize(b, fq), {v,len} );
        return;
      }

      // generic case
      assert.deepStrictEqual( pSerializer.deserialize(concat([type_code, value_flag, b]), true),  {v,len:len+2} );
      assert.deepStrictEqual( pSerializer.deserialize(concat([                       b]), false), {v,len:len+0} );
    }))
  );

  describe('#canBeUsedFor', () =>
    // most of the cases are implicitly tested via AnySerializer.serialize() tests
    [
      { v: null,              e: false },
      { v: undefined,         e: false },
      { v: {},                e: false },
      { v: new Traverser(), e: false },
      { v: new P(),         e: true  },
      { v: [],                e: false },
      { v: [0],               e: false },
      { v: [new P()],       e: false },
    ].forEach(({ v, e }, i) => it(cbuf_title({i,v}), () =>
      assert.strictEqual( pSerializer.canBeUsedFor(v), e )
    ))
  );

});
