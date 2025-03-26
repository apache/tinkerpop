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
import { mapSerializer } from '../../../lib/structure/io/binary/GraphBinary.js';
import { Traverser } from '../../../lib/process/traversal.js';

import Bytecode from '../../../lib/process/bytecode.js';
import { GraphTraversal } from '../../../lib/process/graph-traversal.js';
const g = () => new GraphTraversal(undefined, undefined, new Bytecode());

const { from, concat } = Buffer;

describe('GraphBinary.MapSerializer', () => {

  const type_code =  from([0x0A]);
  const value_flag = from([0x00]);

  const cases = [
    { v:undefined, fq:1, b:[0x0A, 0x01],          av:null },
    { v:undefined, fq:0, b:[0x00,0x00,0x00,0x00], av:{} },
    { v:null,      fq:1, b:[0x0A, 0x01] },
    { v:null,      fq:0, b:[0x00,0x00,0x00,0x00], av:{} },

    { v:{},
      b:[0x00,0x00,0x00,0x00] },

    { v:{ 'a': 'a' },
      b:[0x00,0x00,0x00,0x01, /*'a'*/0x03,0x00,0x00,0x00,0x00,0x01,0x61, /*'a'*/0x03,0x00,0x00,0x00,0x00,0x01,0x61] },

    { v:{ 'a': 'A' },
      b:[0x00,0x00,0x00,0x01, /*'a'*/0x03,0x00,0x00,0x00,0x00,0x01,0x61, /*'A'*/0x03,0x00,0x00,0x00,0x00,0x01,0x41] },

    { v:{ 'a': 1 },
      b:[0x00,0x00,0x00,0x01, /*'a'*/0x03,0x00,0x00,0x00,0x00,0x01,0x61, /*1*/0x01,0x00,0x00,0x00,0x00,0x01] },

    { v:{ 'yz': 'A1' },
      b:[0x00,0x00,0x00,0x01, /*'yz'*/0x03,0x00,0x00,0x00,0x00,0x02,0x79,0x7A, /*'A1'*/0x03,0x00,0x00,0x00,0x00,0x02,0x41,0x31] },

    { v:{ 'one': 1, 'two': 2 },
      b:[0x00,0x00,0x00,0x02,
        /*'one'*/0x03,0x00,0x00,0x00,0x00,0x03,0x6F,0x6E,0x65, /*1*/0x01,0x00,0x00,0x00,0x00,0x01,
        /*'two'*/0x03,0x00,0x00,0x00,0x00,0x03,0x74,0x77,0x6F, /*2*/0x01,0x00,0x00,0x00,0x00,0x02,
      ]
    },

    { v:{ 'one': 1, 'two': 2, 'int32': new Map([ ['min',-2147483648], ['max',2147483647] ]) },
      b:[0x00,0x00,0x00,0x03,
        /*'one'*/0x03,0x00,0x00,0x00,0x00,0x03,0x6F,0x6E,0x65, /*1*/0x01,0x00,0x00,0x00,0x00,0x01,
        /*'two'*/0x03,0x00,0x00,0x00,0x00,0x03,0x74,0x77,0x6F, /*2*/0x01,0x00,0x00,0x00,0x00,0x02,
        /*'int32'*/ 0x03,0x00, 0x00,0x00,0x00,0x05, 0x69,0x6E,0x74,0x33,0x32,
        /*int32 map*/
        0x0A,0x00, 0x00,0x00,0x00,0x02,
          /*'min'*/0x03,0x00,0x00,0x00,0x00,0x03,0x6D,0x69,0x6E, /*-2147483648*/0x01,0x00,0x80,0x00,0x00,0x00,
          /*'max'*/0x03,0x00,0x00,0x00,0x00,0x03,0x6D,0x61,0x78, /* 2147483647*/0x01,0x00,0x7F,0xFF,0xFF,0xFF,
      ]
    },

    { v:{ aliases: new Map([ ['g','g'] ]), gremlin: g().V().getBytecode() },
      b:[0x00,0x00,0x00,0x02,
        /*'aliases'*/
        0x03,0x00, 0x00,0x00,0x00,0x07, ...from('aliases'),
        /*aliases map*/
        0x0A,0x00, 0x00,0x00,0x00,0x01,
          /*'g'*/0x03,0x00,0x00,0x00,0x00,0x01,0x67, /*'g'*/0x03,0x00,0x00,0x00,0x00,0x01,0x67,

        /*'gremlin'*/
        0x03,0x00, 0x00,0x00,0x00,0x07, ...from('gremlin'),
        /*gremlin bytecode*/
        0x15,0x00,
          // {steps_length}
          0x00,0x00,0x00,0x01,
            // step 1 - {name} String
            0x00,0x00,0x00,0x01,  ...from('V'),
            // step 1 - {values_length} Int
            0x00,0x00,0x00,0x00,
          // {sources_length}
          0x00,0x00,0x00,0x00,
      ]
    },

    { des:1, err:/buffer is missing/,                  fq:1, b:undefined },
    { des:1, err:/buffer is missing/,                  fq:0, b:undefined },
    { des:1, err:/buffer is missing/,                  fq:1, b:null },
    { des:1, err:/buffer is missing/,                  fq:0, b:null },
    { des:1, err:/buffer is empty/,                    fq:1, b:[] },
    { des:1, err:/buffer is empty/,                    fq:0, b:[] },

    { des:1, err:/unexpected {type_code}/,             fq:1, b:[0x00] },
    { des:1, err:/unexpected {type_code}/,             fq:1, b:[0x01] },
    { des:1, err:/unexpected {type_code}/,             fq:1, b:[0x09] },
    { des:1, err:/unexpected {type_code}/,             fq:1, b:[0x0B] },
    { des:1, err:/unexpected {type_code}/,             fq:1, b:[0x8A] },
    { des:1, err:/unexpected {type_code}/,             fq:1, b:[0x1A] },
    { des:1, err:/unexpected {type_code}/,             fq:1, b:[0xA0] },

    { des:1, err:/{value_flag} is missing/,            fq:1, b:[0x0A] },
    { des:1, err:/unexpected {value_flag}/,            fq:1, b:[0x0A,0x10] },
    { des:1, err:/unexpected {value_flag}/,            fq:1, b:[0x0A,0x02] },
    { des:1, err:/unexpected {value_flag}/,            fq:1, b:[0x0A,0x0F] },
    { des:1, err:/unexpected {value_flag}/,            fq:1, b:[0x0A,0xFF] },

    { des:1, err:/{length} is less than zero/,               b:[0xFF,0xFF,0xFF,0xFF] },
    { des:1, err:/{length} is less than zero/,               b:[0x80,0x00,0x00,0x00] },

    { des:1, err:/{item_0} key:.*buffer is empty/,           b:[0x00,0x00,0x00,0x01] },
    { des:1, err:/{item_0} key:.*{value_flag} is missing/,   b:[0x00,0x00,0x00,0x01, 0x01] },
    { des:1, err:/{item_0} value:.*buffer is empty/,         b:[0x00,0x00,0x00,0x01, 0x01,0x00,0x00,0x00,0x00,0x0F] },
    { des:1, err:/{item_0} value:.*{value_flag} is missing/, b:[0x00,0x00,0x00,0x01, 0x01,0x00,0x00,0x00,0x00,0x0F, 0x01] },
    { des:1, err:/{item_1} key:.*buffer is empty/,           b:[0x00,0x00,0x00,0x02, 0x01,0x00,0x00,0x00,0x00,0x00, 0x01,0x00,0x00,0x00,0x00,0x00 ] },
    { des:1, err:/{item_1} key:.*{value_flag} is missing/,   b:[0x00,0x00,0x00,0x02, 0x01,0x00,0x00,0x00,0x00,0x00, 0x01,0x00,0x00,0x00,0x00,0x00, 0x0A ] },
    { des:1, err:/{item_1} value:.*buffer is empty/,         b:[0x00,0x00,0x00,0x02, 0x01,0x00,0x00,0x00,0x00,0x00, 0x01,0x00,0x00,0x00,0x00,0x00, 0x01,0x00,0x00,0x00,0x00,0xFF ] },
    { des:1, err:/{item_1} value:.*{value_flag} is missing/, b:[0x00,0x00,0x00,0x02, 0x01,0x00,0x00,0x00,0x00,0x00, 0x01,0x00,0x00,0x00,0x00,0x00, 0x01,0x00,0x00,0x00,0x00,0xFF, 0x01 ] },
  ];

  describe('#serialize', () => {
    cases
    .filter(({des}) => !des)
    .forEach(({ v, fq, b }, i) => it(ser_title({i,v}), () => {
      let map;
      if (v)
        map = new Map( Object.entries(v) );
      b = from(b);

      // when fq is under control
      if (fq !== undefined) {
        assert.deepEqual( mapSerializer.serialize(v, fq), b );
        if (map)
          assert.deepEqual( mapSerializer.serialize(map, fq), b );
        return;
      }

      // generic case
      assert.deepEqual( mapSerializer.serialize(v, true),  concat([type_code, value_flag, b]) );
      assert.deepEqual( mapSerializer.serialize(v, false), concat([                       b]) );
      if (map) {
        assert.deepEqual( mapSerializer.serialize(map, true),  concat([type_code, value_flag, b]) );
        assert.deepEqual( mapSerializer.serialize(map, false), concat([                       b]) );
      }
    }));
  });

  describe('#deserialize', () =>
    cases.forEach(({ v, fq, b, av, err }, i) => it(des_title({i,b}), () => {
      if (Array.isArray(b))
        b = from(b);

      // wrong binary
      if (err !== undefined) {
        if (fq !== undefined)
          assert.throws(() => mapSerializer.deserialize(b, fq), { message: err });
        else {
          assert.throws(() => mapSerializer.deserialize(concat([type_code, value_flag, b]), true),  { message: err });
          assert.throws(() => mapSerializer.deserialize(concat([                       b]), false), { message: err });
        }
        return;
      }

      if (av !== undefined)
        v = av;
      if (v)
        v = new Map( Object.entries(v) );
      const len = b.length;

      // when fq is under control
      if (fq !== undefined) {
        assert.deepStrictEqual( mapSerializer.deserialize(b, fq), {v,len} );
        return;
      }

      // generic case
      assert.deepStrictEqual( mapSerializer.deserialize(concat([type_code, value_flag, b]), true),  {v,len:len+2} );
      assert.deepStrictEqual( mapSerializer.deserialize(concat([                       b]), false), {v,len:len+0} );
    }))
  );

  describe('#canBeUsedFor', () =>
    // most of the cases are implicitly tested via AnySerializer.serialize() tests
    [
      { v: null,              e: false },
      { v: undefined,         e: false },
      { v: {},                e: true },
      { v: new Traverser(), e: true },
      { v: [],                e: false },
      { v: [{}],              e: false },
      { v: [new Map()],       e: false },
      { v: new Map(),         e: true },
    ].forEach(({ v, e }, i) => it(cbuf_title({i,v}), () =>
      assert.strictEqual( mapSerializer.canBeUsedFor(v), e )
    ))
  );

});
