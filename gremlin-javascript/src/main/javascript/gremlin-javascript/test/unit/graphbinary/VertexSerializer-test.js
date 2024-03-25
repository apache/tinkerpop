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
import { vertexSerializer } from '../../../lib/structure/io/binary/GraphBinary.js';
import { Traverser, P } from '../../../lib/process/traversal.js';
import { Vertex } from '../../../lib/structure/graph.js';

const { from, concat } = Buffer;

describe('GraphBinary.VertexSerializer', () => {

  const type_code =  from([0x11]);
  const value_flag = from([0x00]);

  const cases = [
    { v:undefined, fq:1, b:[0x11,0x01],                                                     av:null },
    { v:undefined, fq:0, b:[0x03,0x00,0x00,0x00,0x00,0x00, 0x00,0x00,0x00,0x00, 0xFE,0x01], av:new Vertex('','',null) },
    { v:null,      fq:1, b:[0x11,0x01] },
    { v:null,      fq:0, b:[0x03,0x00,0x00,0x00,0x00,0x00, 0x00,0x00,0x00,0x00, 0xFE,0x01], av:new Vertex('','',null) },

    { v:new Vertex(42, 'A', -1),
      b:[
        0x01,0x00, 0x00,0x00,0x00,0x2A, // id
        0x00,0x00,0x00,0x01, 0x41, // label
        0x01,0x00, 0xFF,0xFF,0xFF,0xFF // properties
      ]
    },

    // real case with id of UUID type, but JS does not have UUID type, it's presented as a string instead
    { des:1,
      v:new Vertex('28f38e3d-7739-4c99-8284-eb43db2a80f1', 'Person', null),
      b:[
        0x0C,0x00, 0x28,0xF3,0x8E,0x3D, 0x77,0x39, 0x4C,0x99, 0x82,0x84, 0xEB,0x43,0xDB,0x2A,0x80,0xF1, // id
        0x00,0x00,0x00,0x06, ...from('Person'), // label
        0xFE,0x01, // properties
      ]
    },

    { des:1, err:/buffer is missing/,       fq:1, b:undefined },
    { des:1, err:/buffer is missing/,       fq:0, b:undefined },
    { des:1, err:/buffer is missing/,       fq:1, b:null },
    { des:1, err:/buffer is missing/,       fq:0, b:null },
    { des:1, err:/buffer is empty/,         fq:1, b:[] },
    { des:1, err:/buffer is empty/,         fq:0, b:[] },

    { des:1, err:/unexpected {type_code}/,  fq:1, b:[0x00] },
    { des:1, err:/unexpected {type_code}/,  fq:1, b:[0x01] },
    { des:1, err:/unexpected {type_code}/,  fq:1, b:[0x10] },
    { des:1, err:/unexpected {type_code}/,  fq:1, b:[0x12] },
    { des:1, err:/unexpected {type_code}/,  fq:1, b:[0xFF] },

    { des:1, err:/{value_flag} is missing/, fq:1, b:[0x11] },
    { des:1, err:/unexpected {value_flag}/, fq:1, b:[0x11,0x10] },
    { des:1, err:/unexpected {value_flag}/, fq:1, b:[0x11,0x02] },
    { des:1, err:/unexpected {value_flag}/, fq:1, b:[0x11,0x0F] },
    { des:1, err:/unexpected {value_flag}/, fq:1, b:[0x11,0xFF] },
  ];

  describe('#serialize', () =>
    cases
    .filter(({des}) => !des)
    .forEach(({ v, fq, b }, i) => it(ser_title({i,v}), () => {
      b = from(b);

      // when fq is under control
      if (fq !== undefined) {
        assert.deepEqual( vertexSerializer.serialize(v, fq), b );
        return;
      }

      // generic case
      assert.deepEqual( vertexSerializer.serialize(v, true),  concat([type_code, value_flag, b]) );
      assert.deepEqual( vertexSerializer.serialize(v, false), concat([                       b]) );
    }))
  );

  describe('#deserialize', () =>
    cases.forEach(({ v, fq, b, av, err }, i) => it(des_title({i,b}), () => {
      if (Array.isArray(b))
        b = from(b);

      // wrong binary
      if (err !== undefined) {
        if (fq !== undefined)
          assert.throws(() => vertexSerializer.deserialize(b, fq), { message: err });
        else {
          assert.throws(() => vertexSerializer.deserialize(concat([type_code, value_flag, b]), true),  { message: err });
          assert.throws(() => vertexSerializer.deserialize(concat([                       b]), false), { message: err });
        }
        return;
      }

      if (av !== undefined)
        v = av;
      const len = b.length;

      // when fq is under control
      if (fq !== undefined) {
        assert.deepStrictEqual( vertexSerializer.deserialize(b, fq), {v,len} );
        return;
      }

      // generic case
      assert.deepStrictEqual( vertexSerializer.deserialize(concat([type_code, value_flag, b]), true),  {v,len:len+2} );
      assert.deepStrictEqual( vertexSerializer.deserialize(concat([                       b]), false), {v,len:len+0} );
    }))
  );

  describe('#canBeUsedFor', () =>
    // most of the cases are implicitly tested via AnySerializer.serialize() tests
    [
      { v: null,              e: false },
      { v: undefined,         e: false },
      { v: {},                e: false },
      { v: new Traverser(), e: false },
      { v: new P(),         e: false },
      { v: [],                e: false },
      { v: [0],               e: false },
      { v: [new Vertex()],  e: false },
      { v: new Vertex(),    e: true  },
    ].forEach(({ v, e }, i) => it(cbuf_title({i,v}), () =>
      assert.strictEqual( vertexSerializer.canBeUsedFor(v), e )
    ))
  );

});
