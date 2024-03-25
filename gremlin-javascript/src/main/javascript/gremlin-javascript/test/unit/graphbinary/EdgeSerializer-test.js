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
import { edgeSerializer } from '../../../lib/structure/io/binary/GraphBinary.js';
import { Traverser, P } from '../../../lib/process/traversal.js';
import { Edge, Vertex } from '../../../lib/structure/graph.js';

const { from, concat } = Buffer;

describe('GraphBinary.EdgeSerializer', () => {

  const type_code =  from([0x0D]);
  const value_flag = from([0x00]);

  const cases = [
    { v:undefined, fq:1, b:[0x0D,0x01], av:null },
    { v:undefined, fq:0,
      b:[
        0x03,0x00, 0x00,0x00,0x00,0x00, // {id}=''
        0x00,0x00,0x00,0x00, // {label}=''
        0x03,0x00, 0x00,0x00,0x00,0x00, // {inVId}=''
        0x00,0x00,0x00,0x00, // {inVLabel}=''
        0x03,0x00, 0x00,0x00,0x00,0x00, // {outVId}=''
        0x00,0x00,0x00,0x00, // {outVLabel}=''
        0xFE,0x01, // parent
        0xFE,0x01, // properties
      ],
      av:new Edge('', new Vertex('','',null), '', new Vertex('','',null), null),
    },
    { v:null, fq:1, b:[0x0D,0x01] },
    { v:null, fq:0,
      b:[
        0x03,0x00, 0x00,0x00,0x00,0x00, // {id}=''
        0x00,0x00,0x00,0x00, // {label}=''
        0x03,0x00, 0x00,0x00,0x00,0x00, // {inVId}=''
        0x00,0x00,0x00,0x00, // {inVLabel}=''
        0x03,0x00, 0x00,0x00,0x00,0x00, // {outVId}=''
        0x00,0x00,0x00,0x00, // {outVLabel}=''
        0xFE,0x01, // parent
        0xFE,0x01, // properties
      ],
      av:new Edge('', new Vertex('','',null), '', new Vertex('','',null), null),
    },

    { v:new Edge('A', new Vertex('B','Person',null), 'has', new Vertex('C','Pet',null), null),
      b:[
        0x03,0x00, 0x00,0x00,0x00,0x01, 0x41, // id
        0x00,0x00,0x00,0x03, ...from('has'), // label
        0x03,0x00, 0x00,0x00,0x00,0x01, 0x43, // inVId
        0x00,0x00,0x00,0x03, ...from('Pet'), // inVLabel
        0x03,0x00, 0x00,0x00,0x00,0x01, 0x42, // outVId
        0x00,0x00,0x00,0x06, ...from('Person'), // outVLabel
        0xFE,0x01, // parent
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
    { des:1, err:/unexpected {type_code}/,  fq:1, b:[0x0C] },
    { des:1, err:/unexpected {type_code}/,  fq:1, b:[0x0E] },
    { des:1, err:/unexpected {type_code}/,  fq:1, b:[0xD0] },
    { des:1, err:/unexpected {type_code}/,  fq:1, b:[0xFF] },

    { des:1, err:/{value_flag} is missing/, fq:1, b:[0x0D] },
    { des:1, err:/unexpected {value_flag}/, fq:1, b:[0x0D,0x10] },
    { des:1, err:/unexpected {value_flag}/, fq:1, b:[0x0D,0x02] },
    { des:1, err:/unexpected {value_flag}/, fq:1, b:[0x0D,0x0F] },
    { des:1, err:/unexpected {value_flag}/, fq:1, b:[0x0D,0xFF] },
  ];

  describe('#serialize', () =>
    cases
    .filter(({des}) => !des)
    .forEach(({ v, fq, b }, i) => it(ser_title({i,v}), () => {
      b = from(b);

      // when fq is under control
      if (fq !== undefined) {
        assert.deepEqual( edgeSerializer.serialize(v, fq), b );
        return;
      }

      // generic case
      assert.deepEqual( edgeSerializer.serialize(v, true),  concat([type_code, value_flag, b]) );
      assert.deepEqual( edgeSerializer.serialize(v, false), concat([                       b]) );
    }))
  );

  describe('#deserialize', () =>
    cases.forEach(({ v, fq, b, av, err }, i) => it(des_title({i,b}), () => {
      if (Array.isArray(b))
        b = from(b);

      // wrong binary
      if (err !== undefined) {
        if (fq !== undefined)
          assert.throws(() => edgeSerializer.deserialize(b, fq), { message: err });
        else {
          assert.throws(() => edgeSerializer.deserialize(concat([type_code, value_flag, b]), true),  { message: err });
          assert.throws(() => edgeSerializer.deserialize(concat([                       b]), false), { message: err });
        }
        return;
      }

      if (av !== undefined)
        v = av;
      const len = b.length;

      // when fq is under control
      if (fq !== undefined) {
        assert.deepStrictEqual( edgeSerializer.deserialize(b, fq), {v,len} );
        return;
      }

      // generic case
      assert.deepStrictEqual( edgeSerializer.deserialize(concat([type_code, value_flag, b]), true),  {v,len:len+2} );
      assert.deepStrictEqual( edgeSerializer.deserialize(concat([                       b]), false), {v,len:len+0} );
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
      { v: new Vertex(),    e: false },
      { v: [new Edge()],    e: false },
      { v: new Edge(),      e: true  },
    ].forEach(({ v, e }, i) => it(cbuf_title({i,v}), () =>
      assert.strictEqual( edgeSerializer.canBeUsedFor(v), e )
    ))
  );

});
