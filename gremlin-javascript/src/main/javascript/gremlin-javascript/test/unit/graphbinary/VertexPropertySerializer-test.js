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
const { vertexPropertySerializer } = require('../../../lib/structure/io/binary/GraphBinary');
const t = require('../../../lib/process/traversal');
const g = require('../../../lib/structure/graph');

const { from, concat } = Buffer;

describe('GraphBinary.VertexPropertySerializer', () => {

  const type_code =  from([0x12]);
  const value_flag = from([0x00]);

  const cases = [
    { v:undefined, fq:1, b:[0x12,0x01],                                                       av:null },
    { v:undefined, fq:0, b:[0xFE,0x01, 0x00,0x00,0x00,0x00, 0xFE,0x01, 0xFE,0x01, 0xFE,0x01], av:new g.VertexProperty(null,'',null,null) },
    { v:null,      fq:1, b:[0x12,0x01] },
    { v:null,      fq:0, b:[0xFE,0x01, 0x00,0x00,0x00,0x00, 0xFE,0x01, 0xFE,0x01, 0xFE,0x01], av:new g.VertexProperty(null,'',null,null) },

    { v:new g.VertexProperty('Id', 'Label', 'Value', null),
      b:[
        0x03,0x00, 0x00,0x00,0x00,0x02, ...from('Id'),
        0x00,0x00,0x00,0x05, ...from('Label'),
        0x03,0x00, 0x00,0x00,0x00,0x05, ...from('Value'),
        0xFE,0x01,
        0xFE,0x01,
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
    { des:1, err:/unexpected {type_code}/,  fq:1, b:[0x11] },
    { des:1, err:/unexpected {type_code}/,  fq:1, b:[0x13] },
    { des:1, err:/unexpected {type_code}/,  fq:1, b:[0x21] },
    { des:1, err:/unexpected {type_code}/,  fq:1, b:[0x1F] },
    { des:1, err:/unexpected {type_code}/,  fq:1, b:[0xFF] },

    { des:1, err:/{value_flag} is missing/, fq:1, b:[0x12] },
    { des:1, err:/unexpected {value_flag}/, fq:1, b:[0x12,0x10] },
    { des:1, err:/unexpected {value_flag}/, fq:1, b:[0x12,0x02] },
    { des:1, err:/unexpected {value_flag}/, fq:1, b:[0x12,0x0F] },
    { des:1, err:/unexpected {value_flag}/, fq:1, b:[0x12,0xFF] },
  ];

  describe('#serialize', () =>
    cases
    .filter(({des}) => !des)
    .forEach(({ v, fq, b }, i) => it(utils.ser_title({i,v}), () => {
      b = from(b);

      // when fq is under control
      if (fq !== undefined) {
        assert.deepEqual( vertexPropertySerializer.serialize(v, fq), b );
        return;
      }

      // generic case
      assert.deepEqual( vertexPropertySerializer.serialize(v, true),  concat([type_code, value_flag, b]) );
      assert.deepEqual( vertexPropertySerializer.serialize(v, false), concat([                       b]) );
    }))
  );

  describe('#deserialize', () =>
    cases.forEach(({ v, fq, b, av, err }, i) => it(utils.des_title({i,b}), () => {
      if (Array.isArray(b))
        b = from(b);

      // wrong binary
      if (err !== undefined) {
        if (fq !== undefined)
          assert.throws(() => vertexPropertySerializer.deserialize(b, fq), { message: err });
        else {
          assert.throws(() => vertexPropertySerializer.deserialize(concat([type_code, value_flag, b]), true),  { message: err });
          assert.throws(() => vertexPropertySerializer.deserialize(concat([                       b]), false), { message: err });
        }
        return;
      }

      if (av !== undefined)
        v = av;
      const len = b.length;

      // when fq is under control
      if (fq !== undefined) {
        assert.deepStrictEqual( vertexPropertySerializer.deserialize(b, fq), {v,len} );
        return;
      }

      // generic case
      assert.deepStrictEqual( vertexPropertySerializer.deserialize(concat([type_code, value_flag, b]), true),  {v,len:len+2} );
      assert.deepStrictEqual( vertexPropertySerializer.deserialize(concat([                       b]), false), {v,len:len+0} );
    }))
  );

  describe('#canBeUsedFor', () =>
    // most of the cases are implicitly tested via AnySerializer.serialize() tests
    [
      { v: null,                     e: false },
      { v: undefined,                e: false },
      { v: {},                       e: false },
      { v: new t.Traverser(),        e: false },
      { v: new t.P(),                e: false },
      { v: [],                       e: false },
      { v: [0],                      e: false },
      { v: [new g.VertexProperty()], e: false },
      { v: new g.VertexProperty(),   e: true  },
    ].forEach(({ v, e }, i) => it(utils.cbuf_title({i,v}), () =>
      assert.strictEqual( vertexPropertySerializer.canBeUsedFor(v), e )
    ))
  );

});
