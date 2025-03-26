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
import { bytecodeSerializer } from '../../../lib/structure/io/binary/GraphBinary.js';

import { Traverser, Traversal } from '../../../lib/process/traversal.js';
import Bytecode from '../../../lib/process/bytecode.js';
import { GraphTraversal } from '../../../lib/process/graph-traversal.js';
import { ReservedKeysVerificationStrategy } from '../../../lib/process/traversal-strategy.js';

const g = (strategy) => {
  const bc = new Bytecode();
  if (strategy)
    bc.addSource('withStrategies', [ strategy ]);
  return new GraphTraversal(undefined, undefined, bc);
}

const { from, concat } = Buffer;

describe('GraphBinary.BytecodeSerializer', () => {

  const type_code =  from([0x15]);
  const value_flag = from([0x00]);

  const cases = [
    { v:undefined, fq:1, b:[0x15,0x01],                                av:null },
    { v:undefined, fq:0, b:[0x00,0x00,0x00,0x00, 0x00,0x00,0x00,0x00], av:g() },
    { v:null,      fq:1, b:[0x15,0x01] },
    { v:null,      fq:0, b:[0x00,0x00,0x00,0x00, 0x00,0x00,0x00,0x00], av:g() },

    { v: g(),
      b: [
        0x00,0x00,0x00,0x00, // {steps_length}
        0x00,0x00,0x00,0x00, // {sources_length}
    ]},

    { v: g().V(),
      b: [
        0x00,0x00,0x00,0x01, // {steps_length}
          0x00,0x00,0x00,0x01, 0x56, // V
          0x00,0x00,0x00,0x00, // (void)
        0x00,0x00,0x00,0x00, // {sources_length}
    ]},

    { v: g().V().hasLabel('Person').has('age', 42),
      b: [
        0x00,0x00,0x00,0x03, // {steps_length}
          0x00,0x00,0x00,0x01, 0x56, // V
            0x00,0x00,0x00,0x00, // ([0])
          0x00,0x00,0x00,0x08, 0x68,0x61,0x73,0x4C,0x61,0x62,0x65,0x6C, // hasLabel
            0x00,0x00,0x00,0x01, // ([1])
            0x03,0x00, 0x00,0x00,0x00,0x06, 0x50,0x65,0x72,0x73,0x6F,0x6E, // 'Person'
          0x00,0x00,0x00,0x03, 0x68,0x61,0x73, // has
            0x00,0x00,0x00,0x02, // ([2])
            0x03,0x00, 0x00,0x00,0x00,0x03, 0x61,0x67,0x65, // 'age'
            0x01,0x00, 0x00,0x00,0x00,0x2A, // 42
        0x00,0x00,0x00,0x00, // {sources_length}
    ]},

    { ser:1, v: g(new ReservedKeysVerificationStrategy(false, true)).V().hasLabel('Person').has('age', 42),
      b: [
        0x00,0x00,0x00,0x03, // {steps_length}
          0x00,0x00,0x00,0x01, 0x56, // V
            0x00,0x00,0x00,0x00, // ([0])
          0x00,0x00,0x00,0x08, 0x68,0x61,0x73,0x4C,0x61,0x62,0x65,0x6C, // hasLabel
            0x00,0x00,0x00,0x01, // ([1])
            0x03,0x00, 0x00,0x00,0x00,0x06, 0x50,0x65,0x72,0x73,0x6F,0x6E, // 'Person'
          0x00,0x00,0x00,0x03, 0x68,0x61,0x73, // has
            0x00,0x00,0x00,0x02, // ([2])
            0x03,0x00, 0x00,0x00,0x00,0x03, 0x61,0x67,0x65, // 'age'
            0x01,0x00, 0x00,0x00,0x00,0x2A, // 42
        0x00,0x00,0x00,0x01, // {sources_length}
          0x00,0x00,0x00,0x0E, ...from('withStrategies'),
          0x00,0x00,0x00,0x01,
            0x29,0x00, 0x00,0x00,0x00,0x65, ...from('org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReservedKeysVerificationStrategy'),
            0x00,0x00,0x00,0x03, // configuration map size
              0x03,0x00, 0x00,0x00,0x00,0x0B, ...from('logWarnings'),
              0x27,0x00, 0x00, // false
              0x03,0x00, 0x00,0x00,0x00,0x0E, ...from('throwException'),
              0x27,0x00, 0x01, // true
              0x03,0x00, 0x00,0x00,0x00,0x04, ...from('keys'),
              0x09,0x00, 0x00,0x00,0x00,0x02,
                0x03,0x00, 0x00,0x00,0x00,0x02, ...from('id'),
                0x03,0x00, 0x00,0x00,0x00,0x05, ...from('label'),
    ]},

    { des:1, err:/buffer is missing/,                  fq:1, b:undefined },
    { des:1, err:/buffer is missing/,                  fq:0, b:undefined },
    { des:1, err:/buffer is missing/,                  fq:1, b:null },
    { des:1, err:/buffer is missing/,                  fq:0, b:null },
    { des:1, err:/buffer is empty/,                    fq:1, b:[] },
    { des:1, err:/buffer is empty/,                    fq:0, b:[] },

    { des:1, err:/unexpected {type_code}/,             fq:1, b:[0x00] },
    { des:1, err:/unexpected {type_code}/,             fq:1, b:[0x14] },
    { des:1, err:/unexpected {type_code}/,             fq:1, b:[0x16] },
    { des:1, err:/unexpected {type_code}/,             fq:1, b:[0x51] },
    { des:1, err:/unexpected {type_code}/,             fq:1, b:[0x85] },
    { des:1, err:/unexpected {type_code}/,             fq:1, b:[0x5F] },
    { des:1, err:/unexpected {type_code}/,             fq:1, b:[0xFF] },

    { des:1, err:/{value_flag} is missing/,            fq:1, b:[0x15] },
    { des:1, err:/unexpected {value_flag}/,            fq:1, b:[0x15,0x10] },
    { des:1, err:/unexpected {value_flag}/,            fq:1, b:[0x15,0x02] },
    { des:1, err:/unexpected {value_flag}/,            fq:1, b:[0x15,0x0F] },
    { des:1, err:/unexpected {value_flag}/,            fq:1, b:[0x15,0xFF] },

    { des:1, err:/{steps_length} is less than zero/,         b:[0xFF,0xFF,0xFF,0xFF] },
    { des:1, err:/{steps_length} is less than zero/,         b:[0x80,0x00,0x00,0x00] },

    { des:1, err:/{sources_length} is less than zero/,       b:[0x00,0x00,0x00,0x00, 0xFF,0xFF,0xFF,0xFF] },
    { des:1, err:/{sources_length} is less than zero/,       b:[0x00,0x00,0x00,0x00, 0x80,0x00,0x00,0x00] },
  ];

  describe('#serialize', () =>
    cases
    .filter(({des}) => !des)
    .forEach(({ v, fq, b }, i) => it(ser_title({i,v}), () => {
      b = from(b);

      // when fq is under control
      if (fq !== undefined) {
        assert.deepEqual( bytecodeSerializer.serialize(v, fq), b );
        return;
      }

      // generic case
      v = v.getBytecode();
      assert.deepEqual( bytecodeSerializer.serialize(v, true),  concat([type_code, value_flag, b]) );
      assert.deepEqual( bytecodeSerializer.serialize(v, false), concat([                       b]) );
    }))
  );

  describe('#deserialize', () =>
    cases
    .filter(({ser}) => !ser)
    .forEach(({ v, fq, b, av, err }, i) => it(des_title({i,b}), () => {
      if (Array.isArray(b))
        b = from(b);

      // wrong binary
      if (err !== undefined) {
        if (fq !== undefined)
          assert.throws(() => bytecodeSerializer.deserialize(b, fq), { message: err });
        else {
          assert.throws(() => bytecodeSerializer.deserialize(concat([type_code, value_flag, b]), true),  { message: err });
          assert.throws(() => bytecodeSerializer.deserialize(concat([                       b]), false), { message: err });
        }
        return;
      }

      if (av !== undefined)
        v = av;
      if (v)
        v = v.getBytecode();
      const len = b.length;

      // when fq is under control
      if (fq !== undefined) {
        assert.deepStrictEqual( bytecodeSerializer.deserialize(b, fq), {v,len} );
        return;
      }

      // generic case
      assert.deepStrictEqual( bytecodeSerializer.deserialize(concat([type_code, value_flag, b]), true),  {v,len:len+2} );
      assert.deepStrictEqual( bytecodeSerializer.deserialize(concat([                       b]), false), {v,len:len+0} );
    }))
  );

  describe('#canBeUsedFor', () =>
    // most of the cases are implicitly tested via AnySerializer.serialize() tests
    [
      { v: null,              e: false },
      { v: undefined,         e: false },
      { v: {},                e: false },
      { v: new Traverser(), e: false },
      { v: [],                e: false },
      { v: [0],               e: false },
      { v: [undefined],       e: false },
      { v: [new Bytecode()],  e: false },
      { v: new Bytecode(),    e: true  },
      { v: [new Traversal], e: false },
      { v: new Traversal,   e: true  },
    ].forEach(({ v, e }, i) => it(cbuf_title({i,v}), () =>
      assert.strictEqual( bytecodeSerializer.canBeUsedFor(v), e )
    ))
  );

});

