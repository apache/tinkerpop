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
import { byteBufferSerializer, intSerializer } from '../../../lib/structure/io/binary/GraphBinary.js';
import { Traverser } from '../../../lib/process/traversal.js';

const { from, concat } = Buffer;
import { constants } from 'buffer';

describe(`GraphBinary.ByteBufferSerializer`, () => {

  const type_code =  from([0x25]);
  const value_flag = from([0x00]);

  const cases = [
    { v:undefined,                          fq:1, b:[0x25,0x01],           av:null },
    { v:undefined,                          fq:0, b:[0x00,0x00,0x00,0x00], av:[] },
    { v:null,                               fq:1, b:[0x25,0x01] },
    { v:null,                               fq:0, b:[0x00,0x00,0x00,0x00], av:[] },

    { v:[],                                       b:[0x00,0x00,0x00,0x00] },
    { v:[0x01],                                   b:[0x00,0x00,0x00,0x01, 0x01] },
    { v:[0xFF,0x00],                              b:[0x00,0x00,0x00,0x02, 0xFF,0x00] },

    { des:1, err:/buffer is missing/,       fq:1, b:undefined },
    { des:1, err:/buffer is missing/,       fq:0, b:undefined },
    { des:1, err:/buffer is missing/,       fq:1, b:null },
    { des:1, err:/buffer is missing/,       fq:0, b:null },
    { des:1, err:/buffer is empty/,         fq:1, b:[] },
    { des:1, err:/buffer is empty/,         fq:0, b:[] },

    { des:1, err:/unexpected {type_code}/,  fq:1, b:[0x00] },
    { des:1, err:/unexpected {type_code}/,  fq:1, b:[0x01] },
    { des:1, err:/unexpected {type_code}/,  fq:1, b:[0x25-1] },
    { des:1, err:/unexpected {type_code}/,  fq:1, b:[0x25+1] },
    { des:1, err:/unexpected {type_code}/,  fq:1, b:[0x52] },
    { des:1, err:/unexpected {type_code}/,  fq:1, b:[0xFF] },

    { des:1, err:/{value_flag} is missing/, fq:1, b:[0x25] },
    { des:1, err:/unexpected {value_flag}/, fq:1, b:[0x25,0x10] },
    { des:1, err:/unexpected {value_flag}/, fq:1, b:[0x25,0x02] },
    { des:1, err:/unexpected {value_flag}/, fq:1, b:[0x25,0x0F] },
    { des:1, err:/unexpected {value_flag}/, fq:1, b:[0x25,0xFF] },

    { des:1, err:/{length} is less than zero/,       b:[0xFF,0xFF,0xFF,0xFF] },
    { des:1, err:/{length} is less than zero/,       b:[0x80,0x00,0x00,0x00] },

    { des:1, err:/unexpected actual {value} length/, b:[0x00,0x00,0x00,0x02, 0x01] },
  ];

  describe('#serialize', () => {
    cases
    .filter(({des}) => !des)
    .forEach(({ v, fq, b }, i) => it(ser_title({i,v}), () => {
      b = from(b);
      if (v)
        v = from(v);

      // when fq is under control
      if (fq !== undefined) {
        assert.deepEqual( byteBufferSerializer.serialize(v, fq), b );
        return;
      }

      // generic case
      assert.deepEqual( byteBufferSerializer.serialize(v, true),  concat([type_code, value_flag, b]) );
      assert.deepEqual( byteBufferSerializer.serialize(v, false), concat([                       b]) );
    }));

    it.skip('should not error if buffer length is INT32_MAX');

    // Buffer.size limit depends on Node version and runtime, see https://nodejs.org/docs/latest/api/buffer.html#bufferconstantsmax_length
    if (constants.MAX_LENGTH < intSerializer.INT32_MAX+1)
      it.skip(`should error if buffer length is greater than INT32_MAX - cannot be tested due to buffer.constants.MAX_LENGTH=${constants.MAX_LENGTH} < INT32_MAX+1=${intSerializer.INT32_MAX+1}`);
    else
      it('should error if buffer length is greater than INT32_MAX', () => assert.throws(
        () => byteBufferSerializer.serialize(Buffer.alloc(intSerializer.INT32_MAX+1)),
        { message: new RegExp(`Buffer length=${intSerializer.INT32_MAX+1} is greater than supported max_length=${intSerializer.INT32_MAX}`) }
      ));
  });

  describe('#deserialize', () =>
    cases.forEach(({ v, fq, b, av, err }, i) => it(des_title({i,b}), () => {
      if (Array.isArray(b))
        b = from(b);

      // wrong binary
      if (err !== undefined) {
        if (fq !== undefined)
          assert.throws(() => byteBufferSerializer.deserialize(b, fq), { message: err });
        else {
          assert.throws(() => byteBufferSerializer.deserialize(concat([type_code, value_flag, b]), true),  { message: err });
          assert.throws(() => byteBufferSerializer.deserialize(concat([                       b]), false), { message: err });
        }
        return;
      }

      if (av !== undefined)
        v = av;
      if (v)
        v = from(v);
      const len = b.length;

      // when fq is under control
      if (fq !== undefined) {
        assert.deepStrictEqual( byteBufferSerializer.deserialize(b, fq), {v,len} );
        return;
      }

      // generic case
      assert.deepStrictEqual( byteBufferSerializer.deserialize(concat([type_code, value_flag, b]), true),  {v,len:len+2} );
      assert.deepStrictEqual( byteBufferSerializer.deserialize(concat([                       b]), false), {v,len:len+0} );
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
      { v: [new Buffer([])],  e: false },
      { v: new Buffer([]),    e: true  },
    ].forEach(({ v, e }, i) => it(cbuf_title({i,v}), () =>
      assert.strictEqual( byteBufferSerializer.canBeUsedFor(v), e )
    ))
  );

});
