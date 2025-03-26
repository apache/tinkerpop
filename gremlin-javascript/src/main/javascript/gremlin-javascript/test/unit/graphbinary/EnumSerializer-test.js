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
import { enumSerializer } from '../../../lib/structure/io/binary/GraphBinary.js';
import { barrier, cardinality, column, direction, operator, order, pick, pop, scope, t, EnumValue } from '../../../lib/process/traversal.js';

const { from, concat } = Buffer;

describe('GraphBinary.EnumSerializer', () => {

  const types = [
    { name: 'Barrier',     code: from([0x13]), enum: barrier },
    { name: 'Cardinality', code: from([0x16]), enum: cardinality },
    { name: 'Column',      code: from([0x17]), enum: column },
    { name: 'Direction',   code: from([0x18]), enum: direction },
    { name: 'Operator',    code: from([0x19]), enum: operator },
    { name: 'Order',       code: from([0x1A]), enum: order },
    { name: 'Pick',        code: from([0x1B]), enum: pick },
    { name: 'Pop',         code: from([0x1C]), enum: pop },
    { name: 'Scope',       code: from([0x1F]), enum: scope },
    { name: 'T',           code: from([0x20]), enum: t },
  ];
  const value_flag = from([0x00]);

  const cases = [
    { v:undefined, fq:1, b:[0x01], av:null },
    { v:undefined, fq:0, b:[0x03,0x00, 0x00,0x00,0x00,0x00], av:'' },
    { v:null,      fq:1, b:[0x01] },
    { v:null,      fq:0, b:[0x03,0x00, 0x00,0x00,0x00,0x00], av:'' },

    // real cases of serialization covered by AnySerializer#serialize tests

    { des:1, err:/buffer is missing/,       fq:1, B:undefined },
    { des:1, err:/buffer is missing/,       fq:0, B:undefined },
    { des:1, err:/buffer is missing/,       fq:1, B:null },
    { des:1, err:/buffer is missing/,       fq:0, B:null },
    { des:1, err:/buffer is empty/,         fq:1, B:[] },
    { des:1, err:/buffer is empty/,         fq:0, B:[] },

    { des:1, err:/unexpected {type_code}/,  fq:1, B:[0x00] },
    { des:1, err:/unexpected {type_code}/,  fq:1, B:[0x01] },
    { des:1, err:/unexpected {type_code}/,  fq:1, B:[0x12] },
    { des:1, err:/unexpected {type_code}/,  fq:1, B:[0x14] },
    { des:1, err:/unexpected {type_code}/,  fq:1, B:[0x1D] },
    { des:1, err:/unexpected {type_code}/,  fq:1, B:[0x21] },
    { des:1, err:/unexpected {type_code}/,  fq:1, B:[0xFF] },

    { des:1, err:/{value_flag} is missing/, fq:1, b:[] },
    { des:1, err:/unexpected {value_flag}/, fq:1, b:[0x10] },
    { des:1, err:/unexpected {value_flag}/, fq:1, b:[0x02] },
    { des:1, err:/unexpected {value_flag}/, fq:1, b:[0x0F] },
    { des:1, err:/unexpected {value_flag}/, fq:1, b:[0xFF] },

    { des:1, err:/elementName: .*StringSerializer.* unexpected {value} length/, B:[0x03,0x00, 0x00,0x00,0x00] },
  ];

  describe('#serialize', () =>
    types.forEach((type) => describe(`${type.name}`, () =>
      cases
      .filter(({des}) => !des)
      .forEach(({ v, fq, b }, i) => it(ser_title({i,v}), () => {
        b = from(b);
        v = new EnumValue(type.name, v);

        // when fq is under control
        if (fq !== undefined) {
          if (fq)
            b = concat([ type.code, b ]);
          assert.deepEqual( enumSerializer.serialize(v, fq), b );
          return;
        }

        // generic case
        assert.deepEqual( enumSerializer.serialize(v, true),  concat([type.code, value_flag, b]) );
        assert.deepEqual( enumSerializer.serialize(v, false), concat([                       b]) );
      }))
    ))
  );

  describe('#deserialize', () => {
    types.forEach((type) => describe(`${type.name}`, () =>
      cases.forEach(({ v, fq, b, B, av, err }, i) => it(des_title({i,b}), () => {
        if (B)
          b = B;
        if (Array.isArray(b)) {
          b = from(b);
          if (fq && B === undefined)
            b = concat([ type.code, b ]);
        }

        // wrong binary
        if (err !== undefined) {
          if (fq !== undefined)
            assert.throws(() => enumSerializer.deserialize(b, fq), { message: err });
          else {
            assert.throws(() => enumSerializer.deserialize(concat([type.code, value_flag, b]), true),  { message: err });
            assert.throws(() => enumSerializer.deserialize(concat([                       b]), false), { message: err });
          }
          return;
        }

        if (av !== undefined)
          v = av;
        if (v !== undefined && v !== null)
          v = new EnumValue(type.name, v);
        const len = b.length;

        // when fq is under control
        if (fq !== undefined) {
          if (v && !fq)
            v.typeName = undefined;
          assert.deepStrictEqual( enumSerializer.deserialize(b, fq), {v,len} );
          return;
        }

        // generic case
        assert.deepStrictEqual( enumSerializer.deserialize(concat([type.code, value_flag, b]), true),  {v,len:len+2} );
        v.typeName = undefined;
        assert.deepStrictEqual( enumSerializer.deserialize(concat([                       b]), false), {v,len:len+0} );
      }))
    ));

    types.forEach(type =>
      Object.values(type.enum).forEach(e => it(`should return existing t.EnumValue(${e.typeName}, ${e.elementName}) instance`, () => {
        const b = enumSerializer.serialize(e);
        const r = enumSerializer.deserialize(b);
        assert.strictEqual(r.v, e);
      }))
    );
  });

  describe('#canBeUsedFor', () => {
    it('should error if type name is not supported', () => assert.throws(
      () => enumSerializer.canBeUsedFor(new EnumValue('UnknownType', 'asc')),
      { message: /typeName=UnknownType is not supported/ }
    ));

    // most of the cases are implicitly tested via AnySerializer.serialize() tests
    [
      { v: null,                        e: false },
      { v: undefined,                   e: false },
      { v: {},                          e: false },
      { v: new EnumValue('Barrier'),  e: true  },
    ].forEach(({ v, e }, i) => it(cbuf_title({i,v}), () =>
      assert.strictEqual( enumSerializer.canBeUsedFor(v), e )
    ));
  });

});
