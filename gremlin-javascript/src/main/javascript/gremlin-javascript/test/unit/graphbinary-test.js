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
 * Glossary:
 *   v  - JavaScript value
 *   b - binary value
 *   av - actually expected JavaScript value
 *   fq - fullyQualifiedFormat
 *   na - nullable
 *
 * @author Igor Ostapenko
 */
'use strict';

const assert = require('assert');

const { GraphBinaryWriter, GraphBinaryReader } = require('../../lib/structure/io/binary/graph-serializer');
const {
  IntSerializer,
  StringSerializer,
  MapSerializer,
  UuidSerializer,
  BytecodeSerializer,
} = require('../../lib/structure/io/binary/type-serializers');

const Bytecode = require('../../lib/process/bytecode');
const { GraphTraversal } = require('../../lib/process/graph-traversal');
const g = () => new GraphTraversal(undefined, undefined, new Bytecode());

const { from, concat } = Buffer;

describe('GraphBinary.AnySerializer', () => {

  describe('getSerializerCanBeUsedFor', () =>
    it.skip('')
  );

  describe('serialize', () =>
    it.skip('')
  );

});

describe('GraphBinary.IntSerializer', () => {

  const type_code = from([0x01]);
  const value_flag = from([0x00]);

  const cases = [
    { v:undefined,                          fq:1, b:[0x01,0x01],           av:null },
    { v:undefined,                          fq:0, b:[0x00,0x00,0x00,0x00], av:0 },
    { v:null,                               fq:1, b:[0x01,0x01] },
    { v:null,                               fq:0, b:[0x00,0x00,0x00,0x00], av:0 },
    { v:0,                                        b:[0x00,0x00,0x00,0x00] },
    { v:1,                                        b:[0x00,0x00,0x00,0x01] },
    { v:256,                                      b:[0x00,0x00,0x01,0x00] },
    { v:65536,                                    b:[0x00,0x01,0x00,0x00] },
    { v:16777216,                                 b:[0x01,0x00,0x00,0x00] },
    { v:2147483647,                               b:[0x7F,0xFF,0xFF,0xFF] },
    { v:-1,                                       b:[0xFF,0xFF,0xFF,0xFF] },
    { v:-2147483648,                              b:[0x80,0x00,0x00,0x00] },

    { des:1, err:/buffer is missing/,       fq:1, b:undefined },
    { des:1, err:/buffer is missing/,       fq:0, b:undefined },
    { des:1, err:/buffer is missing/,       fq:1, b:null },
    { des:1, err:/buffer is missing/,       fq:0, b:null },
    { des:1, err:/buffer is empty/,         fq:1, b:[] },
    { des:1, err:/buffer is empty/,         fq:0, b:[] },

    { des:1, err:/unexpected type code/,    fq:1, b:[0x00] },
    { des:1, err:/unexpected type code/,    fq:1, b:[0x02] },
    { des:1, err:/unexpected type code/,    fq:1, b:[0x1D] },
    { des:1, err:/unexpected type code/,    fq:1, b:[0x81] },
    { des:1, err:/unexpected type code/,    fq:1, b:[0x0F] },
    { des:1, err:/unexpected type code/,    fq:1, b:[0xFF] },

    { des:1, err:/value flag is missing/,   fq:1, b:[0x01] },
    { des:1, err:/unexpected value flag/,   fq:1, b:[0x01,0x10] },
    { des:1, err:/unexpected value flag/,   fq:1, b:[0x01,0x02] },
    { des:1, err:/unexpected value flag/,   fq:1, b:[0x01,0x0F] },
    { des:1, err:/unexpected value flag/,   fq:1, b:[0x01,0xFF] },

    { des:1, err:/unexpected value length/, fq:1, b:[0x01,0x00] },
    { des:1, err:/unexpected value length/,       b:[0x11] },
    { des:1, err:/unexpected value length/,       b:[0x11,0x22,0x33] },
  ];

  describe('canBeUsedFor', () =>
    it.skip('')
  );

  describe('serialize', () =>
    cases.forEach(({ des, v, fq, b }, i) => it(`should be able to handle case #${i}`, () => {
      // deserialize case only
      if (des)
        return; // keep it like passed test not to mess with case index

      b = from(b);

      // when fq is under control
      if (fq !== undefined) {
        assert.deepEqual( IntSerializer.serialize(v, fq), b );
        return;
      }

      // generic case
      assert.deepEqual( IntSerializer.serialize(v, true),  concat([type_code, value_flag, b]) );
      assert.deepEqual( IntSerializer.serialize(v, false), concat([                       b]) );
    }))
  );

  describe('deserialize', () =>
    cases.forEach(({ v, fq, b, av, err }, i) => it(`should be able to handle case #${i}`, () => {
      if (Array.isArray(b))
        b = from(b);

      // wrong binary
      if (err !== undefined) {
        if (fq !== undefined)
          assert.throws(() => IntSerializer.deserialize(b, fq), { message: err });
        else {
          assert.throws(() => IntSerializer.deserialize(concat([type_code, value_flag, b]), true),  { message: err });
          assert.throws(() => IntSerializer.deserialize(concat([                       b]), false), { message: err });
        }
        return;
      }

      if (av !== undefined)
        v = av;
      const len = b.length;

      // when fq is under control
      if (fq !== undefined) {
        assert.deepStrictEqual( IntSerializer.deserialize(from(b), fq), {v,len} );
        return;
      }

      // generic case
      assert.deepStrictEqual( IntSerializer.deserialize(concat([type_code, value_flag, b]), true),  {v,len:len+2} );
      assert.deepStrictEqual( IntSerializer.deserialize(concat([                       b]), false), {v,len:len+0} );
    }))
  );

});

describe('GraphBinary.StringSerializer', () => {

  describe('canBeUsedFor', () =>
    [
      { v: 'some string',    e: true },
      { v: '',               e: true },
      { v: 'Z',              e: true },
      { v: 'Україна',        e: true },
      { v: true,             e: false },
      { v: false,            e: false },
      { v: null,             e: false },
      { v: undefined,        e: false },
      { v: Number.MAX_VALUE, e: false },
      { v: 42,               e: false },
      { v: 0,                e: false },
      { v: Number.MIN_VALUE, e: false },
      { v: NaN,              e: false },
      { v: +Infinity,        e: false },
      { v: -Infinity,        e: false },
      //{ v: Symbol(''), e: false },
    ].forEach(({ v, e }, i) => it(`should return ${e} if value is '${v}', case #${i}`, () => assert.strictEqual(
      StringSerializer.canBeUsedFor(v),
      e,
    )))
  );

  describe('serialize', () =>
    [
      { v: undefined,   fq: true,  e: [0x03,0x01] },
      { v: undefined,   fq: false, e: [           0x00,0x00,0x00,0x00] },
      { v: null,        fq: true,  e: [0x03,0x01] },
      { v: null,        fq: false, e: [           0x00,0x00,0x00,0x00] },
      // the following will be automatically tested for fq=false/true
      { v: '',          e: [0x00,0x00,0x00,0x00] },
      { v: 'Sun',       e: [0x00,0x00,0x00,0x03, 0x53,0x75,0x6E] },
      { v: 'ήλιος',     e: [0x00,0x00,0x00,0x0A, 0xCE,0xAE, 0xCE,0xBB, 0xCE,0xB9, 0xCE,0xBF, 0xCF,0x82] },
    ].forEach(({ v, fq, e }, i) => it(`should be able to handle value of case #${i}`, () => {
      if (fq !== undefined) {
        assert.deepEqual( StringSerializer.serialize(v, fq), Buffer.from(e) );
        return;
      }
      assert.deepEqual( StringSerializer.serialize(v, false), Buffer.from(e) );
      assert.deepEqual( StringSerializer.serialize(v, true), Buffer.concat([Buffer.from([0x03,0x00]), Buffer.from(e)]) );
    }))
  );

  describe('deserialize', () =>
    it.skip('')
  );

});

describe('GraphBinary.MapSerializer', () => {

  describe('canBeUsedFor', () =>
    it.skip('')
  );

  describe('serialize', () => {
    [
      { v: undefined, fq: true,  e: [0x0A, 0x01] },
      { v: undefined, fq: false, e: [            0x00,0x00,0x00,0x00] },
      { v: null,      fq: true,  e: [0x0A, 0x01] },
      { v: null,      fq: false, e: [            0x00,0x00,0x00,0x00] },
      // the following will be automatically tested for fq=false/true
      { v: {},
        e: [0x00,0x00,0x00,0x00] },
      { v: { 'a': 'a' },
        e: [0x00,0x00,0x00,0x01, /*'a'*/0x03,0x00,0x00,0x00,0x00,0x01,0x61, /*'a'*/0x03,0x00,0x00,0x00,0x00,0x01,0x61] },
      { v: { 'a': 'A' },
        e: [0x00,0x00,0x00,0x01, /*'a'*/0x03,0x00,0x00,0x00,0x00,0x01,0x61, /*'A'*/0x03,0x00,0x00,0x00,0x00,0x01,0x41] },
      { v: { 'a': 1 },
        e: [0x00,0x00,0x00,0x01, /*'a'*/0x03,0x00,0x00,0x00,0x00,0x01,0x61, /*1*/0x01,0x00,0x00,0x00,0x00,0x01] },
      { v: { 'yz': 'A1' },
        e: [0x00,0x00,0x00,0x01, /*'yz'*/0x03,0x00,0x00,0x00,0x00,0x02,0x79,0x7A, /*'A1'*/0x03,0x00,0x00,0x00,0x00,0x02,0x41,0x31] },
      { v: { 'one': 1, 'two': 2 },
        e: [0x00,0x00,0x00,0x02,
          /*'one'*/0x03,0x00,0x00,0x00,0x00,0x03,0x6F,0x6E,0x65, /*1*/0x01,0x00,0x00,0x00,0x00,0x01,
          /*'two'*/0x03,0x00,0x00,0x00,0x00,0x03,0x74,0x77,0x6F, /*2*/0x01,0x00,0x00,0x00,0x00,0x02,
        ]
      },
      { v: { 'one': 1, 'two': 2, 'int32': { 'min': -2147483648, 'max': 2147483647 } },
        e: [0x00,0x00,0x00,0x03,
          /*'one'*/0x03,0x00,0x00,0x00,0x00,0x03,0x6F,0x6E,0x65, /*1*/0x01,0x00,0x00,0x00,0x00,0x01,
          /*'two'*/0x03,0x00,0x00,0x00,0x00,0x03,0x74,0x77,0x6F, /*2*/0x01,0x00,0x00,0x00,0x00,0x02,
          /*'int32'*/ 0x03,0x00, 0x00,0x00,0x00,0x05, 0x69,0x6E,0x74,0x33,0x32,
          /*int32 map*/
          0x0A,0x00, 0x00,0x00,0x00,0x02,
            /*'min'*/0x03,0x00,0x00,0x00,0x00,0x03,0x6D,0x69,0x6E, /*-2147483648*/0x01,0x00,0x80,0x00,0x00,0x00,
            /*'max'*/0x03,0x00,0x00,0x00,0x00,0x03,0x6D,0x61,0x78, /* 2147483647*/0x01,0x00,0x7F,0xFF,0xFF,0xFF,
        ]
      },
      { v: { aliases: {g:'g'}, gremlin: g().V().getBytecode() },
        e: [0x00,0x00,0x00,0x02,
          /*'aliases'*/
          0x03,0x00, 0x00,0x00,0x00,0x07, ...Buffer.from('aliases'),
          /*aliases map*/
          0x0A,0x00, 0x00,0x00,0x00,0x01,
            /*'g'*/0x03,0x00,0x00,0x00,0x00,0x01,0x67, /*'g'*/0x03,0x00,0x00,0x00,0x00,0x01,0x67,

          /*'gremlin'*/
          0x03,0x00, 0x00,0x00,0x00,0x07, ...Buffer.from('gremlin'),
          /*gremlin bytecode*/
          0x15,0x00,
            // {steps_length}
            0x00,0x00,0x00,0x01,
              // step 1 - {name} String
              0x00,0x00,0x00,0x01,  ...Buffer.from('V'),
              // step 1 - {values_length} Int
              0x00,0x00,0x00,0x00,
            // {sources_length}
            0x00,0x00,0x00,0x00,
        ]
      },
    ].forEach(({ v, fq, e }, i) => it(`should be able to handle value of case #${i}`, () => {
      if (fq !== undefined) {
        assert.deepEqual( MapSerializer.serialize(v, fq), Buffer.from(e) );
        return;
      }
      assert.deepEqual( MapSerializer.serialize(v, false), Buffer.from(e) );
      assert.deepEqual( MapSerializer.serialize(v, true), Buffer.concat([Buffer.from([0x0A,0x00]), Buffer.from(e)]) );
    }));
  });

  describe('deserialize', () =>
    it.skip('')
  );

});

describe('GraphBinary.UuidSerializer', () => {

  const type_code = from([0x0C]);
  const value_flag = from([0x00]);

  const cases = [
    {        v:undefined,                    fq:1,       b:[0x0C,0x01],                                                                          av:null },
    {        v:undefined,                    fq:0,       b:[0x00,0x00,0x00,0x00, 0x00,0x00,0x00,0x00, 0x00,0x00,0x00,0x00, 0x00,0x00,0x00,0x00], av:'00000000-0000-0000-0000-000000000000' },
    {        v:null,                         fq:1,       b:[0x0C,0x01] },
    {        v:null,                         fq:0,       b:[0x00,0x00,0x00,0x00, 0x00,0x00,0x00,0x00, 0x00,0x00,0x00,0x00, 0x00,0x00,0x00,0x00], av:'00000000-0000-0000-0000-000000000000' },
    { des:1, v:null,                         fq:0, na:1, b:[0x01] },

    { v:'00000000-0000-0000-0000-000000000000',          b:[0x00,0x00,0x00,0x00, 0x00,0x00,0x00,0x00, 0x00,0x00,0x00,0x00, 0x00,0x00,0x00,0x00] },
    { v:'00010203-0405-0607-0809-0A0B0C0D0E0F',          b:[0x00,0x01,0x02,0x03, 0x04,0x05,0x06,0x07, 0x08,0x09,0x0A,0x0B, 0x0C,0x0D,0x0E,0x0F] },
    { v:'AaBbCcDd-EeFf-1122-3344-556677889900',          b:[0xAA,0xBB,0xCC,0xDD, 0xEE,0xFF,0x11,0x22, 0x33,0x44,0x55,0x66, 0x77,0x88,0x99,0x00] },

    // string presentation formats
    { v:'000102030405060708090A0B0C0D0E0F',              b:[0x00,0x01,0x02,0x03, 0x04,0x05,0x06,0x07, 0x08,0x09,0x0A,0x0B, 0x0C,0x0D,0x0E,0x0F], av:'00010203-0405-0607-0809-0A0B0C0D0E0F' },
    { v:'00010203-0405-0607-0809-0A0B0C0D0E0F',          b:[0x00,0x01,0x02,0x03, 0x04,0x05,0x06,0x07, 0x08,0x09,0x0A,0x0B, 0x0C,0x0D,0x0E,0x0F] },
    { v:'{00010203-0405-0607-0809-0A0B0C0D0E0F}',        b:[0x00,0x01,0x02,0x03, 0x04,0x05,0x06,0x07, 0x08,0x09,0x0A,0x0B, 0x0C,0x0D,0x0E,0x0F], av:'00010203-0405-0607-0809-0A0B0C0D0E0F' },
    { v:'urn:uuid:00010203-0405-0607-0809-0A0B0C0D0E0F', b:[0x00,0x01,0x02,0x03, 0x04,0x05,0x06,0x07, 0x08,0x09,0x0A,0x0B, 0x0C,0x0D,0x0E,0x0F], av:'00010203-0405-0607-0809-0A0B0C0D0E0F' },

    // wrong string presentation
    { v:'AaBbCcDd-EeFf-1122-3344-556677889900FFFF',      b:[0xAA,0xBB,0xCC,0xDD, 0xEE,0xFF,0x11,0x22, 0x33,0x44,0x55,0x66, 0x77,0x88,0x99,0x00], av:'AaBbCcDd-EeFf-1122-3344-556677889900' },
    { v:'GHIJKLMN-OPQR-STUV-WXYZ-!@#$%^&*()_+',          b:[0x00,0x00,0x00,0x00, 0x00,0x00,0x00,0x00, 0x00,0x00,0x00,0x00, 0x00,0x00,0x00,0x00], av:'00000000-0000-0000-0000-000000000000' },

    { des:1, err:/buffer is missing/,        fq:1,       b:undefined },
    { des:1, err:/buffer is missing/,        fq:0,       b:undefined },
    { des:1, err:/buffer is missing/,        fq:1,       b:null },
    { des:1, err:/buffer is missing/,        fq:0,       b:null },
    { des:1, err:/buffer is empty/,          fq:1,       b:[] },
    { des:1, err:/buffer is empty/,          fq:0,       b:[] },
    { des:1, err:/buffer is empty/,          fq:0, na:1, b:[] },

    { des:1, err:/unexpected type code/,     fq:1,       b:[0x01] },
    { des:1, err:/unexpected type code/,     fq:1,       b:[0x00] },
    { des:1, err:/unexpected type code/,     fq:1,       b:[0x0D] },
    { des:1, err:/unexpected type code/,     fq:1,       b:[0x0B] },
    { des:1, err:/unexpected type code/,     fq:1,       b:[0x8C] },
    { des:1, err:/unexpected type code/,     fq:1,       b:[0xFC] },

    { des:1, err:/value flag is missing/,    fq:1,       b:[0x0C] },
    { des:1, err:/unexpected value flag/,    fq:1,       b:[0x0C,0x10] },
    { des:1, err:/unexpected value flag/,    fq:0, na:1, b:[0x10] },
    { des:1, err:/unexpected value flag/,    fq:1,       b:[0x0C,0x02] },
    { des:1, err:/unexpected value flag/,    fq:0, na:1, b:[0x02] },
    { des:1, err:/unexpected value flag/,    fq:1,       b:[0x0C,0x0F] },
    { des:1, err:/unexpected value flag/,    fq:0, na:1, b:[0x0F] },
    { des:1, err:/unexpected value flag/,    fq:1,       b:[0x0C,0xFF] },
    { des:1, err:/unexpected value flag/,    fq:0, na:1, b:[0xFF] },

    { des:1, err:/unexpected value length/,  fq:1,       b:[0x0C,0x00] },
    { des:1, err:/unexpected value length/,  fq:0, na:1, b:[0x00] },
    { des:1, err:/unexpected value length/,              b:[0x00] },
    { des:1, err:/unexpected value length/,              b:[0x00,0x01,0x02,0x03,0x04,0x05,0x06,0x07,0x08,0x09,0x0A,0x0B,0x0C,0x0D,0x0E] },
  ];

  describe('canBeUsedFor', () =>
    it.skip('')
  );

  describe('serialize', () =>
    cases.forEach(({ des, v, fq, b, berr }, i) => it(`should be able to handle case #${i}`, () => {
      // deserialize case only
      if (des)
        return; // keep it like passed test not to mess with case index

      b = from(b);

      // when fq is under control
      if (fq !== undefined) {
        assert.deepEqual( UuidSerializer.serialize(v, fq), b );
        return;
      }

      // generic case
      assert.deepEqual( UuidSerializer.serialize(v, true),  concat([type_code, value_flag, b]) );
      assert.deepEqual( UuidSerializer.serialize(v, false), concat([                       b]) );
    }))
  );

  describe('deserialize', () =>
    cases.forEach(({ v, fq, na, b, av, err }, i) => it(`should be able to handle case #${i}`, () => {
      if (Array.isArray(b))
        b = from(b);

      // wrong binary
      if (err !== undefined) {
        if (fq !== undefined)
          assert.throws(() => UuidSerializer.deserialize(b, fq, na), { message: err });
        else {
          assert.throws(() => UuidSerializer.deserialize(concat([type_code, value_flag, b]), true),         { message: err });
          assert.throws(() => UuidSerializer.deserialize(concat([           value_flag, b]), false, true),  { message: err });
          assert.throws(() => UuidSerializer.deserialize(concat([                       b]), false, false), { message: err });
        }
        return;
      }

      if (av !== undefined)
        v = av;
      if (typeof v === 'string')
        v = v.toLowerCase();
      const len = b.length;

      // when fq is under control
      if (fq !== undefined) {
        assert.deepStrictEqual( UuidSerializer.deserialize(from(b), fq, na), {v,len} );
        return;
      }

      // generic case
      assert.deepStrictEqual( UuidSerializer.deserialize(concat([type_code, value_flag, b]), true,  false), {v,len:len+2} );
      assert.deepStrictEqual( UuidSerializer.deserialize(concat([type_code, value_flag, b]), true,  true),  {v,len:len+2} );
      assert.deepStrictEqual( UuidSerializer.deserialize(concat([           value_flag, b]), false, true),  {v,len:len+1} );
      assert.deepStrictEqual( UuidSerializer.deserialize(concat([                       b]), false, false), {v,len:len+0} );
    }))
  );

});

describe('GraphBinary.BytecodeSerializer', () => {

  describe('canBeUsedFor', () =>
    it.skip('')
  );

  describe('serialize', () =>
    [
      { v: undefined, fq: true,  e: [0x15, 0x01] },
      { v: undefined, fq: false, e: [            0x00,0x00,0x00,0x00, 0x00,0x00,0x00,0x00] },
      { v: null,      fq: true,  e: [0x15, 0x01] },
      { v: null,      fq: false, e: [            0x00,0x00,0x00,0x00, 0x00,0x00,0x00,0x00] },
      // the following will be automatically tested for fq=false/true
      { v: g(),
        e: [
          0x00,0x00,0x00,0x00, // {steps_length}
          0x00,0x00,0x00,0x00, // {sources_length}
        ]},
      { v: g().V(),
        e: [
          0x00,0x00,0x00,0x01, // {steps_length}
            0x00,0x00,0x00,0x01, 0x56, // V
            0x00,0x00,0x00,0x00, // (void)
          0x00,0x00,0x00,0x00, // {sources_length}
        ]},
      { v: g().V().hasLabel('Person').has('age', 42),
        e: [
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
      // TODO: add sources related tests
    ].forEach(({ v, fq, e }, i) => it(`should be able to handle value of case #${i}`, () => {
      if (fq !== undefined) {
        assert.deepEqual( BytecodeSerializer.serialize(v, fq), Buffer.from(e) );
        return;
      }
      const bytecode = v.getBytecode();
      assert.deepEqual( BytecodeSerializer.serialize(bytecode, false), Buffer.from(e) );
      assert.deepEqual( BytecodeSerializer.serialize(bytecode, true), Buffer.concat([Buffer.from([0x15, 0x00]), Buffer.from(e)]) );
    }))
  );

  describe('deserialize', () =>
    it.skip('')
  );

});

describe('GraphBinary.Writer', () => {

  describe('writeRequest', () =>
    [
      {
        r: {
          requestId: '00010203-0405-0607-0809-0A0b0c0D0e0F',
          op: 'bytecode',
          processor: 'traversal',
          args: { aliases: {g:'g'}, gremlin: g().V().getBytecode() },
        },
        e: [
          // {version}
          0x81,
          // {request_id} UUID
          0x00,0x01,0x02,0x03,0x04,0x05,0x06,0x07,0x08,0x09,0x0A,0x0B,0x0C,0x0D,0x0E,0x0F,
          // {op} String
          0x00,0x00,0x00,0x08,  ...Buffer.from('bytecode'),
          // {processor} String
          0x00,0x00,0x00,0x09,  ...Buffer.from('traversal'),
          // {args} Map
          0x00,0x00,0x00,0x02,
            // 1.
            // args.aliases key String
            0x03,0x00,  0x00,0x00,0x00,0x07,  ...Buffer.from('aliases'),
            // args.aliases value Map
            0x0A,0x00,  0x00,0x00,0x00,0x01,
              // aliases.g key String
              0x03,0x00,  0x00,0x00,0x00,0x01,  ...Buffer.from('g'),
              // aliases.g value String
              0x03,0x00,  0x00,0x00,0x00,0x01,  ...Buffer.from('g'),
            // 2.
            // args.gremlin key String
            0x03,0x00,  0x00,0x00,0x00,0x07,  ...Buffer.from('gremlin'),
            // args.gremlin value Bytecode
            0x15,0x00,
              // {steps_length}
              0x00,0x00,0x00,0x01,
                // step 1 - {name} String
                0x00,0x00,0x00,0x01,  ...Buffer.from('V'),
                // step 1 - {values_length} Int
                0x00,0x00,0x00,0x00,
              // {sources_length}
              0x00,0x00,0x00,0x00,
        ],
      },
    ].forEach(({ r, e }, i) => it(`should be able to handle request of case #${i}`, () => assert.deepEqual(
      new GraphBinaryWriter().writeRequest(r),
      Buffer.from(e),
    )))
  );

});

describe('GraphBinary.Reader', () => {

  describe('readResponse', () => {

    [ undefined, null ].forEach(buffer =>
      it(`should error if buffer is '${buffer}'`, () => assert.throws(
        () => new GraphBinaryReader().readResponse(buffer),
        { message: /Buffer is missing/ },
      ))
    );

    [ '', -1, 0, 42, 3.14, NaN, Infinity, -Infinity, Symbol('') ].forEach((buffer, i) =>
      it(`should error if it is not a Buffer, case #${i}`, () => assert.throws(
        () => new GraphBinaryReader().readResponse(buffer),
        { message: /Not an instance of Buffer/ },
      ))
    );

    it('should error if buffer is empty', () => assert.throws(
      () => new GraphBinaryReader().readResponse(Buffer.from([])),
      { message: /Buffer is empty/ },
    ));

    [
      [ 0x00 ],
      [ 0x00, 0x81 ],
      [ 0x01 ],
      [ 0x80, 0x81 ],
      [ 0x82, 0x81 ],
      [ 0xFF, 0x00, 0x81 ],
      [ 0x00, 0x81 ],
      [ 0x00, 0x00, 0x81 ],
      [ 0x00, 0x00, 0x00, 0x81 ],
    ].forEach((b, i) => it(`should error if version is incorrect, case #${i}`, () => assert.throws(
      () => new GraphBinaryReader().readResponse(Buffer.from(b)),
      { message: /Unsupported version/ },
    )));

  });

});
