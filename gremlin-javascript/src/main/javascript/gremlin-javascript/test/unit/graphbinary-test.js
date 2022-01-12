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
const {
  IntSerializer,
  StringSerializer,
} = require('../../lib/structure/io/binary/type-serializers');

describe('GraphBinary.IntSerializer', () => {

  describe('canBeUsedFor', () => {
    it.skip('');
  });

  describe('serialize', () => {
    [
      { v: undefined,   fq: true,  e: [0x01,0x01] },
      { v: undefined,   fq: false, e: [           0x00,0x00,0x00,0x00] },
      { v: null,        fq: true,  e: [0x01,0x01] },
      { v: null,        fq: false, e: [           0x00,0x00,0x00,0x00] },
      { v: 0,           fq: true,  e: [0x01,0x00, 0x00,0x00,0x00,0x00] },
      { v: 0,           fq: false, e: [           0x00,0x00,0x00,0x00] },
      { v: 1,           fq: true,  e: [0x01,0x00, 0x00,0x00,0x00,0x01] },
      { v: 1,           fq: false, e: [           0x00,0x00,0x00,0x01] },
      { v: 256,         fq: true,  e: [0x01,0x00, 0x00,0x00,0x01,0x00] },
      { v: 256,         fq: false, e: [           0x00,0x00,0x01,0x00] },
      { v: 65536,       fq: true,  e: [0x01,0x00, 0x00,0x01,0x00,0x00] },
      { v: 65536,       fq: false, e: [           0x00,0x01,0x00,0x00] },
      { v: 16777216,    fq: true,  e: [0x01,0x00, 0x01,0x00,0x00,0x00] },
      { v: 16777216,    fq: false, e: [           0x01,0x00,0x00,0x00] },
      { v: 2147483647,  fq: true,  e: [0x01,0x00, 0x7F,0xFF,0xFF,0xFF] },
      { v: 2147483647,  fq: false, e: [           0x7F,0xFF,0xFF,0xFF] },
      { v: -1,          fq: true,  e: [0x01,0x00, 0xFF,0xFF,0xFF,0xFF] },
      { v: -1,          fq: false, e: [           0xFF,0xFF,0xFF,0xFF] },
      { v: -2147483648, fq: true,  e: [0x01,0x00, 0x80,0x00,0x00,0x00] },
      { v: -2147483648, fq: false, e: [           0x80,0x00,0x00,0x00] },
    ].forEach(({ v, fq, e }) => it(`should be able to handle value=${v} with fullyQualifiedFormat=${fq}`, () => assert.deepEqual(
      IntSerializer.serialize(v, fq),
      Buffer.from(e),
    )));
  });

  describe('deserialize', () => {
    it.skip('');
  });

});

describe('GraphBinary.StringSerializer', () => {

  describe('canBeUsedFor', () => {
    [
      { v: 'some string', e: true },
      { v: '', e: true },
      { v: 'Z', e: true },
      { v: 'Україна', e: true },
      { v: true, e: false },
      { v: false, e: false },
      { v: null, e: false },
      { v: undefined, e: false },
      { v: Number.MAX_VALUE, e: false },
      { v: 42, e: false },
      { v: 0, e: false },
      { v: Number.MIN_VALUE, e: false },
      { v: NaN, e: false },
      { v: +Infinity, e: false },
      { v: -Infinity, e: false },
      //{ v: Symbol(''), e: false },
    ].forEach(({ v, e }, i) => it(`should return ${e} if value is '${v}', case #${i}`, () => assert.strictEqual(
      StringSerializer.canBeUsedFor(v),
      e,
    )));
  });

  describe('serialize', () => {
    [
      { v: undefined,   fq: true,  e: [0x03,0x01] },
      { v: undefined,   fq: false, e: [           0x00,0x00,0x00,0x00] },
      { v: null,        fq: true,  e: [0x03,0x01] },
      { v: null,        fq: false, e: [           0x00,0x00,0x00,0x00] },
      { v: '',          fq: true,  e: [0x03,0x00, 0x00,0x00,0x00,0x00] },
      { v: '',          fq: false, e: [           0x00,0x00,0x00,0x00] },
      { v: 'Sun',       fq: true,  e: [0x03,0x00, 0x00,0x00,0x00,0x03, 0x53,0x75,0x6E] },
      { v: 'Sun',       fq: false, e: [           0x00,0x00,0x00,0x03, 0x53,0x75,0x6E] },
      { v: 'ήλιος',     fq: true,  e: [0x03,0x00, 0x00,0x00,0x00,0x0A, 0xCE,0xAE, 0xCE,0xBB, 0xCE,0xB9, 0xCE,0xBF, 0xCF,0x82] },
      { v: 'ήλιος',     fq: false, e: [           0x00,0x00,0x00,0x0A, 0xCE,0xAE, 0xCE,0xBB, 0xCE,0xB9, 0xCE,0xBF, 0xCF,0x82] },
    ].forEach(({ v, fq, e }) => it(`should be able to handle value='${v}' with fullyQualifiedFormat=${fq}`, () => assert.deepEqual(
      StringSerializer.serialize(v, fq),
      Buffer.from(e),
    )));
  });

  describe('deserialize', () => {
    it.skip('');
  });

});
