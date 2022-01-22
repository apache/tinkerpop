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
const { bytecodeSerializer } = require('../../../lib/structure/io/binary/GraphBinary');

const Bytecode = require('../../../lib/process/bytecode');
const { GraphTraversal } = require('../../../lib/process/graph-traversal');
const g = () => new GraphTraversal(undefined, undefined, new Bytecode());

const { from, concat } = Buffer;

describe('GraphBinary.BytecodeSerializer', () => {

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
        assert.deepEqual( bytecodeSerializer.serialize(v, fq), Buffer.from(e) );
        return;
      }
      const bytecode = v.getBytecode();
      assert.deepEqual( bytecodeSerializer.serialize(bytecode, false), Buffer.from(e) );
      assert.deepEqual( bytecodeSerializer.serialize(bytecode, true), Buffer.concat([Buffer.from([0x15, 0x00]), Buffer.from(e)]) );
    }))
  );

  describe('deserialize', () =>
    it.skip('')
  );

  describe('canBeUsedFor', () =>
    it.skip('')
  );

});

