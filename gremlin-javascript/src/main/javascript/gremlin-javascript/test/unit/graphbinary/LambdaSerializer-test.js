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
const { lambdaSerializer } = require('../../../lib/structure/io/binary/GraphBinary');
const t = require('../../../lib/process/traversal');
const g = require('../../../lib/structure/graph');

const { from, concat } = Buffer;

describe('GraphBinary.LambdaSerializer', () => {

  const type_code =  from([0x1D]);
  const value_flag = from([0x00]);

  const cases = [
    { v:undefined, fq:1, b:[0x1D,0x01], },
    { v:undefined, fq:0, b:[0x00,0x00,0x00,0x00, 0x00,0x00,0x00,0x00, 0x00,0x00,0x00,0x00] },
    { v:null,      fq:1, b:[0x1D,0x01] },
    { v:null,      fq:0, b:[0x00,0x00,0x00,0x00, 0x00,0x00,0x00,0x00, 0x00,0x00,0x00,0x00] },

    { v:function() { return 'Script_1'; },
      b:[
        0x00,0x00,0x00,0x0E, ...from('gremlin-groovy'),
        0x00,0x00,0x00,0x08, ...from('Script_1'),
        0xFF,0xFF,0xFF,0xFF,
      ]
    },

    { v:function() { return [ 'Script_2', 'Scala' ]; },
      b:[
        0x00,0x00,0x00,0x05, ...from('Scala'),
        0x00,0x00,0x00,0x08, ...from('Script_2'),
        0xFF,0xFF,0xFF,0xFF,
      ]
    },
  ];

  describe('#serialize', () =>
    cases
    .forEach(({ v, fq, b }, i) => it(utils.ser_title({i,v}), () => {
      b = from(b);

      // when fq is under control
      if (fq !== undefined) {
        assert.deepEqual( lambdaSerializer.serialize(v, fq), b );
        return;
      }

      // generic case
      assert.deepEqual( lambdaSerializer.serialize(v, true),  concat([type_code, value_flag, b]) );
      assert.deepEqual( lambdaSerializer.serialize(v, false), concat([                       b]) );
    }))
  );

  describe('#canBeUsedFor', () =>
    // most of the cases are implicitly tested via AnySerializer.serialize() tests
    [
      { v: null,              e: false },
      { v: undefined,         e: false },
      { v: {},                e: false },
      { v: new t.Traverser(), e: false },
      { v: new t.P(),         e: false },
      { v: [],                e: false },
      { v: [0],               e: false },
      { v: [function(){}],    e: false },
      { v: function(){},      e: true  },
    ].forEach(({ v, e }, i) => it(utils.cbuf_title({i,v}), () =>
      assert.strictEqual( lambdaSerializer.canBeUsedFor(v), e )
    ))
  );

});
