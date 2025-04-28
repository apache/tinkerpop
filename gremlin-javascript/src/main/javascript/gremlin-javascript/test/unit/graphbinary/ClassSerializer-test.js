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

const utils = require('./utils');
const assert = require('assert');
const { classSerializer } = require('../../../lib/structure/io/binary/GraphBinary');
const t = require('../../../lib/process/traversal');
const ts = require('../../../lib/process/traversal-strategy');

const { from, concat } = Buffer;

describe('GraphBinary.ClassSerializer', () => {

    const type_code =  from([0x06]);
    const value_flag = from([0x00]);

    const cases = [
        { v:undefined, fq:1, b:[0x06,0x01], },
        { v:undefined, fq:0, b:[0x00,0x00,0x00,0x00] },
        { v:null,      fq:1, b:[0x06,0x01] },
        { v:null,      fq:0, b:[0x00,0x00,0x00,0x00] },

        { v:ts.ElementIdStrategy,
            b:[
                0x00,0x00,0x00,0x54, ...from('org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ElementIdStrategy'),
            ]
        },

        { v:ts.OptionsStrategy,
            b:[
                0x00,0x00,0x00,0x52, ...from('org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy'),
            ]
        },
    ];

    describe('#serialize', () =>
        cases
            .forEach(({ v, fq, b }, i) => it(utils.ser_title({i,v}), () => {
                b = from(b);

                // when fq is under control
                if (fq !== undefined) {
                    assert.deepEqual( classSerializer.serialize(v, fq), b );
                    return;
                }

                // generic case
                assert.deepEqual( classSerializer.serialize(v, true),  concat([type_code, value_flag, b]) );
                assert.deepEqual( classSerializer.serialize(v, false), concat([                       b]) );
            }))
    );

    describe('#canBeUsedFor', () =>
        // most of the cases are implicitly tested via AnySerializer.serialize() tests
        [
            { v: null,                       e: false },
            { v: undefined,                  e: false },
            { v: {},                         e: false },
            { v: new t.Traverser(),          e: false },
            { v: new t.P(),                  e: false },
            { v: [],                         e: false },
            { v: [0],                        e: false },
            { v: [function(){}],             e: false },
            { v: function(){},               e: false },
            { v: ts.TraversalStrategy,       e: true  },
        ].forEach(({ v, e }, i) => it(utils.cbuf_title({i,v}), () =>
            assert.strictEqual(classSerializer.canBeUsedFor(v), e )
        ))
    );

});
