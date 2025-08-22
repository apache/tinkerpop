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
const { pathSerializer } = require('../../../lib/structure/io/binary/GraphBinary');
const t = require('../../../lib/process/traversal');
const g = require('../../../lib/structure/graph');

const { from, concat } = Buffer;

describe('GraphBinary.PathSerializer', () => {

  const type_code =  from([0x0E]);
  const value_flag = from([0x00]);

  const cases = [
    { v:undefined, fq:1, b:[0x0E,0x01],                                av:null },
    { v:undefined, fq:0, b:[0x09,0x00,0x00,0x00,0x00,0x00, 0x09,0x00,0x00,0x00,0x00,0x00], av:new g.Path([],[]) },
    { v:null,      fq:1, b:[0x0E,0x01] },
    { v:null,      fq:0, b:[0x09,0x00,0x00,0x00,0x00,0x00, 0x09,0x00,0x00,0x00,0x00,0x00], av:new g.Path([],[]) },

    { v:new g.Path([ ['A','B'], ['C','D'] ], [ 1,-1 ]),
      b:[
        // {labels}
        0x09,0x00,0x00,0x00,0x00,0x02, // List.{length}
          // ['A','B']
          0x09,0x00, 0x00,0x00,0x00,0x02,
          0x03,0x00, 0x00,0x00,0x00,0x01, 0x41,
          0x03,0x00, 0x00,0x00,0x00,0x01, 0x42,
          // ['C','D']
          0x09,0x00, 0x00,0x00,0x00,0x02,
          0x03,0x00, 0x00,0x00,0x00,0x01, 0x43,
          0x03,0x00, 0x00,0x00,0x00,0x01, 0x44,
        // {objects}
        0x09,0x00,0x00,0x00,0x00,0x02, // List.{length}
          0x01,0x00, 0x00,0x00,0x00,0x01,
          0x01,0x00, 0xFF,0xFF,0xFF,0xFF,
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
    { des:1, err:/unexpected {type_code}/,  fq:1, b:[0x0D] },
    { des:1, err:/unexpected {type_code}/,  fq:1, b:[0x0F] },
    { des:1, err:/unexpected {type_code}/,  fq:1, b:[0xE0] },
    { des:1, err:/unexpected {type_code}/,  fq:1, b:[0x1E] },
    { des:1, err:/unexpected {type_code}/,  fq:1, b:[0xFF] },

    { des:1, err:/{value_flag} is missing/, fq:1, b:[0x0E] },
    { des:1, err:/unexpected {value_flag}/, fq:1, b:[0x0E,0x10] },
    { des:1, err:/unexpected {value_flag}/, fq:1, b:[0x0E,0x02] },
    { des:1, err:/unexpected {value_flag}/, fq:1, b:[0x0E,0x0F] },
    { des:1, err:/unexpected {value_flag}/, fq:1, b:[0x0E,0xFF] },
  ];

  describe('#serialize', () =>
    cases
    .filter(({des}) => !des)
    .forEach(({ v, fq, b }, i) => it(utils.ser_title({i,v}), () => {
      b = from(b);

      // when fq is under control
      if (fq !== undefined) {
        assert.deepEqual( pathSerializer.serialize(v, fq), b );
        return;
      }

      // generic case
      assert.deepEqual( pathSerializer.serialize(v, true),  concat([type_code, value_flag, b]) );
      assert.deepEqual( pathSerializer.serialize(v, false), concat([                       b]) );
    }))
  );

  describe('#deserialize', () =>
    cases.forEach(({ v, fq, b, av, err }, i) => it(utils.des_title({i,b}), () => {
      if (Array.isArray(b))
        b = from(b);

      // wrong binary
      if (err !== undefined) {
        if (fq !== undefined)
          assert.throws(() => pathSerializer.deserialize(b, fq), { message: err });
        else {
          assert.throws(() => pathSerializer.deserialize(concat([type_code, value_flag, b]), true),  { message: err });
          assert.throws(() => pathSerializer.deserialize(concat([                       b]), false), { message: err });
        }
        return;
      }

      if (av !== undefined)
        v = av;
      const len = b.length;

      // when fq is under control
      if (fq !== undefined) {
        assert.deepStrictEqual( pathSerializer.deserialize(b, fq), {v,len} );
        return;
      }

      // generic case
      assert.deepStrictEqual( pathSerializer.deserialize(concat([type_code, value_flag, b]), true),  {v,len:len+2} );
      assert.deepStrictEqual( pathSerializer.deserialize(concat([                       b]), false), {v,len:len+0} );
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
      { v: [new g.Path()],    e: false },
      { v: new g.Path(),      e: true  },
    ].forEach(({ v, e }, i) => it(utils.cbuf_title({i,v}), () =>
      assert.strictEqual( pathSerializer.canBeUsedFor(v), e )
    ))
  );

  // Additional complex path tests with Vertex and Edge objects
  describe('complex V-E-V path round-trip', () => {
    function buildVertex(id, label, propsObj) {
      if (!propsObj) return new g.Vertex(id, label, undefined);
      // build array of VertexProperty entries as expected by Vertex
      const vps = Object.keys(propsObj).map((k, idx) => new g.VertexProperty(idx + 1, k, propsObj[k], undefined));
      return new g.Vertex(id, label, vps);
    }

    function buildEdge(id, outV, label, inV, propsObj) {
      // Edge expects properties as a map of key -> g.Property (the constructor extracts .value)
      let props = undefined;
      if (propsObj) {
        props = {};
        for (const k of Object.keys(propsObj)) props[k] = new g.Property(k, propsObj[k]);
      }
      return new g.Edge(id, outV, label, inV, props);
    }

    it('should serialize/deserialize Path with V-E-V that includes properties', () => {
      const vOut = buildVertex(1, 'person', { name: 'marko', age: 29 });
      const vIn = buildVertex(3, 'software', { name: 'lop', lang: 'java' });
      const e = buildEdge(9, vOut, 'created', vIn, { weight: 0.4 });

      const p = new g.Path([[ 'a' ], [ 'b' ], [ 'c' ]], [ vOut, e, vIn ]);

      // round-trip with non-fully-qualified format
      const buf = pathSerializer.serialize(p, false);
      const { v: deser, len } = pathSerializer.deserialize(buf, false);

      // sanity on buffer consumed
      assert.strictEqual(len, buf.length);

      // assert labels
      assert.strictEqual(deser.labels.length, 3);
      assert.strictEqual(deser.labels[0].length, 1);
      assert.strictEqual(deser.labels[1].length, 1);
      assert.strictEqual(deser.labels[2].length, 1);

      // assert objects and their types
      assert.strictEqual(deser.objects.length, 3);
      const a = deser.objects[0];
      const b = deser.objects[1];
      const c = deser.objects[2];
      assert.ok(a instanceof g.Vertex);
      assert.ok(b instanceof g.Edge);
      assert.ok(c instanceof g.Vertex);

      // Vertex properties should be present (VertexSerializer preserves them)
      assert.ok(Array.isArray(a.properties));
      assert.ok(a.properties.length > 0);
      assert.ok(Array.isArray(c.properties));
      assert.ok(c.properties.length > 0);

      // Edge properties are not serialized in GraphBinary (EdgeSerializer writes null), expect empty object
      assert.ok(b.properties);
      assert.strictEqual(Object.keys(b.properties).length, 0);
    });

    it('should serialize/deserialize Path with V-E-V without properties', () => {
      const vOut = buildVertex(1, 'person', undefined);
      const vIn = buildVertex(3, 'software', undefined);
      const e = buildEdge(9, vOut, 'created', vIn, undefined);

      const p = new g.Path([[ 'a' ], [ 'b' ], [ 'c' ]], [ vOut, e, vIn ]);

      // round-trip with fully-qualified format
      const fqBuf = pathSerializer.serialize(p, true);
      const { v: deser, len } = pathSerializer.deserialize(fqBuf, true);

      assert.strictEqual(len, fqBuf.length);

      // types
      const a = deser.objects[0];
      const b = deser.objects[1];
      const c = deser.objects[2];
      assert.ok(a instanceof g.Vertex);
      assert.ok(b instanceof g.Edge);
      assert.ok(c instanceof g.Vertex);

      // Vertex properties default to [] when null/undefined
      assert.ok(Array.isArray(a.properties));
      assert.strictEqual(a.properties.length, 0);
      assert.ok(Array.isArray(c.properties));
      assert.strictEqual(c.properties.length, 0);

      // Edge properties default to {} when null/undefined
      assert.ok(b.properties);
      assert.strictEqual(Object.keys(b.properties).length, 0);
    });
  });

});
