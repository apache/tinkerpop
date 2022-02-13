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
const { DataType, anySerializer } = require('../../../lib/structure/io/binary/GraphBinary');

const { Vertex, VertexProperty, Edge, Path, Property } = require('../../../lib/structure/graph');
const t = require('../../../lib/process/traversal');
const Bytecode = require('../../../lib/process/bytecode');
const { GraphTraversal } = require('../../../lib/process/graph-traversal');
const g = () => new GraphTraversal(undefined, undefined, new Bytecode());

const { from, concat } = Buffer;

describe('GraphBinary.AnySerializer', () => {

  describe('#serialize', () => {
    [
      /*
        This is the most complex part of GraphBinary,
        we are expected to guess right serializer, i.e. right type.

        Let's follow existing structure/io/graph-serializer.GraphSON3Writer.adaptObject() logic
        of serializer/type selection and add the rest of the types.

        This test suite is the documentation and set of examples of type inference.
      */

      // NumberSerializer
      // Double, TODO: Float is never used during serialization, still okay here?
      { v:0.1,         b:[DataType.DOUBLE,0x00, 0x3F,0xB9,0x99,0x99,0x99,0x99,0x99,0x9A] },
      { v:0.375,       b:[DataType.DOUBLE,0x00, 0x3F,0xD8,0x00,0x00,0x00,0x00,0x00,0x00] },
      { v:0.00390625,  b:[DataType.DOUBLE,0x00, 0x3F,0x70,0x00,0x00,0x00,0x00,0x00,0x00] },
      { v:Infinity,    b:[DataType.DOUBLE,0x00, 0x7F,0xF0,0x00,0x00,0x00,0x00,0x00,0x00] },
      { v:-Infinity,   b:[DataType.DOUBLE,0x00, 0xFF,0xF0,0x00,0x00,0x00,0x00,0x00,0x00] },
      { v:NaN,         b:[DataType.DOUBLE,0x00, 0x7F,0xF8,0x00,0x00,0x00,0x00,0x00,0x00] },
      // Int
      { v:0,           b:[DataType.INT,0x00, 0x00,0x00,0x00,0x00] },
      { v:1,           b:[DataType.INT,0x00, 0x00,0x00,0x00,0x01] },
      { v:2147483647,  b:[DataType.INT,0x00, 0x7F,0xFF,0xFF,0xFF] }, // INT32_MAX
      { v:-2147483648, b:[DataType.INT,0x00, 0x80,0x00,0x00,0x00] }, // INT32_MIN
      { v:-1,          b:[DataType.INT,0x00, 0xFF,0xFF,0xFF,0xFF] },
      // TODO: long/int64

      // DateSerializer
      { v:new Date(1651436603000), b:[DataType.DATE,0x00, 0x00,0x00,0x01,0x80,0x81,0x4A,0xC6,0x78] },

      // BytecodeSerializer
      // Traversal type
      { v: g().V(),
        b: [
          DataType.BYTECODE,0x00,
          0x00,0x00,0x00,0x01, // {steps_length}
            0x00,0x00,0x00,0x01, 0x56, // V
            0x00,0x00,0x00,0x00, // (void)
          0x00,0x00,0x00,0x00, // {sources_length}
        ]
      },
      // Bytecode type
      { v: g().V().getBytecode(),
        b: [
          DataType.BYTECODE,0x00,
          0x00,0x00,0x00,0x01, // {steps_length}
            0x00,0x00,0x00,0x01, 0x56, // V
            0x00,0x00,0x00,0x00, // (void)
          0x00,0x00,0x00,0x00, // {sources_length}
        ]
      },

      // TraverserSerializer
      { v: new t.Traverser("A1", 16),
        b: [
          DataType.TRAVERSER,0x00,
          0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x10,
          DataType.STRING,0x00, 0x00,0x00,0x00,0x02, 0x41,0x31,
        ]
      },

      // TraversalStrategySerializer
      // TODO

      // PSerializer
      { v: t.P.eq('marko'),
        b: [
          DataType.P,0x00,
          0x00,0x00,0x00,0x02, ...from('eq'),
          0x00,0x00,0x00,0x01,
          0x03,0x00, 0x00,0x00,0x00,0x05, ...from('marko'),
        ]
      },

      // TextPSerializer
      // TODO

      // LambdaSerializer
      { v:function() { return 'Script_1'; },
        b:[
          DataType.LAMBDA,0x00,
          0x00,0x00,0x00,0x0E, ...from('gremlin-groovy'),
          0x00,0x00,0x00,0x08, ...from('Script_1'),
          0xFF,0xFF,0xFF,0xFF,
        ]
      },

      // EnumSerializer (actually represents different enum like types)
      { v: new t.EnumValue('Barrier', 'normSack'),
        b: [ DataType.BARRIER,0x00, DataType.STRING,0x00, 0x00,0x00,0x00,0x08, ...from('normSack') ]
      },
      { v: new t.EnumValue('Cardinality', 'single'),
        b: [ DataType.CARDINALITY,0x00, DataType.STRING,0x00, 0x00,0x00,0x00,0x06, ...from('single') ]
      },
      { v: new t.EnumValue('Column', 'keys'),
        b: [ DataType.COLUMN,0x00, DataType.STRING,0x00, 0x00,0x00,0x00,0x04, ...from('keys') ]
      },
      { v: new t.EnumValue('Direction', 'OUT'),
        b: [ DataType.DIRECTION,0x00, DataType.STRING,0x00, 0x00,0x00,0x00,0x03, ...from('OUT') ]
      },
      { v: new t.EnumValue('Operator', 'addAll'),
        b: [ DataType.OPERATOR,0x00, DataType.STRING,0x00, 0x00,0x00,0x00,0x06, ...from('addAll') ]
      },
      { v: new t.EnumValue('Order', 'desc'),
        b: [ DataType.ORDER,0x00, DataType.STRING,0x00, 0x00,0x00,0x00,0x04, ...from('desc') ]
      },
      { v: new t.EnumValue('Pick', 'any'),
        b: [ DataType.PICK,0x00, DataType.STRING,0x00, 0x00,0x00,0x00,0x03, ...from('any') ]
      },
      { v: new t.EnumValue('Pop', 'first'),
        b: [ DataType.POP,0x00, DataType.STRING,0x00, 0x00,0x00,0x00,0x05, ...from('first') ]
      },
      { v: new t.EnumValue('Scope', 'local'),
        b: [ DataType.SCOPE,0x00, DataType.STRING,0x00, 0x00,0x00,0x00,0x05, ...from('local') ]
      },
      { v: new t.EnumValue('T', 'id'),
        b: [ DataType.T,0x00, DataType.STRING,0x00, 0x00,0x00,0x00,0x02, ...from('id') ]
      },

      // VertexSerializer
      { v: new Vertex('A', 'Person', null),
        b: [
          DataType.VERTEX,0x00,
          DataType.STRING,0x00, 0x00,0x00,0x00,0x01, 0x41, // {id}
          0x00,0x00,0x00,0x06, ...from('Person'), // {label}
          0xFE,0x01, // {properties}
        ]
      },

      // EdgeSerializer
      { v:new Edge('A', new Vertex('B','Person',null), 'has', new Vertex('C','Pet',null), null),
        b:[
          DataType.EDGE,0x00,
          DataType.STRING,0x00, 0x00,0x00,0x00,0x01, 0x41, // id
          0x00,0x00,0x00,0x03, ...from('has'), // label
          DataType.STRING,0x00, 0x00,0x00,0x00,0x01, 0x43, // inVId
          0x00,0x00,0x00,0x03, ...from('Pet'), // inVLabel
          DataType.STRING,0x00, 0x00,0x00,0x00,0x01, 0x42, // outVId
          0x00,0x00,0x00,0x06, ...from('Person'), // outVLabel
          0xFE,0x01, // parent
          0xFE,0x01, // properties
        ]
      },

      // PathSerializer
      { v:new Path([ ['A','B'], ['C','D'] ], [ 1,-1 ]),
        b:[
          DataType.PATH,0x00,
          // {labels}
          DataType.LIST,0x00, 0x00,0x00,0x00,0x02,
            // ['A','B']
            0x09,0x00, 0x00,0x00,0x00,0x02,
            0x03,0x00, 0x00,0x00,0x00,0x01, 0x41,
            0x03,0x00, 0x00,0x00,0x00,0x01, 0x42,
            // ['C','D']
            0x09,0x00, 0x00,0x00,0x00,0x02,
            0x03,0x00, 0x00,0x00,0x00,0x01, 0x43,
            0x03,0x00, 0x00,0x00,0x00,0x01, 0x44,
          // {objects}
          DataType.LIST,0x00, 0x00,0x00,0x00,0x02,
            0x01,0x00, 0x00,0x00,0x00,0x01,
            0x01,0x00, 0xFF,0xFF,0xFF,0xFF,
        ]
      },

      // PropertySerializer
      {
        v:new Property('Key', [ 'value' ]),
        b:[
          DataType.PROPERTY,0x00,
          0x00,0x00,0x00,0x03, ...from('Key'),
          0x09,0x00, 0x00,0x00,0x00,0x01, // list
            0x03,0x00, 0x00,0x00,0x00,0x05, ...from('value'),
          0xFE,0x01, // parent
        ]
      },

      // VertexPropertySerializer
      { v:new VertexProperty('00010203-0405-0607-0809-0a0b0c0d0e0f', 'Label', 42),
        b:[
          0x12,0x00,
          0x03,0x00, 0x00,0x00,0x00,0x24, ...from('00010203-0405-0607-0809-0a0b0c0d0e0f'),
          0x00,0x00,0x00,0x05, ...from('Label'),
          0x01,0x00, 0x00,0x00,0x00,0x2A,
          0xFE,0x01,
          0xFE,0x01,
        ]
      },

      // LongSerializer
      // TODO +

      // ListSerializer (Set is never used during serialization)
      { v:[],  b:[DataType.LIST,0x00, 0x00,0x00,0x00,0x00] },
      { v:[1], b:[DataType.LIST,0x00, 0x00,0x00,0x00,0x01, 0x01,0x00, 0x00,0x00,0x00,0x01] },

      // SetSerializer
      // TODO

      // MapSerializer
      { v: new Map([ ['A',3] ]),
        b: [
          DataType.MAP,0x00,
          0x00,0x00,0x00,0x01, // {length}
          0x03,0x00,0x00,0x00,0x00,0x01,0x41, // A
          0x01,0x00,0x00,0x00,0x00,0x03 // 3
        ]
      },

      // Default (strings / objects / ...)
      // TODO: align with Java.parse(GraphSON) logic
      // TODO: uuid
      // TODO: leftovers

      // BooleanSerializer
      { v:true,  b:[DataType.BOOLEAN,0x00, 0x01] },
      { v:false, b:[DataType.BOOLEAN,0x00, 0x00] },

      // StringSerializer
      { v:'A1', b:[DataType.STRING,0x00, 0x00,0x00,0x00,0x02, 0x41,0x31] },
      { v:'',   b:[DataType.STRING,0x00, 0x00,0x00,0x00,0x00] },

      // Object
      { v: { 'B': 2 },
        b: [
          DataType.MAP,0x00,
          0x00,0x00,0x00,0x01, // {length}
          0x03,0x00,0x00,0x00,0x00,0x01,0x42, // B
          0x01,0x00,0x00,0x00,0x00,0x02 // 2
        ]
      },

      // UnspecifiedNullSerializer
      { v:null,      fq:1, b:[0xFE,0x01] },
      { v:undefined, fq:1, b:[0xFE,0x01] },

    ].forEach(({ v, fq, b }, i) => it(utils.ser_title({i,v}), () => {
      b = from(b);

      // when fq is under control
      if (fq !== undefined) {
        assert.deepEqual( anySerializer.serialize(v, fq), b );
        return;
      }

      // generic case
      assert.deepEqual(anySerializer.serialize(v, true),  b);
      assert.deepEqual(anySerializer.serialize(v, false), b.slice(2));
    }));

    it(`should error if value is unsupported`, () => assert.throws(
      () => anySerializer.serialize( Symbol('sym1') ),
      { message: "No serializer found to support item where typeof(item)='symbol' and String(item)='Symbol(sym1)'." }
    ));

  });

  describe('#deserialize', () =>
    [
      { err:/buffer is missing/,                  b:undefined },
      { err:/buffer is missing/,                  b:undefined },
      { err:/buffer is missing/,                  b:null },
      { err:/buffer is missing/,                  b:null },
      { err:/buffer is empty/,                    b:[] },
      { err:/buffer is empty/,                    b:[] },

      { err:/unknown {type_code}/,                b:[0x2E] },
      { err:/unknown {type_code}/,                b:[0x30] },
      { err:/unknown {type_code}/,                b:[0x8F] },
      { err:/unknown {type_code}/,                b:[0xFF] },

      // this is like a register of supported type deserializers:

      // INT
      { v:null,                                   b:[0x01,0x01] },
      { v:1,                                      b:[0x01,0x00, 0x00,0x00,0x00,0x01] },

      // LONG
      { v:null,                                   b:[0x02,0x01] },
      { v:1n,                                     b:[0x02,0x00, 0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x01] },

      // STRING
      { v:null,                                   b:[0x03,0x01] },
      { v:'Ab0',                                  b:[0x03,0x00, 0x00,0x00,0x00,0x03, 0x41,0x62,0x30] },

      // DATE
      { v:null,                                   b:[0x04,0x01] },
      { v:new Date('1969-12-31T23:59:59.999Z'),   b:[0x04,0x00, 0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF] },

      // TIMESTAMP
      { v:null,                                   b:[0x05,0x01] },
      { v:new Date('1969-12-31T23:59:59.999Z'),   b:[0x05,0x00, 0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF] },

      // DOUBLE
      { v:null,                                   b:[0x07,0x01] },
      { v:0.00390625,                             b:[0x07,0x00, 0x3F,0x70,0x00,0x00,0x00,0x00,0x00,0x00] },

      // FLOAT
      { v:null,                                   b:[0x08,0x01] },
      { v:0.375,                                  b:[0x08,0x00, 0x3E,0xC0,0x00,0x00] },

      // LIST
      { v:null,                                   b:[0x09,0x01] },
      { v:[],                                     b:[0x09,0x00, 0x00,0x00,0x00,0x00] },

      // MAP
      { v:null,                                   b:[0x0A,0x01] },
      { v:new Map(),                              b:[0x0A,0x00, 0x00,0x00,0x00,0x00] },

      // UUID
      { v:null,                                   b:[0x0C,0x01] },
      { v:'00010203-0405-0607-0809-0a0b0c0d0e0f', b:[0x0C,0x00, 0x00,0x01,0x02,0x03,0x04,0x05,0x06,0x07,0x08,0x09,0x0A,0x0B,0x0C,0x0D,0x0E,0x0F] },

      // EDGE
      { v:null,                                   b:[0x0D,0x01] },
      { v:new Edge(1, new Vertex(2,'Person',null), 'has', new Vertex(3,'Pet',null), null),
        b:[
          0x0D,0x00,
          0x01,0x00, 0x00,0x00,0x00,0x01, // id
          0x00,0x00,0x00,0x03, ...from('has'), // label
          0x01,0x00, 0x00,0x00,0x00,0x03, // inVId
          0x00,0x00,0x00,0x03, ...from('Pet'), // inVLabel
          0x01,0x00, 0x00,0x00,0x00,0x02, // outVId
          0x00,0x00,0x00,0x06, ...from('Person'), // outVLabel
          0xFE,0x01, // parent
          0xFE,0x01, // properties
        ]
      },

      // PATH
      { v:null,                                   b:[0x0E,0x01] },
      { v:new Path([ ['B','A'], ['D'] ], [ -1 ]),
        b:[
          0x0E,0x00,
          // {labels}
          0x09,0x00, 0x00,0x00,0x00,0x02,
            // ['B','A']
            0x09,0x00, 0x00,0x00,0x00,0x02,
            0x03,0x00, 0x00,0x00,0x00,0x01, 0x42,
            0x03,0x00, 0x00,0x00,0x00,0x01, 0x41,
            // ['D']
            0x09,0x00, 0x00,0x00,0x00,0x01,
            0x03,0x00, 0x00,0x00,0x00,0x01, 0x44,
          // {objects}
          0x09,0x00, 0x00,0x00,0x00,0x01,
            0x01,0x00, 0xFF,0xFF,0xFF,0xFF,
        ]
      },

      // PROPERTY
      { v:null,                                   b:[0x0F,0x01] },
      {
        v:new Property('Key', [ 'value' ]),
        b:[
          0x0F,0x00,
          0x00,0x00,0x00,0x03, ...from('Key'),
          0x09,0x00, 0x00,0x00,0x00,0x01, // list
            0x03,0x00, 0x00,0x00,0x00,0x05, ...from('value'),
          0xFE,0x01, // parent
        ]
      },

      // VERTEX
      { v:null,                                   b:[0x11,0x01] },
      { v:new Vertex('00010203-0405-0607-0809-0a0b0c0d0e0f', 'A', null),
        b:[0x11,0x00, 0x0C,0x00,0x00,0x01,0x02,0x03,0x04,0x05,0x06,0x07,0x08,0x09,0x0A,0x0B,0x0C,0x0D,0x0E,0x0F, 0x00,0x00,0x00,0x01,0x41, 0xFE,0x01]
      },

      // VERTEXPROPERTY
      { v:null,                                   b:[0x12,0x01] },
      { v:new VertexProperty('00010203-0405-0607-0809-0a0b0c0d0e0f', 'Label', 42, null),
        b:[
          0x12,0x00,
          0x0C,0x00, 0x00,0x01,0x02,0x03,0x04,0x05,0x06,0x07,0x08,0x09,0x0A,0x0B,0x0C,0x0D,0x0E,0x0F,
          0x00,0x00,0x00,0x05, ...from('Label'),
          0x01,0x00, 0x00,0x00,0x00,0x2A,
          0xFE,0x01,
          0xFE,0x01,
        ]
      },

      // BARRIER
      { v:null,                                   b:[0x13,0x01] },
      { v:new t.EnumValue('Barrier','normSack'),  b:[0x13,0x00, 0x03,0x00, 0x00,0x00,0x00,0x08, ...from('normSack')] },

      // BYTECODE
      { v:null,                                   b:[0x15,0x01] },
      { v:new Bytecode(),                         b:[0x15,0x00, 0x00,0x00,0x00,0x00, 0x00,0x00,0x00,0x00] },

      // CARDINALITY
      { v:null,                                   b:[0x16,0x01] },
      { v:new t.EnumValue('Cardinality', 'set'),  b:[0x16,0x00, 0x03,0x00, 0x00,0x00,0x00,0x03, ...from('set')] },

      // COLUMN
      { v:null,                                   b:[0x17,0x01] },
      { v:new t.EnumValue('Column','keys'),       b:[0x17,0x00, 0x03,0x00, 0x00,0x00,0x00,0x04, ...from('keys')] },

      // DIRECTION
      { v:null,                                   b:[0x18,0x01] },
      { v:new t.EnumValue('Direction','OUT'),     b:[0x18,0x00, 0x03,0x00, 0x00,0x00,0x00,0x03, ...from('OUT')] },

      // OPERATOR
      { v:null,                                   b:[0x19,0x01] },
      { v:new t.EnumValue('Operator','addAll'),   b:[0x19,0x00, 0x03,0x00, 0x00,0x00,0x00,0x06, ...from('addAll')] },

      // ORDER
      { v:null,                                   b:[0x1A,0x01] },
      { v:new t.EnumValue('Order','desc'),        b:[0x1A,0x00, 0x03,0x00, 0x00,0x00,0x00,0x04, ...from('desc')] },

      // PICK
      { v:null,                                   b:[0x1B,0x01] },
      { v:new t.EnumValue('Pick','any'),          b:[0x1B,0x00, 0x03,0x00, 0x00,0x00,0x00,0x03, ...from('any')] },

      // POP
      { v:null,                                   b:[0x1C,0x01] },
      { v:new t.EnumValue('Pop','first'),         b:[0x1C,0x00, 0x03,0x00, 0x00,0x00,0x00,0x05, ...from('first')] },

      // LAMBDA
      // TODO: it's not expected to be deserialized, is it correct assumption?

      // P
      { v:t.P.eq(7),                              b:[0x1E,0x00, 0x00,0x00,0x00,0x02, ...from('eq'), 0x00,0x00,0x00,0x01, 0x01,0x00,0x00,0x00,0x00,0x07] },

      // SCOPE
      { v:null,                                   b:[0x1F,0x01] },
      { v:new t.EnumValue('Scope','local'),       b:[0x1F,0x00, 0x03,0x00, 0x00,0x00,0x00,0x05, ...from('local')] },

      // T
      { v:null,                                   b:[0x20,0x01] },
      { v:new t.EnumValue('T','id'),              b:[0x20,0x00, 0x03,0x00, 0x00,0x00,0x00,0x02, ...from('id')] },

      // TRAVERSER
      { v:null,                                   b:[0x21,0x01] },
      { v:new t.Traverser('A', 2n),               b:[0x21,0x00, 0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x02, 0x03,0x00,0x00,0x00,0x00,0x01,0x41] },

      // BOOLEAN
      { v:null,                                   b:[0x27,0x01] },
      { v:true,                                   b:[0x27,0x00, 0x01] },

      // UNSPECIFIED_NULL
      { v:null,                                   b:[0xFE,0x01] },

      // TODO: "register" other types
    ]
    .forEach(({ v, b, err }, i) => it(utils.des_title({i,b}), () => {
      if (Array.isArray(b))
        b = from(b);

      // wrong binary
      if (err !== undefined) {
        assert.throws(() => anySerializer.deserialize(b), { message: err });
        return;
      }

      const len = b.length;
      assert.deepStrictEqual( anySerializer.deserialize(b), {v,len} );
    }))
  );

});
