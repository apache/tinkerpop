/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

import { Vertex, Edge, Property, VertexProperty, Path } from '../../../lib/structure/graph.js';
import { direction, t } from '../../../lib/process/traversal.js';

/*
 * Unsupported types (no map entries, .gbin files exist but types not implemented):
 * - single-byte-char, multi-byte-char (Char 0x80 not implemented)
 * - traversal-tree (Tree type not implemented)
 * - tinker-graph (Graph type not implemented)
 * - pos-bigdecimal, neg-bigdecimal (BigDecimal type not implemented)
 * - forever-duration, zero-duration (Duration type not implemented)
 */

const model = {};

// BigInteger values
model['pos-biginteger'] = BigInt('123456789987654321123456789987654321');
model['neg-biginteger'] = BigInt('-123456789987654321123456789987654321');

// Byte values (JS has no byte type, deserializes to Number)
model['min-byte'] = -128;
model['max-byte'] = 127;

// Binary values
model['empty-binary'] = Buffer.from('', 'utf-8');
model['str-binary'] = Buffer.from('some bytes for you', 'utf-8');

// Double values
model['max-double'] = 1.7976931348623157e+308;
model['min-double'] = 5e-324;
model['neg-max-double'] = -1.7976931348623157e+308;
model['neg-min-double'] = -5e-324;
model['nan-double'] = NaN;
model['pos-inf-double'] = Infinity;
model['neg-inf-double'] = -Infinity;
model['neg-zero-double'] = -0;

// Float values (JS has no float type, deserializes to Number with IEEE 754 double representation)
model['max-float'] = 3.4028234663852886e+38;
model['min-float'] = 1.401298464324817e-45;
model['neg-max-float'] = -3.4028234663852886e+38;
model['neg-min-float'] = -1.401298464324817e-45;
model['nan-float'] = NaN;
model['pos-inf-float'] = Infinity;
model['neg-inf-float'] = -Infinity;
model['neg-zero-float'] = -0;

// Null
model['unspecified-null'] = null;

// Boolean values
model['true-boolean'] = true;
model['false-boolean'] = false;

// String values
model['single-byte-string'] = 'abc';
model['mixed-string'] = 'abc\u0391\u0392\u0393';

// List values
model['var-bulklist'] = ['marko', 'josh', 'josh'];
model['empty-bulklist'] = [];
model['var-type-list'] = [1, 'person', true, null];
model['empty-list'] = [];

// Edge values
model['traversal-edge'] = new Edge(
  13,
  new Vertex(1, 'person'),    // outV (first in JS constructor)
  'develops',
  new Vertex(10, 'software'), // inV
  [new Property('since', 2009)]
);
model['no-prop-edge'] = new Edge(
  13,
  new Vertex(1, 'person'),
  'develops',
  new Vertex(10, 'software')
);

// Integer values
model['max-int'] = 2147483647;
model['min-int'] = -2147483648;

// Long values (lose precision beyond 2^53 in JS)
model['max-long'] = 9223372036854776000;  // Number, not BigInt
model['min-long'] = -9223372036854776000;

// Map values
const dateKey = new Date(Date.UTC(1970, 0, 1, 0, 24, 41, 295)); // 1481295 ms
model['var-type-map'] = new Map([
  [null, null],
  [[1, 2, 3], dateKey],
  [dateKey, 'red'],
  ['test', 123]
]);
model['empty-map'] = new Map();

// Path values
model['traversal-path'] = new Path(
  [new Set(), new Set(), new Set()],
  [new Vertex(1, 'person'), new Vertex(10, 'software'), new Vertex(11, 'software')]
);
model['empty-path'] = new Path([], []);

// Complex path with nested properties
const propPathVertex = new Vertex(1, 'person', [
  new VertexProperty(0, 'name', 'marko'),
  new VertexProperty(6, 'location', 'san diego', [
    new Property('startTime', 1997),
    new Property('endTime', 2001)
  ]),
  new VertexProperty(7, 'location', 'santa cruz', [
    new Property('startTime', 2001),
    new Property('endTime', 2004)
  ]),
  new VertexProperty(8, 'location', 'brussels', [
    new Property('startTime', 2004),
    new Property('endTime', 2005)
  ]),
  new VertexProperty(9, 'location', 'santa fe', [
    new Property('startTime', 2005)
  ])
]);
const propPathVertex2 = new Vertex(10, 'software', [
  new VertexProperty(4, 'name', 'gremlin')
]);
const propPathVertex3 = new Vertex(11, 'software', [
  new VertexProperty(5, 'name', 'tinkergraph')
]);
model['prop-path'] = new Path(
  [new Set(), new Set(), new Set()],
  [propPathVertex, propPathVertex2, propPathVertex3]
);

// Property values (no parent param in JS)
model['edge-property'] = new Property('since', 2009);
model['null-property'] = new Property('', null);

// Set values
model['var-type-set'] = new Set([2, 'person', true, null]);
model['empty-set'] = new Set();

// Short values (JS has no short type, deserializes to Number)
model['max-short'] = 32767;
model['min-short'] = -32768;

// UUID values (plain strings in JS)
model['specified-uuid'] = '41d2e28a-20a4-4ab0-b379-d810dede3786';
model['nil-uuid'] = '00000000-0000-0000-0000-000000000000';

// Vertex values
model['no-prop-vertex'] = new Vertex(1, 'person');

// VertexProperty values (no parent param, key equals label)
model['traversal-vertexproperty'] = new VertexProperty(0, 'name', 'marko');
model['meta-vertexproperty'] = new VertexProperty(1, 'person', 'stephen', [new Property('a', 'b')]);
model['set-cardinality-vertexproperty'] = new VertexProperty(1, 'person', new Set(['stephen', 'marko']), [new Property('a', 'b')]);

// Enum values
model['id-t'] = t.id;
model['out-direction'] = direction.out;

// Complex vertex with properties (from .gbin deserialization structure)
const name = new VertexProperty(0, 'name', 'marko');
const sanDiego = new VertexProperty(6, 'location', 'san diego', [
  new Property('startTime', 1997),
  new Property('endTime', 2001)
]);
const santaCruz = new VertexProperty(7, 'location', 'santa cruz', [
  new Property('startTime', 2001),
  new Property('endTime', 2004)
]);
const brussels = new VertexProperty(8, 'location', 'brussels', [
  new Property('startTime', 2004),
  new Property('endTime', 2005)
]);
const santaFe = new VertexProperty(9, 'location', 'santa fe', [
  new Property('startTime', 2005)
]);
model['traversal-vertex'] = new Vertex(1, 'person', [name, sanDiego, santaCruz, brussels, santaFe]);

// DateTime values (invalid dates for overflow cases)
model['max-offsetdatetime'] = new Date(NaN);  // Year 999999999 overflows JS Date
model['min-offsetdatetime'] = new Date(NaN);  // Year -999999999 overflows JS Date

export { model };