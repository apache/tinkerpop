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

/*
 * GraphBinaryWriter v4 request format tests.
 * Tests the writer's ability to generate v4 request format:
 * {version:0x81}{fields:Map bare}{gremlin:String bare}
 */

import { assert } from 'chai';
import { Buffer } from 'buffer';
import GraphBinaryWriter from '../../../lib/structure/io/binary/internals/GraphBinaryWriter.js';
import ioc from '../../../lib/structure/io/binary/GraphBinary.js';

describe('GraphBinaryWriter', () => {
  const writer = new GraphBinaryWriter(ioc);

  describe('version byte', () => {
    it('first byte is 0x81', () => {
      const result = writer.writeRequest({ gremlin: 'g.V()', fields: new Map() });
      assert.equal(result[0], 0x81);
    });
  });

  describe('empty fields + gremlin', () => {
    it('empty map + bare string', () => {
      const result = writer.writeRequest({ gremlin: 'g.V()', fields: new Map() });
      const expected = Buffer.from([
        0x81, // version
        0x00, 0x00, 0x00, 0x00, // empty map bare (length=0)
        0x00, 0x00, 0x00, 0x05, // string length=5
        0x67, 0x2E, 0x56, 0x28, 0x29 // "g.V()"
      ]);
      assert.deepEqual(result, expected);
    });
  });

  describe('fields with entries', () => {
    it('map with evaluationTimeout entry', () => {
      const fields = new Map();
      fields.set('evaluationTimeout', 1000);
      const result = writer.writeRequest({ gremlin: 'g.V()', fields });
      const expected = Buffer.from([
        0x81, // version
        0x00, 0x00, 0x00, 0x01, // map length=1
        0x03, 0x00, // key type_code=STRING, value_flag=0x00
        0x00, 0x00, 0x00, 0x11, // key string length=17
        0x65, 0x76, 0x61, 0x6C, 0x75, 0x61, 0x74, 0x69, 0x6F, 0x6E, 0x54, 0x69, 0x6D, 0x65, 0x6F, 0x75, 0x74, // "evaluationTimeout"
        0x01, 0x00, // value type_code=INT, value_flag=0x00
        0x00, 0x00, 0x03, 0xE8, // value int=1000
        0x00, 0x00, 0x00, 0x05, // gremlin string length=5
        0x67, 0x2E, 0x56, 0x28, 0x29 // "g.V()"
      ]);
      assert.deepEqual(result, expected);
    });
  });

  describe('default fields', () => {
    it('undefined fields defaults to empty map', () => {
      const result = writer.writeRequest({ gremlin: 'g.V()', fields: undefined });
      const expected = Buffer.from([
        0x81, // version
        0x00, 0x00, 0x00, 0x00, // empty map bare (length=0)
        0x00, 0x00, 0x00, 0x05, // string length=5
        0x67, 0x2E, 0x56, 0x28, 0x29 // "g.V()"
      ]);
      assert.deepEqual(result, expected);
    });

    it('null fields defaults to empty map', () => {
      const result = writer.writeRequest({ gremlin: 'g.V()', fields: null });
      const expected = Buffer.from([
        0x81, // version
        0x00, 0x00, 0x00, 0x00, // empty map bare (length=0)
        0x00, 0x00, 0x00, 0x05, // string length=5
        0x67, 0x2E, 0x56, 0x28, 0x29 // "g.V()"
      ]);
      assert.deepEqual(result, expected);
    });
  });

  describe('gremlin as bare string', () => {
    it('no type_code/value_flag prefix on gremlin', () => {
      const result = writer.writeRequest({ gremlin: 'g.V().count()', fields: new Map() });
      // Verify gremlin portion starts with length, not type_code
      const gremlinStart = 5; // after version + empty map
      assert.equal(result[gremlinStart], 0x00); // length byte 1
      assert.equal(result[gremlinStart + 1], 0x00); // length byte 2
      assert.equal(result[gremlinStart + 2], 0x00); // length byte 3
      assert.equal(result[gremlinStart + 3], 0x0D); // length=13
      assert.equal(result[gremlinStart + 4], 0x67); // 'g'
    });
  });

  describe('empty gremlin', () => {
    it('empty string has length 0', () => {
      const result = writer.writeRequest({ gremlin: '', fields: new Map() });
      const expected = Buffer.from([
        0x81, // version
        0x00, 0x00, 0x00, 0x00, // empty map bare (length=0)
        0x00, 0x00, 0x00, 0x00 // empty string bare (length=0)
      ]);
      assert.deepEqual(result, expected);
    });
  });
});
