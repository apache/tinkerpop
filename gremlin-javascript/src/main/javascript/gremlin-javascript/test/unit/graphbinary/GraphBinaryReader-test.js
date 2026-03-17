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
 * GraphBinaryReader v4 response format tests.
 * Tests the reader's ability to parse v4 response format:
 * {version:0x81}{bulked:Byte}{result_data stream}{marker:0xFD 0x00 0x00}{status_code:Int bare}{status_message:nullable}{exception:nullable}
 */

import { assert } from 'chai';
import { Buffer } from 'buffer';
import GraphBinaryReader from '../../../lib/structure/io/binary/internals/GraphBinaryReader.js';
import ioc from '../../../lib/structure/io/binary/GraphBinary.js';

describe('GraphBinaryReader', () => {
  const reader = new GraphBinaryReader(ioc);

  describe('input validation', () => {
    it('undefined buffer throws error', () => {
      assert.throws(() => reader.readResponse(undefined), /Buffer is missing/);
    });

    it('null buffer throws error', () => {
      assert.throws(() => reader.readResponse(null), /Buffer is missing/);
    });

    it('non-Buffer throws error', () => {
      assert.throws(() => reader.readResponse('not a buffer'), /Not an instance of Buffer/);
    });

    it('empty buffer throws error', () => {
      assert.throws(() => reader.readResponse(Buffer.alloc(0)), /Buffer is empty/);
    });
  });

  describe('version validation', () => {
    it('rejects version 0x00', () => {
      const buffer = Buffer.from([0x00]);
      assert.throws(() => reader.readResponse(buffer), /Unsupported version '0'/);
    });

    it('rejects version 0x84', () => {
      const buffer = Buffer.from([0x84]);
      assert.throws(() => reader.readResponse(buffer), /Unsupported version '132'/);
    });

    it('rejects version 0xFF', () => {
      const buffer = Buffer.from([0xFF]);
      assert.throws(() => reader.readResponse(buffer), /Unsupported version '255'/);
    });
  });

  describe('non-bulked responses', () => {
    it('single value', () => {
      const buffer = Buffer.from([
        0x81, // version
        0x00, // bulked=false
        0x01, 0x00, 0x00, 0x00, 0x00, 0x43, // fq Int: type_code=0x01, value_flag=0x00, value=67
        0xFD, 0x00, 0x00, // marker
        0x00, 0x00, 0x00, 0xC8, // status_code=200
        0x01, // status_message null flag
        0x01  // exception null flag
      ]);
      const result = reader.readResponse(buffer);
      assert.deepEqual(result, {
        status: { code: 200, message: null, exception: null },
        result: { data: [67], bulked: false },
      });
    });

    it('multiple values', () => {
      const buffer = Buffer.from([
        0x81, // version
        0x00, // bulked=false
        0x01, 0x00, 0x00, 0x00, 0x00, 0x43, // fq Int: 67
        0x03, 0x00, 0x00, 0x00, 0x00, 0x05, 0x68, 0x65, 0x6C, 0x6C, 0x6F, // fq String: "hello"
        0xFD, 0x00, 0x00, // marker
        0x00, 0x00, 0x00, 0xC8, // status_code=200
        0x01, // status_message null
        0x01  // exception null
      ]);
      const result = reader.readResponse(buffer);
      assert.deepEqual(result, {
        status: { code: 200, message: null, exception: null },
        result: { data: [67, 'hello'], bulked: false }
      });
    });

    it('empty result', () => {
      const buffer = Buffer.from([
        0x81, // version
        0x00, // bulked=false
        0xFD, 0x00, 0x00, // marker (no data)
        0x00, 0x00, 0x00, 0xCC, // status_code=204
        0x01, // status_message null
        0x01  // exception null
      ]);
      const result = reader.readResponse(buffer);
      assert.deepEqual(result, {
        status: { code: 204, message: null, exception: null },
        result: { data: [], bulked: false }
      });
    });
  });

  describe('bulked responses', () => {
    it('single item with bulk count', () => {
      const buffer = Buffer.from([
        0x81, // version
        0x01, // bulked=true
        0x01, 0x00, 0x00, 0x00, 0x00, 0x43, // fq Int: 67
        0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, // fq Long bulk=3
        0xFD, 0x00, 0x00, // marker
        0x00, 0x00, 0x00, 0xC8, // status_code=200
        0x01, // status_message null
        0x01  // exception null
      ]);
      const result = reader.readResponse(buffer);
      assert.deepEqual(result, {
        status: { code: 200, message: null, exception: null },
        result: { data: [{ v: 67, bulk: 3 }], bulked: true }
      });
    });

    it('multiple items with bulk counts', () => {
      const buffer = Buffer.from([
        0x81, // version
        0x01, // bulked=true
        0x01, 0x00, 0x00, 0x00, 0x00, 0x43, // fq Int: 67
        0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, // fq Long bulk=2
        0x03, 0x00, 0x00, 0x00, 0x00, 0x05, 0x68, 0x65, 0x6C, 0x6C, 0x6F, // fq String: "hello"
        0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // fq Long bulk=1
        0xFD, 0x00, 0x00, // marker
        0x00, 0x00, 0x00, 0xC8, // status_code=200
        0x01, // status_message null
        0x01  // exception null
      ]);
      const result = reader.readResponse(buffer);
      assert.deepEqual(result, {
        status: { code: 200, message: null, exception: null },
        result: { data: [{ v: 67, bulk: 2 }, { v: 'hello', bulk: 1 }], bulked: true }
      });
    });
  });

  describe('status codes', () => {
    it('status 403', () => {
      const buffer = Buffer.from([
        0x81, 0x00, // version, bulked=false
        0xFD, 0x00, 0x00, // marker
        0x00, 0x00, 0x01, 0x93, // status_code=403
        0x01, 0x01 // null message, null exception
      ]);
      const result = reader.readResponse(buffer);
      assert.equal(result.status.code, 403);
    });

    it('status 500', () => {
      const buffer = Buffer.from([
        0x81, 0x00, // version, bulked=false
        0xFD, 0x00, 0x00, // marker
        0x00, 0x00, 0x01, 0xF4, // status_code=500
        0x01, 0x01 // null message, null exception
      ]);
      const result = reader.readResponse(buffer);
      assert.equal(result.status.code, 500);
    });
  });

  describe('nullable status_message', () => {
    it('present message', () => {
      const buffer = Buffer.from([
        0x81, 0x00, // version, bulked=false
        0xFD, 0x00, 0x00, // marker
        0x00, 0x00, 0x00, 0xC8, // status_code=200
        0x00, // message present flag
        0x00, 0x00, 0x00, 0x07, 0x53, 0x75, 0x63, 0x63, 0x65, 0x73, 0x73, // bare String: "Success"
        0x01 // exception null
      ]);
      const result = reader.readResponse(buffer);
      assert.equal(result.status.message, 'Success');
    });

    it('null message', () => {
      const buffer = Buffer.from([
        0x81, 0x00, // version, bulked=false
        0xFD, 0x00, 0x00, // marker
        0x00, 0x00, 0x00, 0xC8, // status_code=200
        0x01, // message null flag
        0x01 // exception null
      ]);
      const result = reader.readResponse(buffer);
      assert.equal(result.status.message, null);
    });
  });

  describe('nullable exception', () => {
    it('present exception', () => {
      const buffer = Buffer.from([
        0x81, 0x00, // version, bulked=false
        0xFD, 0x00, 0x00, // marker
        0x00, 0x00, 0x01, 0xF4, // status_code=500
        0x01, // message null
        0x00, // exception present flag
        0x00, 0x00, 0x00, 0x05, 0x45, 0x72, 0x72, 0x6F, 0x72 // bare String: "Error"
      ]);
      const result = reader.readResponse(buffer);
      assert.equal(result.status.exception, 'Error');
    });

    it('null exception', () => {
      const buffer = Buffer.from([
        0x81, 0x00, // version, bulked=false
        0xFD, 0x00, 0x00, // marker
        0x00, 0x00, 0x00, 0xC8, // status_code=200
        0x01, // message null
        0x01 // exception null flag
      ]);
      const result = reader.readResponse(buffer);
      assert.equal(result.status.exception, null);
    });
  });

  describe('error response', () => {
    it('no result data with error status', () => {
      const buffer = Buffer.from([
        0x81, 0x00, // version, bulked=false
        0xFD, 0x00, 0x00, // marker (no data)
        0x00, 0x00, 0x01, 0xF4, // status_code=500
        0x00, // message present
        0x00, 0x00, 0x00, 0x0E, 0x49, 0x6E, 0x74, 0x65, 0x72, 0x6E, 0x61, 0x6C, 0x20, 0x65, 0x72, 0x72, 0x6F, 0x72, // "Internal error" (14 chars)
        0x00, // exception present
        0x00, 0x00, 0x00, 0x09, 0x45, 0x78, 0x63, 0x65, 0x70, 0x74, 0x69, 0x6F, 0x6E // "Exception"
      ]);
      const result = reader.readResponse(buffer);
      assert.deepEqual(result, {
        status: { code: 500, message: 'Internal error', exception: 'Exception' },
        result: { data: [], bulked: false }
      });
    });
  });

  describe('complex result values', () => {
    it('vertex in result data', () => {
      const buffer = Buffer.from([
        0x81, 0x00, // version, bulked=false
        0x11, 0x00, // fq Vertex: type_code=0x11, value_flag=0x00
        0x01, 0x00, 0x00, 0x00, 0x00, 0x01, // id: fq Int=1
        0x00, 0x00, 0x00, 0x01, // label: bare List length=1
        0x03, 0x00, 0x00, 0x00, 0x00, 0x06, 0x70, 0x65, 0x72, 0x73, 0x6F, 0x6E, // fq String="person"
        0x0B, 0x00, 0x00, 0x00, 0x00, 0x00, // properties: fq List=empty
        0xFD, 0x00, 0x00, // marker
        0x00, 0x00, 0x00, 0xC8, // status_code=200
        0x01, 0x01 // null message, null exception
      ]);
      const result = reader.readResponse(buffer);
      assert.equal(result.result.data.length, 1);
      assert.equal(result.result.data[0].id, 1);
      assert.equal(result.result.data[0].label, 'person');
    });
  });
});
