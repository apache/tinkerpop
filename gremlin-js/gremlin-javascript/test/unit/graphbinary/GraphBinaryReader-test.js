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

/*
 * GraphBinaryReader v4 response format tests.
 * Tests the reader's ability to parse v4 response format:
 * {version:0x84}{bulked:Byte}{result_data stream}{marker:0xFD 0x00 0x00}{status_code:Int bare}{status_message:nullable}{exception:nullable}
 */

import { assert } from 'chai';
import { Buffer } from 'buffer';
import GraphBinaryReader from '../../../lib/structure/io/binary/internals/GraphBinaryReader.js';
import StreamReader from '../../../lib/structure/io/binary/internals/StreamReader.js';
import ioc from '../../../lib/structure/io/binary/GraphBinary.js';
import { Traverser } from '../../../lib/process/traversal.js';
import ResponseError from '../../../lib/driver/response-error.js';

describe('GraphBinaryReader', () => {
  const reader = new GraphBinaryReader(ioc);

  describe('input validation', () => {
    it('undefined buffer throws error', async () => {
      try {
        await reader.readResponse(undefined);
        assert.fail('should have thrown');
      } catch (e) {
        assert.match(e.message, /Buffer is missing/);
      }
    });

    it('null buffer throws error', async () => {
      try {
        await reader.readResponse(null);
        assert.fail('should have thrown');
      } catch (e) {
        assert.match(e.message, /Buffer is missing/);
      }
    });

    it('empty buffer throws error', async () => {
      try {
        await reader.readResponse(Buffer.alloc(0));
        assert.fail('should have thrown');
      } catch (e) {
        assert.match(e.message, /Buffer is empty/);
      }
    });
  });

  describe('version validation', () => {
    it('rejects version 0x00', async () => {
      try {
        await reader.readResponse(Buffer.from([0x00, 0x00]));
        assert.fail('should have thrown');
      } catch (e) {
        assert.match(e.message, /Unsupported version/);
      }
    });

    it('rejects version 0x81', async () => {
      try {
        await reader.readResponse(Buffer.from([0x81, 0x00]));
        assert.fail('should have thrown');
      } catch (e) {
        assert.match(e.message, /Unsupported version/);
      }
    });
  });

  describe('non-bulked responses', () => {
    it('single value', async () => {
      const buffer = Buffer.from([
        0x84, // version
        0x00, // bulked=false
        0x01, 0x00, 0x00, 0x00, 0x00, 0x43, // fq Int: type_code=0x01, value_flag=0x00, value=67
        0xFD, 0x00, 0x00, // marker
        0x00, 0x00, 0x00, 0xC8, // status_code=200
        0x01, // status_message null flag
        0x01  // exception null flag
      ]);
      const result = await reader.readResponse(buffer);
      assert.deepEqual(result, {
        status: { code: 200, message: null, exception: null },
        result: { data: [67], bulked: false },
      });
    });

    it('multiple values', async () => {
      const buffer = Buffer.from([
        0x84, // version
        0x00, // bulked=false
        0x01, 0x00, 0x00, 0x00, 0x00, 0x43, // fq Int: 67
        0x03, 0x00, 0x00, 0x00, 0x00, 0x05, 0x68, 0x65, 0x6C, 0x6C, 0x6F, // fq String: "hello"
        0xFD, 0x00, 0x00, // marker
        0x00, 0x00, 0x00, 0xC8, // status_code=200
        0x01, // status_message null
        0x01  // exception null
      ]);
      const result = await reader.readResponse(buffer);
      assert.deepEqual(result, {
        status: { code: 200, message: null, exception: null },
        result: { data: [67, 'hello'], bulked: false }
      });
    });

    it('empty result', async () => {
      const buffer = Buffer.from([
        0x84, // version
        0x00, // bulked=false
        0xFD, 0x00, 0x00, // marker (no data)
        0x00, 0x00, 0x00, 0xCC, // status_code=204
        0x01, // status_message null
        0x01  // exception null
      ]);
      const result = await reader.readResponse(buffer);
      assert.deepEqual(result, {
        status: { code: 204, message: null, exception: null },
        result: { data: [], bulked: false }
      });
    });
  });

  describe('bulked responses', () => {
    it('single item with bulk count', async () => {
      const buffer = Buffer.from([
        0x84, // version
        0x01, // bulked=true
        0x01, 0x00, 0x00, 0x00, 0x00, 0x43, // fq Int: 67
        0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, // fq Long bulk=3
        0xFD, 0x00, 0x00, // marker
        0x00, 0x00, 0x00, 0xC8, // status_code=200
        0x01, // status_message null
        0x01  // exception null
      ]);
      const result = await reader.readResponse(buffer);
      assert.deepEqual(result, {
        status: { code: 200, message: null, exception: null },
        result: { data: [{ v: 67, bulk: 3 }], bulked: true }
      });
    });

    it('multiple items with bulk counts', async () => {
      const buffer = Buffer.from([
        0x84, // version
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
      const result = await reader.readResponse(buffer);
      assert.deepEqual(result, {
        status: { code: 200, message: null, exception: null },
        result: { data: [{ v: 67, bulk: 2 }, { v: 'hello', bulk: 1 }], bulked: true }
      });
    });
  });

  describe('status codes', () => {
    it('status 403', async () => {
      const buffer = Buffer.from([
        0x84, 0x00, // version, bulked=false
        0xFD, 0x00, 0x00, // marker
        0x00, 0x00, 0x01, 0x93, // status_code=403
        0x01, 0x01 // null message, null exception
      ]);
      const result = await reader.readResponse(buffer);
      assert.equal(result.status.code, 403);
    });

    it('status 500', async () => {
      const buffer = Buffer.from([
        0x84, 0x00, // version, bulked=false
        0xFD, 0x00, 0x00, // marker
        0x00, 0x00, 0x01, 0xF4, // status_code=500
        0x01, 0x01 // null message, null exception
      ]);
      const result = await reader.readResponse(buffer);
      assert.equal(result.status.code, 500);
    });
  });

  describe('nullable status_message', () => {
    it('present message', async () => {
      const buffer = Buffer.from([
        0x84, 0x00, // version, bulked=false
        0xFD, 0x00, 0x00, // marker
        0x00, 0x00, 0x00, 0xC8, // status_code=200
        0x00, // message present flag
        0x00, 0x00, 0x00, 0x07, 0x53, 0x75, 0x63, 0x63, 0x65, 0x73, 0x73, // bare String: "Success"
        0x01 // exception null
      ]);
      const result = await reader.readResponse(buffer);
      assert.equal(result.status.message, 'Success');
    });

    it('null message', async () => {
      const buffer = Buffer.from([
        0x84, 0x00, // version, bulked=false
        0xFD, 0x00, 0x00, // marker
        0x00, 0x00, 0x00, 0xC8, // status_code=200
        0x01, // message null flag
        0x01 // exception null
      ]);
      const result = await reader.readResponse(buffer);
      assert.equal(result.status.message, null);
    });
  });

  describe('nullable exception', () => {
    it('present exception', async () => {
      const buffer = Buffer.from([
        0x84, 0x00, // version, bulked=false
        0xFD, 0x00, 0x00, // marker
        0x00, 0x00, 0x01, 0xF4, // status_code=500
        0x01, // message null
        0x00, // exception present flag
        0x00, 0x00, 0x00, 0x05, 0x45, 0x72, 0x72, 0x6F, 0x72 // bare String: "Error"
      ]);
      const result = await reader.readResponse(buffer);
      assert.equal(result.status.exception, 'Error');
    });

    it('null exception', async () => {
      const buffer = Buffer.from([
        0x84, 0x00, // version, bulked=false
        0xFD, 0x00, 0x00, // marker
        0x00, 0x00, 0x00, 0xC8, // status_code=200
        0x01, // message null
        0x01 // exception null flag
      ]);
      const result = await reader.readResponse(buffer);
      assert.equal(result.status.exception, null);
    });
  });

  describe('error response', () => {
    it('no result data with error status', async () => {
      const buffer = Buffer.from([
        0x84, 0x00, // version, bulked=false
        0xFD, 0x00, 0x00, // marker (no data)
        0x00, 0x00, 0x01, 0xF4, // status_code=500
        0x00, // message present
        0x00, 0x00, 0x00, 0x0E, 0x49, 0x6E, 0x74, 0x65, 0x72, 0x6E, 0x61, 0x6C, 0x20, 0x65, 0x72, 0x72, 0x6F, 0x72, // "Internal error" (14 chars)
        0x00, // exception present
        0x00, 0x00, 0x00, 0x09, 0x45, 0x78, 0x63, 0x65, 0x70, 0x74, 0x69, 0x6F, 0x6E // "Exception"
      ]);
      const result = await reader.readResponse(buffer);
      assert.deepEqual(result, {
        status: { code: 500, message: 'Internal error', exception: 'Exception' },
        result: { data: [], bulked: false }
      });
    });
  });

  describe('complex result values', () => {
    it('vertex in result data', async () => {
      const buffer = Buffer.from([
        0x84, 0x00, // version, bulked=false
        0x11, 0x00, // fq Vertex: type_code=0x11, value_flag=0x00
        0x01, 0x00, 0x00, 0x00, 0x00, 0x01, // id: fq Int=1
        0x00, 0x00, 0x00, 0x01, // label: bare List length=1
        0x03, 0x00, 0x00, 0x00, 0x00, 0x06, 0x70, 0x65, 0x72, 0x73, 0x6F, 0x6E, // fq String="person"
        0x0B, 0x00, 0x00, 0x00, 0x00, 0x00, // properties: fq List=empty
        0xFD, 0x00, 0x00, // marker
        0x00, 0x00, 0x00, 0xC8, // status_code=200
        0x01, 0x01 // null message, null exception
      ]);
      const result = await reader.readResponse(buffer);
      assert.equal(result.result.data.length, 1);
      assert.equal(result.result.data[0].id, 1);
      assert.equal(result.result.data[0].label, 'person');
    });
  });

  describe('readResponseStream', () => {
    it('non-bulked stream yields raw values', async () => {
      const buffer = Buffer.from([
        0x84, // version
        0x00, // bulked=false
        0x01, 0x00, 0x00, 0x00, 0x00, 0x43, // fq Int=67
        0xFD, 0x00, 0x00, // marker
        0x00, 0x00, 0x00, 0xC8, // status_code=200
        0x01, // status_message null
        0x01  // exception null
      ]);
      const streamReader = StreamReader.fromBuffer(buffer);
      const results = [];
      for await (const item of reader.readResponseStream(streamReader)) {
        results.push(item);
      }
      assert.deepEqual(results, [67]);
    });

    it('bulked stream yields Traverser objects', async () => {
      const buffer = Buffer.from([
        0x84, // version
        0x01, // bulked=true
        0x01, 0x00, 0x00, 0x00, 0x00, 0x43, // fq Int=67
        0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, // fq Long=3
        0xFD, 0x00, 0x00, // marker
        0x00, 0x00, 0x00, 0xC8, // status_code=200
        0x01, // status_message null
        0x01  // exception null
      ]);
      const streamReader = StreamReader.fromBuffer(buffer);
      const results = [];
      for await (const item of reader.readResponseStream(streamReader)) {
        results.push(item);
      }
      assert.equal(results.length, 1);
      assert.instanceOf(results[0], Traverser);
      assert.equal(results[0].object, 67);
      assert.equal(results[0].bulk, 3);
    });

    it('bulked stream Traversers expand correctly via Traversal', async () => {
      const { Traversal } = await import('../../../lib/process/traversal.js');
      const buffer = Buffer.from([
        0x84, // version
        0x01, // bulked=true
        0x01, 0x00, 0x00, 0x00, 0x00, 0x43, // fq Int=67
        0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, // fq Long=3
        0xFD, 0x00, 0x00, // marker
        0x00, 0x00, 0x00, 0xC8, // status_code=200
        0x01, 0x01
      ]);
      const streamReader = StreamReader.fromBuffer(buffer);
      const gen = reader.readResponseStream(streamReader);
      const traversal = new Traversal(null, null);
      traversal._resultsStream = gen;
      const list = await traversal.toList();
      // bulk=3 should expand to 3 copies of 67
      assert.deepEqual(list, [67, 67, 67]);
    });

    it('trailing status is accessible via Traversal.getStatus()', async () => {
      const { Traversal } = await import('../../../lib/process/traversal.js');
      const buffer = Buffer.from([
        0x84, // version
        0x00, // bulked=false
        0x01, 0x00, 0x00, 0x00, 0x00, 0x43, // fq Int=67
        0xFD, 0x00, 0x00, // marker
        0x00, 0x00, 0x00, 0xC8, // status_code=200
        0x00, // message present
        0x00, 0x00, 0x00, 0x02, 0x4F, 0x4B, // "OK"
        0x01  // exception null
      ]);
      const streamReader = StreamReader.fromBuffer(buffer);
      const gen = reader.readResponseStream(streamReader);
      const traversal = new Traversal(null, null);
      traversal._resultsStream = gen;

      assert.isNull(traversal.getStatus());
      await traversal.toList();
      const status = traversal.getStatus();
      assert.isNotNull(status);
      assert.equal(status.code, 200);
      assert.equal(status.message, 'OK');
      assert.isNull(status.exception);
    });

    it('throws ResponseError on server error status after yielding values', async () => {
      const buffer = Buffer.from([
        0x84, // version
        0x00, // bulked=false
        0x01, 0x00, 0x00, 0x00, 0x00, 0x43, // fq Int=67
        0xFD, 0x00, 0x00, // marker
        0x00, 0x00, 0x01, 0xF4, // status_code=500
        0x00, // message present
        0x00, 0x00, 0x00, 0x0E, 0x49, 0x6E, 0x74, 0x65, 0x72, 0x6E, 0x61, 0x6C, 0x20, 0x65, 0x72, 0x72, 0x6F, 0x72, // "Internal error"
        0x00, // exception present
        0x00, 0x00, 0x00, 0x09, 0x45, 0x78, 0x63, 0x65, 0x70, 0x74, 0x69, 0x6F, 0x6E // "Exception"
      ]);
      const streamReader = StreamReader.fromBuffer(buffer);
      const results = [];
      try {
        for await (const item of reader.readResponseStream(streamReader)) {
          results.push(item);
        }
        assert.fail('should have thrown');
      } catch (e) {
        assert.instanceOf(e, ResponseError);
        assert.equal(e.statusCode, 500);
        assert.match(e.message, /Internal error/);
      }
      // Verify partial results were yielded before the error
      assert.deepEqual(results, [67]);
    });

    it('hasNext() peeks without consuming from streaming source', async () => {
      const { Traversal } = await import('../../../lib/process/traversal.js');
      const buffer = Buffer.from([
        0x84, 0x00,
        0x01, 0x00, 0x00, 0x00, 0x00, 0x01, // fq Int=1
        0x01, 0x00, 0x00, 0x00, 0x00, 0x02, // fq Int=2
        0xFD, 0x00, 0x00,
        0x00, 0x00, 0x00, 0xC8,
        0x01, 0x01
      ]);
      const streamReader = StreamReader.fromBuffer(buffer);
      const gen = reader.readResponseStream(streamReader);
      const traversal = new Traversal(null, null);
      traversal._resultsStream = gen;

      // hasNext should return true without consuming
      assert.isTrue(await traversal.hasNext());
      assert.isTrue(await traversal.hasNext()); // calling again should still be true

      // next() should return the peeked value
      const first = await traversal.next();
      assert.equal(first.value, 1);

      assert.isTrue(await traversal.hasNext());
      const second = await traversal.next();
      assert.equal(second.value, 2);

      assert.isFalse(await traversal.hasNext());
    });

    it('deserializes correctly when response is split into 1-byte chunks via ReadableStream', async () => {
      // Build a complete v4 response: version + bulked=false + Int(67) + String("hi") + marker + status 200
      const buffer = Buffer.from([
        0x84, // version
        0x00, // bulked=false
        0x01, 0x00, 0x00, 0x00, 0x00, 0x43, // fq Int=67
        0x03, 0x00, 0x00, 0x00, 0x00, 0x02, 0x68, 0x69, // fq String="hi"
        0xFD, 0x00, 0x00, // marker
        0x00, 0x00, 0x00, 0xC8, // status_code=200
        0x01, // status_message null
        0x01  // exception null
      ]);
      // Split into 1-byte chunks
      const chunks = [];
      for (let i = 0; i < buffer.length; i++) {
        chunks.push(buffer.subarray(i, i + 1));
      }
      const stream = new ReadableStream({
        pull(controller) {
          if (chunks.length > 0) {
            controller.enqueue(new Uint8Array(chunks.shift()));
          } else {
            controller.close();
          }
        },
      });
      const streamReader = StreamReader.fromReadableStream(stream);
      const results = [];
      for await (const item of reader.readResponseStream(streamReader)) {
        results.push(item);
      }
      assert.deepEqual(results, [67, 'hi']);
    });

    it('yields first result before remaining data is available', async () => {
      // v4 response: version + bulked=false + Int(42) + Int(99) + marker + status 200
      const response = Buffer.from([
        0x84,                                 // version
        0x00,                                 // bulked=false
        0x01, 0x00, 0x00, 0x00, 0x00, 0x2A,  // fq Int=42
        0x01, 0x00, 0x00, 0x00, 0x00, 0x63,  // fq Int=99
        0xFD, 0x00, 0x00,                     // marker
        0x00, 0x00, 0x00, 0xC8,               // status_code=200
        0x01,                                 // status_message null
        0x01                                  // exception null
      ]);

      // Split: first chunk has header (2 bytes) + first value (6 bytes) = 8 bytes.
      // The rest (second value + marker + status) is withheld.
      const firstChunk = response.subarray(0, 8);
      const remaining = response.subarray(8);

      let releaseRemaining;
      const remainingReady = new Promise((resolve) => { releaseRemaining = resolve; });
      let closed = false;

      const stream = new ReadableStream({
        async pull(controller) {
          if (!closed) {
            // First pull: deliver just enough for the header + first value
            controller.enqueue(new Uint8Array(firstChunk));
            closed = true;
            // Block until test signals to release the rest
            await remainingReady;
            controller.enqueue(new Uint8Array(remaining));
            controller.close();
          }
        },
      });

      const streamReader = StreamReader.fromReadableStream(stream);
      const gen = reader.readResponseStream(streamReader);

      // First next() should resolve with 42 — only the first chunk was delivered
      const first = await gen.next();
      assert.isFalse(first.done);
      assert.equal(first.value, 42);

      // Release the remaining data
      releaseRemaining();

      // Second value should now be available
      const second = await gen.next();
      assert.isFalse(second.done);
      assert.equal(second.value, 99);

      // Generator should complete with status as return value
      const end = await gen.next();
      assert.isTrue(end.done);
      assert.equal(end.value.code, 200);
    });
  });
});
