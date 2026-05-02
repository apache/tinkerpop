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

import { assert } from 'chai';
import { Buffer } from 'buffer';
import StreamReader from '../../../lib/structure/io/binary/internals/StreamReader.js';

/**
 * Helper: create a ReadableStream that yields the given chunks with optional delays.
 * @param {Buffer[]} chunks
 * @returns {ReadableStream}
 */
function chunkedStream(chunks) {
  let i = 0;
  return new ReadableStream({
    pull(controller) {
      if (i < chunks.length) {
        controller.enqueue(new Uint8Array(chunks[i++]));
      } else {
        controller.close();
      }
    },
  });
}

describe('StreamReader', () => {
  describe('fromBuffer', () => {
    it('readUInt8 reads single bytes', async () => {
      const reader = StreamReader.fromBuffer(Buffer.from([0x01, 0xff, 0x00]));
      assert.equal(await reader.readUInt8(), 0x01);
      assert.equal(await reader.readUInt8(), 0xff);
      assert.equal(await reader.readUInt8(), 0x00);
    });

    it('readByte reads signed bytes', async () => {
      const reader = StreamReader.fromBuffer(Buffer.from([0x7f, 0x80]));
      assert.equal(await reader.readByte(), 127);
      assert.equal(await reader.readByte(), -128);
    });

    it('readInt16BE reads signed 16-bit', async () => {
      const buf = Buffer.alloc(4);
      buf.writeInt16BE(12345, 0);
      buf.writeInt16BE(-1, 2);
      const reader = StreamReader.fromBuffer(buf);
      assert.equal(await reader.readInt16BE(), 12345);
      assert.equal(await reader.readInt16BE(), -1);
    });

    it('readInt32BE reads signed 32-bit', async () => {
      const buf = Buffer.alloc(8);
      buf.writeInt32BE(2147483647, 0);
      buf.writeInt32BE(-2147483648, 4);
      const reader = StreamReader.fromBuffer(buf);
      assert.equal(await reader.readInt32BE(), 2147483647);
      assert.equal(await reader.readInt32BE(), -2147483648);
    });

    it('readBigInt64BE reads signed 64-bit', async () => {
      const buf = Buffer.alloc(8);
      buf.writeBigInt64BE(9223372036854775807n, 0);
      const reader = StreamReader.fromBuffer(buf);
      assert.equal(await reader.readBigInt64BE(), 9223372036854775807n);
    });

    it('readFloatBE reads 32-bit float', async () => {
      const buf = Buffer.alloc(4);
      buf.writeFloatBE(3.14, 0);
      const reader = StreamReader.fromBuffer(buf);
      assert.closeTo(await reader.readFloatBE(), 3.14, 0.001);
    });

    it('readDoubleBE reads 64-bit double', async () => {
      const buf = Buffer.alloc(8);
      buf.writeDoubleBE(3.141592653589793, 0);
      const reader = StreamReader.fromBuffer(buf);
      assert.equal(await reader.readDoubleBE(), 3.141592653589793);
    });

    it('readBytes returns exact slice', async () => {
      const reader = StreamReader.fromBuffer(Buffer.from([0x01, 0x02, 0x03, 0x04]));
      const bytes = await reader.readBytes(2);
      assert.deepEqual([...bytes], [0x01, 0x02]);
      const rest = await reader.readBytes(2);
      assert.deepEqual([...rest], [0x03, 0x04]);
    });

    it('throws on read past end of buffer', async () => {
      const reader = StreamReader.fromBuffer(Buffer.from([0x01]));
      await reader.readUInt8();
      try {
        await reader.readUInt8();
        assert.fail('should have thrown');
      } catch (e) {
        assert.match(e.message, /Unexpected end of buffer/);
      }
    });

    it('mixed reads advance offset correctly', async () => {
      const buf = Buffer.alloc(13);
      buf.writeUInt8(0xAB, 0);
      buf.writeInt32BE(42, 1);
      buf.writeDoubleBE(1.5, 5);
      const reader = StreamReader.fromBuffer(buf);
      assert.equal(await reader.readUInt8(), 0xAB);
      assert.equal(await reader.readInt32BE(), 42);
      assert.equal(await reader.readDoubleBE(), 1.5);
    });
  });

  describe('fromReadableStream', () => {
    it('reads from a single chunk', async () => {
      const stream = chunkedStream([Buffer.from([0x01, 0x02, 0x03])]);
      const reader = StreamReader.fromReadableStream(stream);
      assert.equal(await reader.readUInt8(), 0x01);
      assert.equal(await reader.readUInt8(), 0x02);
      assert.equal(await reader.readUInt8(), 0x03);
    });

    it('reads across chunk boundaries', async () => {
      // Int32 (4 bytes) split across two 2-byte chunks
      const buf = Buffer.alloc(4);
      buf.writeInt32BE(305419896, 0); // 0x12345678
      const stream = chunkedStream([buf.subarray(0, 2), buf.subarray(2, 4)]);
      const reader = StreamReader.fromReadableStream(stream);
      assert.equal(await reader.readInt32BE(), 305419896);
    });

    it('reads across many small chunks', async () => {
      // 8-byte double split into 1-byte chunks
      const buf = Buffer.alloc(8);
      buf.writeDoubleBE(2.718281828, 0);
      const chunks = [];
      for (let i = 0; i < 8; i++) {
        chunks.push(buf.subarray(i, i + 1));
      }
      const stream = chunkedStream(chunks);
      const reader = StreamReader.fromReadableStream(stream);
      assert.closeTo(await reader.readDoubleBE(), 2.718281828, 1e-9);
    });

    it('handles readBytes spanning chunks', async () => {
      const stream = chunkedStream([Buffer.from([0x01, 0x02]), Buffer.from([0x03, 0x04, 0x05])]);
      const reader = StreamReader.fromReadableStream(stream);
      const bytes = await reader.readBytes(4);
      assert.deepEqual([...bytes], [0x01, 0x02, 0x03, 0x04]);
      assert.equal(await reader.readUInt8(), 0x05);
    });

    it('throws on premature end of stream', async () => {
      const stream = chunkedStream([Buffer.from([0x01])]);
      const reader = StreamReader.fromReadableStream(stream);
      try {
        await reader.readInt32BE();
        assert.fail('should have thrown');
      } catch (e) {
        assert.match(e.message, /Unexpected end of stream/);
      }
    });

    it('handles empty stream', async () => {
      const stream = chunkedStream([]);
      const reader = StreamReader.fromReadableStream(stream);
      try {
        await reader.readUInt8();
        assert.fail('should have thrown');
      } catch (e) {
        assert.match(e.message, /Unexpected end of stream/);
      }
    });

    it('sequential reads across multiple chunks', async () => {
      const buf = Buffer.alloc(9);
      buf.writeUInt8(0x84, 0);       // version byte
      buf.writeInt32BE(200, 1);       // status code
      buf.writeInt32BE(-1, 5);        // another int
      // Split: [version + 1 byte of int] [3 bytes of int + 2 bytes] [2 bytes]
      const stream = chunkedStream([buf.subarray(0, 2), buf.subarray(2, 7), buf.subarray(7, 9)]);
      const reader = StreamReader.fromReadableStream(stream);
      assert.equal(await reader.readUInt8(), 0x84);
      assert.equal(await reader.readInt32BE(), 200);
      assert.equal(await reader.readInt32BE(), -1);
    });

    it('handles Uint8Array chunks from fetch', async () => {
      // fetch response.body yields Uint8Array, not Buffer
      const data = new Uint8Array([0x01, 0x00, 0x00, 0x00, 0x2A]);
      const stream = new ReadableStream({
        start(controller) {
          controller.enqueue(data);
          controller.close();
        },
      });
      const reader = StreamReader.fromReadableStream(stream);
      assert.equal(await reader.readUInt8(), 0x01);
      assert.equal(await reader.readInt32BE(), 42);
    });
  });
});
