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

import { Buffer } from 'buffer';

/**
 * Async byte reader that provides a uniform interface over both a complete Buffer
 * (for non-streaming submit()) and a ReadableStream (for streaming HTTP responses).
 *
 * Handles chunk boundaries transparently and blocks (awaits) until the requested bytes are available.
 */
export default class StreamReader {
  /** @type {Buffer} */
  #buffer;
  /** @type {number} */
  #offset;
  /** @type {ReadableStreamDefaultReader|null} */
  #reader;
  /** @type {number} Total bytes consumed (monotonically increasing, survives chunk reassembly) */
  #position;

  /**
   * @param {Buffer} initialBuffer
   * @param {ReadableStreamDefaultReader|null} reader
   */
  constructor(initialBuffer, reader) {
    this.#buffer = initialBuffer;
    this.#offset = 0;
    this.#reader = reader;
    this.#position = 0;
  }

  /**
   * Create a StreamReader backed by a complete Buffer.
   * All reads are satisfied from the buffer; no async I/O occurs.
   * @param {Buffer} buffer
   * @returns {StreamReader}
   */
  static fromBuffer(buffer) {
    return new StreamReader(buffer, null);
  }

  /**
   * Create a StreamReader backed by a ReadableStream (e.g. fetch response.body).
   * Reads pull chunks from the stream as needed.
   * @param {ReadableStream} readableStream
   * @returns {StreamReader}
   */
  static fromReadableStream(readableStream) {
    return new StreamReader(Buffer.alloc(0), readableStream.getReader());
  }

  /**
   * Ensure at least `n` bytes are available in the buffer from the current offset.
   * For buffer-backed readers this is a bounds check. For stream-backed readers
   * this pulls chunks until enough data is buffered.
   * @param {number} n
   */
  async #ensure(n) {
    const available = this.#buffer.length - this.#offset;
    if (available >= n) {
      return;
    }

    if (this.#reader === null) {
      throw new Error(
        `Unexpected end of buffer at position ${this.#position}: needed ${n} bytes, ${available} available`,
      );
    }

    // Collect chunks until we have enough
    const chunks = [this.#buffer.subarray(this.#offset)];
    let total = available;

    while (total < n) {
      const { value, done } = await this.#reader.read();
      if (done) {
        break;
      }
      const chunk = Buffer.isBuffer(value) ? value : Buffer.from(value);
      chunks.push(chunk);
      total += chunk.length;
    }

    if (total < n) {
      throw new Error(`Unexpected end of stream at position ${this.#position}: needed ${n} bytes, ${total} available`);
    }

    this.#buffer = Buffer.concat(chunks);
    this.#offset = 0;
  }

  /**
   * Total number of bytes consumed so far (monotonically increasing).
   * Useful for error diagnostics.
   * @returns {number}
   */
  get position() {
    return this.#position;
  }

  /**
   * Read exactly `n` bytes and return them as a Buffer.
   * @param {number} n
   * @returns {Promise<Buffer>}
   */
  async readBytes(n) {
    await this.#ensure(n);
    const result = this.#buffer.subarray(this.#offset, this.#offset + n);
    this.#offset += n;
    this.#position += n;
    return result;
  }

  /**
   * @returns {Promise<number>} unsigned 8-bit integer
   */
  async readUInt8() {
    await this.#ensure(1);
    this.#position++;
    return this.#buffer[this.#offset++];
  }

  /**
   * @returns {Promise<number>} signed 8-bit integer
   */
  async readByte() {
    await this.#ensure(1);
    this.#position++;
    return this.#buffer.readInt8(this.#offset++);
  }

  /**
   * @returns {Promise<number>} signed 16-bit big-endian integer
   */
  async readInt16BE() {
    await this.#ensure(2);
    const v = this.#buffer.readInt16BE(this.#offset);
    this.#offset += 2;
    this.#position += 2;
    return v;
  }

  /**
   * @returns {Promise<number>} signed 32-bit big-endian integer
   */
  async readInt32BE() {
    await this.#ensure(4);
    const v = this.#buffer.readInt32BE(this.#offset);
    this.#offset += 4;
    this.#position += 4;
    return v;
  }

  /**
   * @returns {Promise<bigint>} signed 64-bit big-endian integer
   */
  async readBigInt64BE() {
    await this.#ensure(8);
    const v = this.#buffer.readBigInt64BE(this.#offset);
    this.#offset += 8;
    this.#position += 8;
    return v;
  }

  /**
   * @returns {Promise<number>} 32-bit big-endian float
   */
  async readFloatBE() {
    await this.#ensure(4);
    const v = this.#buffer.readFloatBE(this.#offset);
    this.#offset += 4;
    this.#position += 4;
    return v;
  }

  /**
   * @returns {Promise<number>} 64-bit big-endian double
   */
  async readDoubleBE() {
    await this.#ensure(8);
    const v = this.#buffer.readDoubleBE(this.#offset);
    this.#offset += 8;
    this.#position += 8;
    return v;
  }
}
