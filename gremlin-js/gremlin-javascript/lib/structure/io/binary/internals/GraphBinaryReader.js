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

import { Buffer } from 'buffer';
import StreamReader from './StreamReader.js';
import { END_OF_STREAM } from './MarkerSerializer.js';
import { Traverser } from '../../../../process/traversal.js';
import ResponseError from '../../../../driver/response-error.js';

/** GraphBinary response status codes. */
const StatusCode = {
  SUCCESS: 200,
  NO_CONTENT: 204,
  PARTIAL_CONTENT: 206,
};

/**
 * GraphBinary reader.
 */
export default class GraphBinaryReader {
  constructor(ioc) {
    this.ioc = ioc;
  }

  get mimeType() {
    return 'application/vnd.graphbinary-v4.0';
  }

  /**
   * Read a complete response from a Buffer. Used by the non-streaming submit() path.
   * Returns the full { status, result } object after reading all data.
   * @param {Buffer} buffer
   * @returns {Promise<{status: {code, message, exception}, result: {data: any[], bulked: boolean}}>}
   */
  async readResponse(buffer) {
    if (buffer === undefined || buffer === null) {
      throw new Error('Buffer is missing.');
    }
    if (!(buffer instanceof Buffer)) {
      throw new Error('Not an instance of Buffer.');
    }
    if (buffer.length < 1) {
      throw new Error('Buffer is empty.');
    }

    const reader = StreamReader.fromBuffer(buffer);
    return await this.#readFromReader(reader);
  }

  /**
   * Stream results from a StreamReader, yielding each value as it's deserialized.
   * Used by the streaming Connection.stream() path.
   *
   * Note: In the GraphBinary v4 streaming protocol, the status (including error codes)
   * is sent *after* all result data. This means values are yielded to the consumer as
   * they arrive, and a server error is only thrown after all values have been yielded.
   * Consumers should be aware that partial results may have been processed before a
   * ResponseError is thrown.
   *
   * @param {StreamReader} reader
   * @returns {AsyncGenerator<any>}
   */
  async *readResponseStream(reader) {
    // {version}
    const version = await reader.readUInt8();
    if (version !== 0x84) {
      throw new Error(`Unsupported version '${version}'.`);
    }

    // {bulked}
    const bulked = (await reader.readUInt8()) === 0x01;

    // {result_data} stream — yield values until EndOfStream marker
    while (true) {
      const value = await this.ioc.anySerializer.deserialize(reader);

      if (value === END_OF_STREAM) {
        break;
      }

      if (bulked) {
        const bulk = await this.ioc.longSerializer.deserialize(reader);
        yield new Traverser(value, Number(bulk));
      } else {
        yield value;
      }
    }

    // {status_code} {status_message} {exception}
    const status = await this.#readStatus(reader);
    if (
      status.code &&
      status.code !== StatusCode.SUCCESS &&
      status.code !== StatusCode.NO_CONTENT &&
      status.code !== StatusCode.PARTIAL_CONTENT
    ) {
      throw new ResponseError(`Server error: ${status.message || 'Unknown error'} (${status.code})`, {
        code: status.code,
        message: status.message || '',
        exception: status.exception,
      });
    }

    // Attach status to the generator's return value
    return status;
  }

  /**
   * Internal: read the full response into a collected result (non-streaming).
   */
  async #readFromReader(reader) {
    // {version}
    const version = await reader.readUInt8();
    if (version !== 0x84) {
      throw new Error(`Unsupported version '${version}'.`);
    }

    // {bulked}
    const bulked = (await reader.readUInt8()) === 0x01;

    // {result_data} — collect all values
    const data = [];
    while (true) {
      const value = await this.ioc.anySerializer.deserialize(reader);

      if (value === END_OF_STREAM) {
        break;
      }

      if (bulked) {
        const bulk = await this.ioc.longSerializer.deserialize(reader);
        data.push({ v: value, bulk: Number(bulk) });
      } else {
        data.push(value);
      }
    }

    // {status}
    const status = await this.#readStatus(reader);

    return {
      status,
      result: { data, bulked },
    };
  }

  /**
   * Read the status block: {code:Int bare}{message:nullable String}{exception:nullable String}
   */
  async #readStatus(reader) {
    const code = await reader.readInt32BE();

    let message = null;
    const msgFlag = await reader.readUInt8();
    if (msgFlag === 0x00) {
      message = await this.ioc.stringSerializer.deserializeValue(reader, 0x00, this.ioc.DataType.STRING);
    }

    let exception = null;
    const excFlag = await reader.readUInt8();
    if (excFlag === 0x00) {
      exception = await this.ioc.stringSerializer.deserializeValue(reader, 0x00, this.ioc.DataType.STRING);
    }

    return { code, message, exception };
  }
}
