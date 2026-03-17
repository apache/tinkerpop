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

/**
 * GraphBinary reader.
 */
export default class GraphBinaryReader {
  constructor(ioc) {
    this.ioc = ioc;
  }

  readResponse(buffer) {
    if (buffer === undefined || buffer === null) {
      throw new Error('Buffer is missing.');
    }
    if (!(buffer instanceof Buffer)) {
      throw new Error('Not an instance of Buffer.');
    }
    if (buffer.length < 1) {
      throw new Error('Buffer is empty.');
    }

    let cursor = buffer;
    let len;

    // {version} is a Byte representing the protocol version
    const version = cursor[0];
    if (version !== 0x81) {
      throw new Error(`Unsupported version '${version}'.`);
    }
    cursor = cursor.slice(1); // skip version

    // {bulked} is a Byte: 0x00 = not bulked, 0x01 = bulked
    const bulked = cursor[0] === 0x01;
    cursor = cursor.slice(1);

    // {result_data} stream - read values until marker
    const data = [];
    while (cursor[0] !== 0xfd) {
      const { v, len: valueLen } = this.ioc.anySerializer.deserialize(cursor);
      cursor = cursor.slice(valueLen);

      if (bulked) {
        const { v: bulk, len: bulkLen } = this.ioc.longSerializer.deserialize(cursor, true);
        cursor = cursor.slice(bulkLen);
        data.push({ v, bulk: Number(bulk) });
      } else {
        data.push(v);
      }
    }

    // Skip marker [0xFD, 0x00, 0x00]
    cursor = cursor.slice(3);

    // {status_code} is an Int bare
    const { v: code, len: codeLen } = this.ioc.intSerializer.deserialize(cursor, false);
    cursor = cursor.slice(codeLen);

    // {status_message} is nullable
    let message = null;
    if (cursor[0] === 0x00) {
      cursor = cursor.slice(1);
      ({ v: message, len } = this.ioc.stringSerializer.deserialize(cursor, false));
      cursor = cursor.slice(len);
    } else {
      cursor = cursor.slice(1); // skip 0x01 null flag
    }

    // {exception} is nullable
    let exception = null;
    if (cursor[0] === 0x00) {
      cursor = cursor.slice(1);
      ({ v: exception } = this.ioc.stringSerializer.deserialize(cursor, false));
    }

    return {
      status: { code, message, exception },
      result: { data, bulked },
    };
  }
}
