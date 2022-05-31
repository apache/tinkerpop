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

/**
 * GraphBinary reader.
 */
module.exports = class GraphBinaryReader {
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

    const response = { status: {}, result: {} };
    let cursor = buffer;
    let len;

    // {version} is a Byte representing the protocol version
    const version = cursor[0];
    if (version !== 0x81) {
      throw new Error(`Unsupported version '${version}'.`);
    }
    cursor = cursor.slice(1); // skip version

    // {request_id} is a nullable UUID
    ({ v: response.requestId, len } = this.ioc.uuidSerializer.deserialize(cursor, false, true));
    cursor = cursor.slice(len);

    // {status_code} is an Int
    ({ v: response.status.code, len } = this.ioc.intSerializer.deserialize(cursor, false));
    cursor = cursor.slice(len);

    // {status_message} is a nullable String
    ({ v: response.status.message, len } = this.ioc.stringSerializer.deserialize(cursor, false, true));
    cursor = cursor.slice(len);

    // {status_attributes} is a Map
    ({ v: response.status.attributes, len } = this.ioc.mapSerializer.deserialize(cursor, false));
    cursor = cursor.slice(len);

    // {result_meta} is a Map
    ({ v: response.result.meta, len } = this.ioc.mapSerializer.deserialize(cursor, false));
    cursor = cursor.slice(len);

    // {result_data} is a fully qualified typed value composed of {type_code}{type_info}{value_flag}{value}
    ({ v: response.result.data } = this.ioc.anySerializer.deserialize(cursor));

    return response;
  }
};
