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
 * GraphBinary writer.
 */
module.exports = class GraphBinaryWriter {
  constructor(ioc) {
    this.ioc = ioc;
  }

  writeRequest({ requestId, op, processor, args }) {
    const bufs = [
      // {version} 1 byte
      Buffer.from([0x81]),
      // {request_id} UUID
      this.ioc.uuidSerializer.serialize(requestId, false),
      // {op} String
      this.ioc.stringSerializer.serialize(op, false),
      // {processor} String
      this.ioc.stringSerializer.serialize(processor, false),
      // {args} Map
      this.ioc.mapSerializer.serialize(args, false),
    ];
    return Buffer.concat(bufs);

    /*// Detailed example for a quick reference:
      // {version}
      0x81,
      // {request_id} UUID
      0x00,0x01,0x02,0x03,0x04,0x05,0x06,0x07,0x08,0x09,0x0A,0x0B,0x0C,0x0D,0x0E,0x0F,
      // {op} String
      0x00,0x00,0x00,0x08,  ...Buffer.from('bytecode'),
      // {processor} String
      0x00,0x00,0x00,0x09,  ...Buffer.from('traversal'),
      // {args} Map
      0x00,0x00,0x00,0x02,
        // 1.
        // args.aliases key String
        0x03,0x00,  0x00,0x00,0x00,0x07,  ...Buffer.from('aliases'),
        // args.aliases value Map
        0x0A,0x00,  0x00,0x00,0x00,0x01,
          // aliases.g key String
          0x03,0x00,  0x00,0x00,0x00,0x01,  ...Buffer.from('g'),
          // aliases.g value String
          0x03,0x00,  0x00,0x00,0x00,0x01,  ...Buffer.from('g'),
        // 2.
        // args.gremlin key String
        0x03,0x00,  0x00,0x00,0x00,0x07,  ...Buffer.from('gremlin'),
        // args.gremlin value Bytecode
        0x15,0x00,
          // {steps_length}
          0x00,0x00,0x00,0x01,
            // step 1 - {name} String
            0x00,0x00,0x00,0x01,  ...Buffer.from('V'),
            // step 1 - {values_length} Int
            0x00,0x00,0x00,0x00,
          // {sources_length}
          0x00,0x00,0x00,0x00,
    */
  }
};
