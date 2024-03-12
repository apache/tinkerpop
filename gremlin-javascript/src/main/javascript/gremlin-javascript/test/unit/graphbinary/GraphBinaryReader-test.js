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

import assert from 'assert';
import { graphBinaryReader } from '../../../lib/structure/io/binary/GraphBinary.js';

import { Traverser } from '../../../lib/process/traversal.js';
import Bytecode from '../../../lib/process/bytecode.js';
import { GraphTraversal } from '../../../lib/process/graph-traversal.js';
const g = () => new GraphTraversal(undefined, undefined, new Bytecode());

const { from } = Buffer;

describe('GraphBinary.Reader', () => {

  describe('#readResponse', () => {

    [ undefined, null ].forEach(buffer =>
      it(`should error if buffer is '${buffer}'`, () => assert.throws(
        () => graphBinaryReader.readResponse(buffer),
        { message: /Buffer is missing/ },
      ))
    );

    [ '', -1, 0, 42, 3.14, NaN, Infinity, -Infinity, Symbol('') ].forEach((buffer, i) =>
      it(`should error if it is not a Buffer, case #${i}`, () => assert.throws(
        () => graphBinaryReader.readResponse(buffer),
        { message: /Not an instance of Buffer/ },
      ))
    );

    it('should error if buffer is empty', () => assert.throws(
      () => graphBinaryReader.readResponse(from([])),
      { message: /Buffer is empty/ },
    ));

    [
      [ 0x00 ],
      [ 0x00, 0x81 ],
      [ 0x01 ],
      [ 0x80, 0x81 ],
      [ 0x82, 0x81 ],
      [ 0xFF, 0x00, 0x81 ],
      [ 0x00, 0x81 ],
      [ 0x00, 0x00, 0x81 ],
      [ 0x00, 0x00, 0x00, 0x81 ],
    ].forEach((b, i) => it(`should error if version is incorrect, case #${i}`, () => assert.throws(
      () => graphBinaryReader.readResponse(from(b)),
      { message: /Unsupported version/ },
    )));

    [
      { b:[
          // {version}
          0x81,
          // {request_id} nullable UUID
          0x00, 0x01,0x02,0x03,0x04,0x05,0x06,0x07,0x08,0x09,0x0A,0x0B,0x0C,0x0D,0x0E,0x0F,0x10,
          // {status_code} is an Int
          0x00,0x00,0x00,0xC8,
          // {status_message} is a nullable String
          0x00, 0x00,0x00,0x00,0x02,  ...from('OK'),
          // {status_attributes} is a Map
          0x00,0x00,0x00,0x01,
            // key String
            0x03,0x00,  0x00,0x00,0x00,0x0C,  ...from('life-meaning'),
            // value Int
            0x01,0x00, 0x00,0x00,0x00,0x2A,
          // {result_meta} is a Map
          0x00,0x00,0x00,0x02,
            // 1 key String
            0x03,0x00,  0x00,0x00,0x00,0x02,  ...from('m1'),
            // 1 value Int
            0x03,0x00, 0x00,0x00,0x00,0x00,
            // 2 key String
            0x03,0x00,  0x00,0x00,0x00,0x02,  ...from('m2'),
            // 2 value Int
            0x03,0x00, 0x00,0x00,0x00,0x03, ...from('a_B'),
          // {result_data} is a fully qualified typed value composed of {type_code}{type_info}{value_flag}{value}
            // list
            0x09,0x00, 0x00,0x00,0x00,0x01,
              // Traverser
              0x21,0x00,
                // {bulk}
                0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x03,
                // {value}
                0x03,0x00, 0x00,0x00,0x00,0x05, ...from('marko'),
        ],
        v:{
          requestId: '01020304-0506-0708-090a-0b0c0d0e0f10',
          status: {
            code: 200,
            message: 'OK',
            attributes: new Map([ ['life-meaning',42] ]),
          },
          result: {
            meta: new Map([ ['m1',''], ['m2','a_B'] ]),
            data: [ new Traverser('marko', 3n) ],
          }
        }
      },
    ].forEach(({b,v}, i) => it(`should be able to handle case #${i}`, () => assert.deepEqual(
      graphBinaryReader.readResponse( from(b) ),
      v,
    )));

  });

});
