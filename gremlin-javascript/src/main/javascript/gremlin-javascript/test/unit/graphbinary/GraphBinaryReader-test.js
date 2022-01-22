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

const assert = require('assert');
const { graphBinaryReader } = require('../../../lib/structure/io/binary/GraphBinary');

const { from, concat } = Buffer;

describe('GraphBinary.Reader', () => {

  describe('readResponse', () => {

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

  });

});
