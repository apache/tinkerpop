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

import assert from 'assert';
import { deepEqual, Long } from '../../lib/utils.js';

describe('deepEqual', function () {
  it('compares primitives with Object.is semantics', function () {
    assert.ok(deepEqual(1, 1));
    assert.ok(deepEqual('a', 'a'));
    assert.ok(deepEqual(NaN, NaN));
    assert.ok(!deepEqual(0, -0));
    assert.ok(!deepEqual(1, 2));
    assert.ok(!deepEqual(1, '1'));
    assert.ok(deepEqual(null, null));
    assert.ok(!deepEqual(null, undefined));
  });

  it('compares arrays element-wise', function () {
    assert.ok(deepEqual([1, [2, 3]], [1, [2, 3]]));
    assert.ok(!deepEqual([1, 2], [1, 2, 3]));
    assert.ok(!deepEqual([1, 2], [2, 1]));
  });

  it('compares plain objects by own keys, ignoring order', function () {
    assert.ok(deepEqual({ a: 1, b: 2 }, { b: 2, a: 1 }));
    assert.ok(!deepEqual({ a: 1 }, { a: 1, b: 2 }));
    assert.ok(!deepEqual({ a: undefined }, {}));
  });

  it('compares Map and Set contents', function () {
    assert.ok(deepEqual(new Map([['a', 1]]), new Map([['a', 1]])));
    assert.ok(!deepEqual(new Map([['a', 1]]), new Map([['a', 2]])));
    assert.ok(deepEqual(new Set([1, 2]), new Set([2, 1])));
    assert.ok(!deepEqual(new Set([1, 2]), new Set([1, 3])));
  });

  it('compares Date and RegExp by value', function () {
    assert.ok(deepEqual(new Date(2020, 0, 1), new Date(2020, 0, 1)));
    assert.ok(!deepEqual(new Date(2020, 0, 1), new Date(2020, 0, 2)));
    assert.ok(deepEqual(/abc/gi, /abc/gi));
    assert.ok(!deepEqual(/abc/g, /abc/i));
  });

  it('compares typed arrays / Buffer by contents', function () {
    assert.ok(deepEqual(Uint8Array.from([1, 2, 3]), Uint8Array.from([1, 2, 3])));
    assert.ok(!deepEqual(Uint8Array.from([1, 2, 3]), Uint8Array.from([1, 2, 4])));
  });

  it('requires matching prototypes, like isDeepStrictEqual', function () {
    assert.ok(deepEqual(new Long(5), new Long(5)));
    assert.ok(!deepEqual(new Long(5), { value: 5, type: 'long' }));
  });
});
