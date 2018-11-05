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
 * @author Jorge Bay Gondra
 */
'use strict';

const assert = require('assert');
const util = require('util');
const ResultSet = require('../../lib/driver/result-set');

describe('ResultSet', function () {

  describe('#toArray()', () => {
    it('should return an array of items', () => {
      const items = [ 'a', 'b' ];
      const result = new ResultSet(items);
      assert.ok(Array.isArray(result.toArray()));
      assert.deepStrictEqual(result.toArray(), items);
    });
  });

  describe('#length', () => {
    it('should return the length of the items', () => {
      const items = [ 'a', 'b', 1, 0 ];
      const result = new ResultSet(items);
      assert.strictEqual(result.length, items.length);
    });
  });

  describe('#first()', () => {
    it('should return the first item when there are one or more than one item', () => {
      assert.strictEqual(new ResultSet(['a', 'b']).first(), 'a');
      assert.strictEqual(new ResultSet(['z']).first(), 'z');
    });

    it('should return null when there are no items', () => {
      assert.strictEqual(new ResultSet([]).first(), null);
    });
  });

  describe('#[Symbol.iterator]()', () => {
    it('should support be iterable', () => {
      const items = [ 1, 2, 3 ];
      const result = new ResultSet(items);
      const obtained = [];
      for (let item of result) {
        obtained.push(item);
      }

      assert.deepStrictEqual(obtained, items);
      assert.deepStrictEqual(Array.from(result), items);
    });
  });

  describe('#[util.inspect.custom]()', () => {
    it('should return the Array representation', () => {
      assert.strictEqual(util.inspect(new ResultSet([ 1, 2, 3 ])), '[ 1, 2, 3 ]');
    });
  });

  describe('#attributes', () => {
    it('should default to an empty Map when not defined', () => {
      const rs = new ResultSet([]);
      assert.ok(rs.attributes instanceof Map);
      assert.strictEqual(rs.attributes.size, 0);
    });

    it('should return the attributes when defined', () => {
      const attributes = new Map([['a', 1], ['b', 1]]);
      const rs = new ResultSet([], attributes);
      assert.ok(rs.attributes instanceof Map);
      assert.strictEqual(rs.attributes, attributes);
    });
  });
});