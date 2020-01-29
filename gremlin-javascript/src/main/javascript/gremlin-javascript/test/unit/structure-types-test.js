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

const { assert } = require('chai');
const { VertexProperty, Property, Vertex, Edge, Path } = require('../../lib/structure/graph');

describe('Edge', () => {
  describe('#toString()', () => {
    it('should support empty outV and inV', () => {
      const element = new Edge('123', null, 'label1', undefined, null);
      assert.strictEqual(element.toString(), `e[123][?-label1->?]`);
    });
  });
});

describe('Vertex', () => {
  describe('#toString()', () => {
    it('should return the string representation based on the id', () => {
      const element = new Vertex(-200, 'label1', null);
      assert.strictEqual(element.toString(), `v[-200]`);
    });
  });
});

describe('VertexProperty', () => {
  describe('#toString()', () => {
    it('should return the string representation and summarize', () => {
      [
        [ new VertexProperty(1, 'label1', 'value1'), 'vp[label1->value1]' ],
        [ new VertexProperty(1, 'label2', null), 'vp[label2->null]' ],
        [ new VertexProperty(1, 'label3', undefined), 'vp[label3->undefined]' ],
        [ new VertexProperty(1, 'label4', 'abcdefghijklmnopqrstuvwxyz'), 'vp[label4->abcdefghijklmnopqrst]' ]
      ].forEach(item => {
        assert.strictEqual(item[0].toString(), item[1]);
      });
    });
  });
});

describe('Property', () => {
  describe('#toString()', () => {
    it('should return the string representation and summarize', () => {
      [
        [ new Property('key1', 'value1'), 'p[key1->value1]' ],
        [ new Property('key2', null), 'p[key2->null]' ],
        [ new Property('key3', undefined), 'p[key3->undefined]' ],
        [ new Property('key4', 'abcdefghijklmnopqrstuvwxyz'), 'p[key4->abcdefghijklmnopqrst]' ]
      ].forEach(item => {
        assert.strictEqual(item[0].toString(), item[1]);
      });
    });
  });
});

describe('Path', () => {
  describe('#toString()', () => {
    it('should return the string representation and summarize', () => {
      [
        [ new Path(['a', 'b'], [1, 2]), 'path[1, 2]' ],
        [ new Path(['a', 'b'], null), 'path[]' ]
      ].forEach(item => {
        assert.strictEqual(item[0].toString(), item[1]);
      });
    });
  });
});