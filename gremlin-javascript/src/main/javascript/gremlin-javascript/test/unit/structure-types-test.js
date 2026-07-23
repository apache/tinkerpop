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

  describe('properties', () => {
    it('should default to empty array when not provided', () => {
      const edge = new Edge('123', new Vertex(1), 'knows', new Vertex(2));
      assert.deepStrictEqual(edge.properties, []);
    });

    it('should default to empty array when undefined', () => {
      const edge = new Edge('123', new Vertex(1), 'knows', new Vertex(2), undefined);
      assert.deepStrictEqual(edge.properties, []);
    });
  });

  describe('#propertyMap()', () => {
    it('should group properties by key into single-element arrays', () => {
      const since = new Property('since', 2009);
      const weight = new Property('weight', 0.5);
      const edge = new Edge('123', new Vertex(1), 'knows', new Vertex(2), [since, weight]);
      const pm = edge.propertyMap();
      // propertyMap() returns a null-prototype object, so compare by keys/values
      // instead of deep-equal against a plain {} literal.
      assert.deepStrictEqual(Object.keys(pm), ['since', 'weight']);
      assert.deepStrictEqual(pm.since, [since]);
      assert.deepStrictEqual(pm.weight, [weight]);
    });

    it('should return an empty object when there are no properties', () => {
      const edge = new Edge('123', new Vertex(1), 'knows', new Vertex(2));
      assert.deepStrictEqual(Object.keys(edge.propertyMap()), []);
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

  describe('properties', () => {
    it('should default to empty array when not provided', () => {
      const vertex = new Vertex(1, 'person');
      assert.deepStrictEqual(vertex.properties, []);
    });

    it('should default to empty array when undefined', () => {
      const vertex = new Vertex(1, 'person', undefined);
      assert.deepStrictEqual(vertex.properties, []);
    });

    it('should default to empty array when null', () => {
      const vertex = new Vertex(1, 'person', null);
      assert.deepStrictEqual(vertex.properties, []);
    });
  });

  describe('#propertyMap()', () => {
    it('should group multi-properties of the same key into an array', () => {
      const nameA = new VertexProperty(0, 'name', 'marko');
      const nameB = new VertexProperty(1, 'name', 'marko a. rodriguez');
      const vertex = new Vertex(1, 'person', [nameA, nameB]);
      const pm = vertex.propertyMap();
      // propertyMap() returns a null-prototype object, so compare by keys/values
      // instead of deep-equal against a plain {} literal.
      assert.deepStrictEqual(Object.keys(pm), ['name']);
      assert.deepStrictEqual(pm.name, [nameA, nameB]);
    });

    it('should give single-element arrays for single-valued properties', () => {
      const name = new VertexProperty(0, 'name', 'marko');
      const age = new VertexProperty(1, 'age', 29);
      const vertex = new Vertex(1, 'person', [name, age]);
      const pm = vertex.propertyMap();
      assert.deepStrictEqual(Object.keys(pm), ['name', 'age']);
      assert.deepStrictEqual(pm.name, [name]);
      assert.deepStrictEqual(pm.age, [age]);
    });

    it('should return an empty object when there are no properties', () => {
      const vertex = new Vertex(1, 'person');
      assert.deepStrictEqual(Object.keys(vertex.propertyMap()), []);
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

  describe('properties', () => {
    it('should default to empty array when not provided', () => {
      const vp = new VertexProperty(24, 'name', 'marko');
      assert.deepStrictEqual(vp.properties, []);
    });

    it('should default to empty array when undefined', () => {
      const vp = new VertexProperty(24, 'name', 'marko', undefined);
      assert.deepStrictEqual(vp.properties, []);
    });
  });

  describe('#propertyMap()', () => {
    it('should group properties by key into single-element arrays', () => {
      const startTime = new Property('startTime', 2001);
      const endTime = new Property('endTime', 2004);
      const vp = new VertexProperty(24, 'name', 'marko', [startTime, endTime]);
      const pm = vp.propertyMap();
      // propertyMap() returns a null-prototype object, so compare by keys/values
      // instead of deep-equal against a plain {} literal.
      assert.deepStrictEqual(Object.keys(pm), ['startTime', 'endTime']);
      assert.deepStrictEqual(pm.startTime, [startTime]);
      assert.deepStrictEqual(pm.endTime, [endTime]);
    });

    it('should return an empty object when there are no properties', () => {
      const vp = new VertexProperty(24, 'name', 'marko');
      assert.deepStrictEqual(Object.keys(vp.propertyMap()), []);
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