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
const graph = require('../../lib/structure/graph');
const t = require('../../lib/process/traversal.js');
const gs = require('../../lib/structure/io/graph-serializer.js');
const GraphSONReader = gs.GraphSONReader;
const GraphSONWriter = gs.GraphSONWriter;
const P = t.P;

describe('GraphSONReader', function () {
  it('should parse GraphSON int32, float and double to Number from GraphSON', function () {
    const reader = new GraphSONReader();
    [
      [ 'g:Int32', 31 ],
      [ 'g:Float', 31.3],
      [ 'g:Double', 31.2]
    ].forEach(function (item) {
      const result = reader.read({
        "@type": item[0],
        "@value": item[1]
      });
      assert.strictEqual(typeof result, 'number');
      assert.strictEqual(result, item[1]);
    });
  });
  it('should parse GraphSON NaN from GraphSON', function () {
      const reader = new GraphSONReader();
      var result = reader.read({
                "@type": "g:Double",
                "@value": "NaN"
              });
      assert.ok(isNaN(result));
  });
  it('should parse GraphSON -Infinity from GraphSON', function () {
      const reader = new GraphSONReader();
      var result = reader.read({
                "@type": "g:Double",
                "@value": "-Infinity"
              });
      assert.strictEqual(result, Number.NEGATIVE_INFINITY);
  });
  it('should parse GraphSON Infinity from GraphSON', function () {
      const reader = new GraphSONReader();
      var result = reader.read({
                "@type": "g:Double",
                "@value": "Infinity"
              });
      assert.strictEqual(result, Number.POSITIVE_INFINITY);
  });
  it('should parse BulkSet', function() {
      const obj = {"@type": "g:BulkSet", "@value": ["marko", {"@type": "g:Int64", "@value": 1}, "josh", {"@type": "g:Int64", "@value": 3}]};
      const reader = new GraphSONReader();
      const result = reader.read(obj);
      assert.strictEqual(result.length, 4);
      assert.deepStrictEqual(result, ["marko", "josh", "josh", "josh"]);
  });
  it('should parse Date', function() {
    const obj = { "@type" : "g:Date", "@value" : 1481750076295 };
    const reader = new GraphSONReader();
    const result = reader.read(obj);
    assert.ok(result instanceof Date);
  });
  it('should parse vertices from GraphSON', function () {
    const obj = {
      "@type":"g:Vertex", "@value":{"id":{"@type":"g:Int32","@value":1},"label":"person",
        "properties":{"name":[{"id":{"@type":"g:Int64","@value":0},"value":"marko"}],
          "age":[{"id":{"@type":"g:Int64","@value":1},"value":{"@type":"g:Int32","@value":29}}]}}};
    const reader = new GraphSONReader(obj);
    const result = reader.read(obj);
    assert.ok(result instanceof graph.Vertex);
    assert.strictEqual(result.label, 'person');
    assert.strictEqual(typeof result.id, 'number');
    assert.ok(result.properties);
    assert.ok(result.properties['name']);
    assert.strictEqual(result.properties['name'].length, 1);
    assert.strictEqual(result.properties['name'][0].value, 'marko');
    assert.ok(result.properties['age']);
    assert.strictEqual(result.properties['age'].length, 1);
    assert.strictEqual(result.properties['age'][0].value, 29);
  });
  it('should parse paths from GraphSON', function () {
    const obj = {"@type":"g:Path","@value":{"labels":[["a"],["b","c"],[]],"objects":[
      {
        "@type":"g:Vertex","@value":{"id":{"@type":"g:Int32","@value":1},"label":"person",
        "properties":{"name":[{"@type":"g:VertexProperty","@value":{"id":{"@type":"g:Int64","@value":0},
          "value":"marko","label":"name"}}],"age":[{
              "@type":"g:VertexProperty","@value":{"id":{"@type":"g:Int64","@value":1},
              "value":{"@type":"g:Int32","@value":29},"label":"age"}}]}}},
      {
        "@type":"g:Vertex","@value":{"id":{"@type":"g:Int32","@value":3},"label":"software",
        "properties":{"name":[{"@type":"g:VertexProperty","@value":{"id":{"@type":"g:Int64","@value":4},
          "value":"lop","label":"name"}}],
          "lang":[{"@type":"g:VertexProperty","@value":{"id":{"@type":"g:Int64","@value":5},
            "value":"java","label":"lang"}}]}}},
      "lop"]}};
    const reader = new GraphSONReader(obj);
    const result = reader.read(obj);
    assert.ok(result);
    assert.ok(result.objects);
    assert.ok(result.labels);
    assert.strictEqual(result.objects[2], 'lop');
    assert.ok(result.objects[0] instanceof graph.Vertex);
    assert.ok(result.objects[1] instanceof graph.Vertex);
    assert.strictEqual(result.objects[0].label, 'person');
    assert.strictEqual(result.objects[1].label, 'software');
  });
});
describe('GraphSONWriter', function () {
  it('should write numbers', function () {
    const writer = new GraphSONWriter();
    assert.strictEqual(writer.write(2), '2');
  });
  it('should write NaN', function () {
    const writer = new GraphSONWriter();
    const expected = JSON.stringify({ "@type" : "g:Double", "@value" : "NaN" });
    assert.strictEqual(writer.write(NaN), expected);
  });
  it('should write Infinity', function () {
    const writer = new GraphSONWriter();
    const expected = JSON.stringify({ "@type" : "g:Double", "@value" : "Infinity" });
    assert.strictEqual(writer.write(Number.POSITIVE_INFINITY), expected);
  });
  it('should write -Infinity', function () {
    const writer = new GraphSONWriter();
    const expected = JSON.stringify({ "@type" : "g:Double", "@value" : "-Infinity" });
    assert.strictEqual(writer.write(Number.NEGATIVE_INFINITY), expected);
  });
  it('should write Date', function() {
    const writer = new GraphSONWriter();
    const expected = JSON.stringify({ "@type" : "g:Date", "@value" : 1481750076295 });
    assert.strictEqual(writer.write(new Date(1481750076295)), expected);
  });
  it('should write boolean values', function () {
    const writer = new GraphSONWriter();
    assert.strictEqual(writer.write(true), 'true');
    assert.strictEqual(writer.write(false), 'false');
  });
  it('should write enum values', function () {
    const writer = new GraphSONWriter();
    assert.strictEqual(writer.write(t.cardinality.set), '{"@type":"g:Cardinality","@value":"set"}');
  });
  it('should write P', function () {
    const writer = new GraphSONWriter();
    const expected = JSON.stringify({"@type":"g:P","@value":{"predicate":"and","value":[
      {"@type":"g:P","@value":{"predicate":"or","value":[{"@type":"g:P","@value":{"predicate":"lt","value":"b"}},
        {"@type":"g:P","@value":{"predicate":"gt","value":"c"}}]}},
      {"@type":"g:P","@value":{"predicate":"neq","value":"d"}}]}});
    assert.strictEqual(writer.write(P.lt("b").or(P.gt("c")).and(P.neq("d"))), expected);
  });
});