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

var assert = require('assert');
var graph = require('../../lib/structure/graph');
var t = require('../../lib/process/traversal.js');
var gs = require('../../lib/structure/io/graph-serializer.js');
var GraphSONReader = gs.GraphSONReader;
var GraphSONWriter = gs.GraphSONWriter;
var P = t.P;

describe('GraphSONReader', function () {
  it('should parse GraphSON int32, float and double to Number from GraphSON', function () {
    var reader = new GraphSONReader();
    [
      [ 'g:Int32', 31 ],
      [ 'g:Float', 31.3],
      [ 'g:Double', 31.2]
    ].forEach(function (item) {
      var result = reader.read({
        "@type": item[0],
        "@value": item[1]
      });
      assert.strictEqual(typeof result, 'number');
      assert.strictEqual(result, item[1]);
    });
  });
  it('should parse vertices from GraphSON', function () {
    var obj = {
      "@type":"g:Vertex", "@value":{"id":{"@type":"g:Int32","@value":1},"label":"person",
        "properties":{"name":[{"id":{"@type":"g:Int64","@value":0},"value":"marko"}],
          "age":[{"id":{"@type":"g:Int64","@value":1},"value":{"@type":"g:Int32","@value":29}}]}}};
    var reader = new GraphSONReader(obj);
    var result = reader.read(obj);
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
    var obj = {"@type":"g:Path","@value":{"labels":[["a"],["b","c"],[]],"objects":[
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
    var reader = new GraphSONReader(obj);
    var result = reader.read(obj);
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
    var writer = new GraphSONWriter();
    assert.strictEqual(writer.write(2), '2');
  });
  it('should write boolean values', function () {
    var writer = new GraphSONWriter();
    assert.strictEqual(writer.write(true), 'true');
    assert.strictEqual(writer.write(false), 'false');
  });
  it('should write P', function () {
    var writer = new GraphSONWriter();
    var expected = JSON.stringify({"@type":"g:P","@value":{"predicate":"and","value":[
      {"@type":"g:P","@value":{"predicate":"or","value":[{"@type":"g:P","@value":{"predicate":"lt","value":"b"}},
        {"@type":"g:P","@value":{"predicate":"gt","value":"c"}}]}},
      {"@type":"g:P","@value":{"predicate":"neq","value":"d"}}]}});
    assert.strictEqual(writer.write(P.lt("b").or(P.gt("c")).and(P.neq("d"))), expected);
  });
});