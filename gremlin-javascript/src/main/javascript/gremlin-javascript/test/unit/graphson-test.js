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

import assert from 'assert';
import { Vertex } from '../../lib/structure/graph.js';
import { P, cardinality } from '../../lib/process/traversal.js';
import { GraphSONReader, GraphSONWriter } from '../../lib/structure/io/graph-serializer.js';

describe('GraphSONReader', function () {
  it('should parse GraphSON null', function () {
    const reader = new GraphSONReader();
    const result = reader.read(null);
    assert.equal(result, null);
  });
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
    assert.ok(result instanceof Vertex);
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
    assert.ok(result.objects[0] instanceof Vertex);
    assert.ok(result.objects[1] instanceof Vertex);
    assert.strictEqual(result.objects[0].label, 'person');
    assert.strictEqual(result.objects[1].label, 'software');
  });
  it('should parse paths from GraphSON3', function () {
    const obj = {
      "@type" : "g:Path",
      "@value" : {
        "labels" : {
          "@type" : "g:List",
          "@value" : [ {
            "@type" : "g:Set",
            "@value" : [ ]
          }, {
            "@type" : "g:Set",
            "@value" : [ ]
          }, {
            "@type" : "g:Set",
            "@value" : [ ]
          } ]
        },
        "objects" : {
          "@type" : "g:List",
          "@value" : [ {
            "@type" : "g:Vertex",
            "@value" : {
              "id" : {
                "@type" : "g:Int32",
                "@value" : 1
              },
              "label" : "person"
            }
          }, {
            "@type" : "g:Vertex",
            "@value" : {
              "id" : {
                "@type" : "g:Int32",
                "@value" : 10
              },
              "label" : "software"
            }
          }, "lop" ]
        }
      }
    };
    const reader = new GraphSONReader(obj);
    const result = reader.read(obj);
    assert.ok(result);
    assert.ok(result.objects);
    assert.ok(result.labels);
    assert.strictEqual(result.objects[2], 'lop');
    assert.ok(result.objects[0] instanceof Vertex);
    assert.ok(result.objects[1] instanceof Vertex);
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
  it('should write list values', function () {
    const writer = new GraphSONWriter();
    const expected = JSON.stringify({"@type": "g:List", "@value": ["marko"]});
    assert.strictEqual(writer.write(["marko"]), expected);
  });
  it('should write enum values', function () {
    const writer = new GraphSONWriter();
    assert.strictEqual(writer.write(cardinality.set), '{"@type":"g:Cardinality","@value":"set"}');
  });
  it('should write P', function () {
    const writer = new GraphSONWriter();
    const expected = JSON.stringify({"@type":"g:P","@value":{"predicate":"and","value":[
      {"@type":"g:P","@value":{"predicate":"or","value":[{"@type":"g:P","@value":{"predicate":"lt","value":"b"}},
        {"@type":"g:P","@value":{"predicate":"gt","value":"c"}}]}},
      {"@type":"g:P","@value":{"predicate":"neq","value":"d"}}]}});
    assert.strictEqual(writer.write(P.lt("b").or(P.gt("c")).and(P.neq("d"))), expected);
  });
  it('should write P.within single', function () {
    const writer = new GraphSONWriter();
    const expected = JSON.stringify({"@type": "g:P", "@value": {"predicate": "within", "value": {"@type": "g:List", "@value": [ "marko" ]}}});
    assert.strictEqual(writer.write(P.within(["marko"])), expected);
    assert.strictEqual(writer.write(P.within("marko")), expected);
  });
  it('should write P.within multiple', function () {
    const writer = new GraphSONWriter();
    const expected = JSON.stringify({"@type": "g:P", "@value": {"predicate": "within", "value": {"@type": "g:List", "@value": [ "marko", "josh"]}}});
    assert.strictEqual(writer.write(P.within(["marko","josh"])), expected);
    assert.strictEqual(writer.write(P.within("marko","josh")), expected);
  });
  it('should write 1-arg lambda values', function () {
    const writer = new GraphSONWriter();
    assert.strictEqual(writer.write(() => 'it.get()'),
        '{"@type":"g:Lambda","@value":{"arguments":-1,"language":"gremlin-groovy","script":"it.get()"}}');
    assert.strictEqual(writer.write(() => 'x -> x.get()'),
        '{"@type":"g:Lambda","@value":{"arguments":1,"language":"gremlin-groovy","script":"x -> x.get()"}}');
  });
  it('should write 2-arg lambda values', function () {
    const writer = new GraphSONWriter();
    assert.strictEqual(writer.write(() => '(x,y) -> x.get() + y'),
        '{"@type":"g:Lambda","@value":{"arguments":2,"language":"gremlin-groovy","script":"(x,y) -> x.get() + y"}}');
  });
});