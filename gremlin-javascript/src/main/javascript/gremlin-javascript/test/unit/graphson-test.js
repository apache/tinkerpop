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
const utils = require('../../lib/utils');
const { ConnectiveStrategy, SeedStrategy } = require("../../lib/process/traversal-strategy");
const GraphSONReader = gs.GraphSONReader;
const GraphSONWriter = gs.GraphSONWriter;
const P = t.P;

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
      "@type":"g:Vertex","@value":{"id":{"@type":"g:Int32","@value":1},"label":"person",
        "properties":{"name":[{"@type":"g:VertexProperty","@value":{"id":{"@type":"g:Int64","@value":0},
              "value":"marko","label":"name"}}],"age":[{
            "@type":"g:VertexProperty","@value":{"id":{"@type":"g:Int64","@value":1},
              "value":{"@type":"g:Int32","@value":29},"label":"age"}}]}}}
    const reader = new GraphSONReader(obj);
    const result = reader.read(obj);
    assert.ok(result instanceof graph.Vertex);
    assert.strictEqual(result.label, 'person');
    assert.strictEqual(typeof result.id, 'number');
    assert.ok(result.properties);
    assert.ok(result.properties[0]);
    assert.strictEqual(result.properties[0].key, 'name');
    assert.strictEqual(result.properties[0].value, 'marko');
    assert.ok(result.properties[1]);
    assert.strictEqual(result.properties[1].key, 'age');
    assert.strictEqual(result.properties[1].value, 29);
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
    const a = result.objects[0];
    const bc = result.objects[1];
    assert.ok(a instanceof graph.Vertex);
    assert.ok(bc instanceof graph.Vertex);
    assert.strictEqual(a.label, 'person');
    assert.strictEqual(bc.label, 'software');
    assert.ok(a.properties);
    assert.ok(a.properties[0]);
    assert.strictEqual(a.properties[0].key, 'name');
    assert.strictEqual(a.properties[0].value, 'marko');
    assert.ok(a.properties[1]);
    assert.strictEqual(a.properties[1].key, 'age');
    assert.strictEqual(a.properties[1].value, 29);
    assert.ok(bc.properties);
    assert.ok(bc.properties[0]);
    assert.strictEqual(bc.properties[0].key, 'name');
    assert.strictEqual(bc.properties[0].value, 'lop');
    assert.ok(bc.properties[1]);
    assert.strictEqual(bc.properties[1].key, 'lang');
    assert.strictEqual(bc.properties[1].value, 'java');
  });
  it('should parse paths from GraphSON3 without properties', function () {
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
    const a = result.objects[0];
    const bc = result.objects[1];
    assert.ok(a instanceof graph.Vertex);
    assert.ok(bc instanceof graph.Vertex);
    assert.strictEqual(a.label, 'person');
    assert.strictEqual(bc.label, 'software');
    assert.deepStrictEqual(a.properties, []);
    assert.deepStrictEqual(bc.properties, []);
  });

  it('should deserialize vertices without properties to empty array', function() {
    const obj = {"@type":"g:Vertex", "@value":{"id":{"@type":"g:Int32","@value":2},"label":"person"}};
    const reader = new GraphSONReader();
    const vertex = reader.read(obj);
    assert.strictEqual(vertex.constructor.name, 'Vertex');
    assert.strictEqual(vertex.label, 'person');
    assert.strictEqual(vertex.id, 2);
    assert.deepStrictEqual(vertex.properties, []);
  });

  it('should deserialize edges without properties to empty array', function() {
    const obj = {"@type":"g:Edge", "@value":{"id":{"@type":"g:Int64","@value":18},"label":"knows","inV":"a","outV":"b","inVLabel":"xLab"}};
    const reader = new GraphSONReader();
    const edge = reader.read(obj);
    assert.strictEqual(edge.constructor.name, 'Edge');
    assert.deepStrictEqual(edge.properties, []);
  });

  it('should deserialize vertex properties without meta-properties to empty array', function() {
    const obj = {"@type":"g:VertexProperty", "@value":{"id":"anId","label":"aKey","value":true,"vertex":{"@type":"g:Int32","@value":9}}};
    const reader = new GraphSONReader();
    const vp = reader.read(obj);
    assert.strictEqual(vp.constructor.name, 'VertexProperty');
    assert.deepStrictEqual(vp.properties, []);
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
  it('should write Date (as OffsetDateTime', function() {
    const writer = new GraphSONWriter();
    const expected = JSON.stringify({ "@type" : "gx:OffsetDateTime", "@value" : "2016-12-14T21:14:36.295Z" });
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
  it('should write Class values', function () {
    const writer = new GraphSONWriter();
    const expected = JSON.stringify({"@type": "g:Class", "@value": "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ConnectiveStrategy"});
    assert.strictEqual(writer.write(ConnectiveStrategy), expected);
  });
  it('should write TraversalStrategy values', function () {
    const writer = new GraphSONWriter();
    const expected = JSON.stringify({"@type": "g:SeedStrategy", "@value": {"fqcn": "org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SeedStrategy", "conf": {"seed":100}}});
    assert.strictEqual(writer.write(new SeedStrategy({seed: 100})), expected);
  });
});