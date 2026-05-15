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

import { assert } from 'chai';
import { Buffer } from 'buffer';
import ioc from '../../lib/structure/io/binary/GraphBinary.js';
import StreamReader from '../../lib/structure/io/binary/internals/StreamReader.js';
import { Graph, Vertex, Edge, VertexProperty, Property } from '../../lib/structure/graph.js';

/**
 * Round-trips a value through serialize + StreamReader -> anySerializer.deserialize.
 * Mirrors the pattern used in test/unit/graphbinary/async-deserialize-test.js.
 */
async function roundTripAny(value) {
  const buf = ioc.anySerializer.serialize(value);
  const reader = StreamReader.fromBuffer(buf);
  return ioc.anySerializer.deserialize(reader);
}

describe('Graph', () => {
  describe('toString()', () => {
    it('renders an empty graph with zero counts', () => {
      const graph = new Graph();
      assert.equal(graph.toString(), 'graph[vertices:0 edges:0]');
    });

    it('renders the counts of vertices and edges', () => {
      const graph = new Graph();
      const v1 = new Vertex(1, 'person', []);
      const v2 = new Vertex(2, 'person', []);
      graph.vertices.set(1, v1);
      graph.vertices.set(2, v2);
      graph.edges.set(3, new Edge(3, v1, 'knows', v2, []));
      assert.equal(graph.toString(), 'graph[vertices:2 edges:1]');
    });
  });
});

describe('GraphSerializer', () => {
  describe('round-trip', () => {
    it('preserves vertices, edges, vertex-properties, and properties', async () => {
      const graph = new Graph();

      const v1 = new Vertex(1, 'person', []);
      const v2 = new Vertex(2, 'person', []);
      graph.vertices.set(1, v1);
      graph.vertices.set(2, v2);

      // VertexProperty on v1 with one meta-property
      const vp1 = new VertexProperty(4, 'name', 'marko', [new Property('acl', 'public')]);
      v1.properties.push(vp1);

      // Edge v1 -knows-> v2 with weight property
      const e1 = new Edge(3, v1, 'knows', v2, [new Property('weight', 0.5)]);
      graph.edges.set(3, e1);

      const output = await roundTripAny(graph);

      assert.instanceOf(output, Graph);
      assert.equal(output.vertices.size, 2);
      assert.equal(output.edges.size, 1);

      const rv1 = output.vertices.get(1);
      assert.instanceOf(rv1, Vertex);
      assert.equal(rv1.label, 'person');
      assert.equal(rv1.properties.length, 1);

      const rvp1 = rv1.properties[0];
      assert.instanceOf(rvp1, VertexProperty);
      assert.equal(rvp1.value, 'marko');
      assert.equal(rvp1.label, 'name');
      assert.equal(rvp1.properties.length, 1);
      assert.equal(rvp1.properties[0].key, 'acl');
      assert.equal(rvp1.properties[0].value, 'public');

      const rv2 = output.vertices.get(2);
      assert.instanceOf(rv2, Vertex);
      assert.equal(rv2.label, 'person');

      const re1 = output.edges.get(3);
      assert.instanceOf(re1, Edge);
      assert.equal(re1.label, 'knows');
      assert.equal(re1.outV.id, 1);
      assert.equal(re1.inV.id, 2);
      assert.equal(re1.properties.length, 1);
      assert.equal(re1.properties[0].key, 'weight');
      assert.equal(re1.properties[0].value, 0.5);
    });

    it('handles an empty graph', async () => {
      const graph = new Graph();
      const output = await roundTripAny(graph);

      assert.instanceOf(output, Graph);
      assert.equal(output.vertices.size, 0);
      assert.equal(output.edges.size, 0);
    });

    it('handles vertices with no properties and edges with no properties', async () => {
      const graph = new Graph();
      const v1 = new Vertex(10, 'person', []);
      const v2 = new Vertex(20, 'software', []);
      graph.vertices.set(10, v1);
      graph.vertices.set(20, v2);
      graph.edges.set(30, new Edge(30, v1, 'created', v2, []));

      const output = await roundTripAny(graph);
      assert.instanceOf(output, Graph);
      assert.equal(output.vertices.size, 2);
      assert.equal(output.edges.size, 1);

      const re = output.edges.get(30);
      assert.equal(re.label, 'created');
      assert.equal(re.outV.id, 10);
      assert.equal(re.inV.id, 20);
      // Edge points at the same Vertex instance we already deserialized into the graph
      assert.strictEqual(re.outV, output.vertices.get(10));
      assert.strictEqual(re.inV, output.vertices.get(20));
      assert.equal(re.properties.length, 0);
    });

    it('handles string ids', async () => {
      const graph = new Graph();
      const v1 = new Vertex('a', 'person', []);
      const v2 = new Vertex('b', 'person', []);
      graph.vertices.set('a', v1);
      graph.vertices.set('b', v2);
      graph.edges.set('e1', new Edge('e1', v1, 'knows', v2, []));

      const output = await roundTripAny(graph);
      assert.equal(output.vertices.size, 2);
      assert.equal(output.edges.size, 1);
      assert.equal(output.vertices.get('a').label, 'person');
      assert.equal(output.edges.get('e1').outV.id, 'a');
      assert.equal(output.edges.get('e1').inV.id, 'b');
    });
  });

  describe('null handling', () => {
    it('serializes null as fully-qualified null bytes', () => {
      const buf = ioc.graphSerializer.serialize(null, true);
      assert.deepEqual([...buf], [ioc.DataType.GRAPH, 0x01]);
    });

    it('deserialize() returns null for value_flag=0x01', async () => {
      const buf = Buffer.from([ioc.DataType.GRAPH, 0x01]);
      const reader = StreamReader.fromBuffer(buf);
      const result = await ioc.graphSerializer.deserialize(reader);
      assert.isNull(result);
    });

    it('rejects an unexpected type_code', async () => {
      const buf = Buffer.from([ioc.DataType.VERTEX, 0x00]);
      const reader = StreamReader.fromBuffer(buf);
      try {
        await ioc.graphSerializer.deserialize(reader);
        assert.fail('should have thrown');
      } catch (err) {
        assert.match(err.message, /unexpected \{type_code\}/);
      }
    });
  });

  describe('AnySerializer routing', () => {
    it('selects GraphSerializer for a Graph instance', () => {
      const graph = new Graph();
      const serializer = ioc.anySerializer.getSerializerCanBeUsedFor(graph);
      assert.strictEqual(serializer, ioc.graphSerializer);
    });

    it('serializes a Graph via anySerializer with the GRAPH type code', () => {
      const graph = new Graph();
      const buf = ioc.anySerializer.serialize(graph);
      assert.equal(buf[0], ioc.DataType.GRAPH);
      assert.equal(buf[1], 0x00);
    });
  });
});
