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
import { Vertex, Edge, VertexProperty } from '../../lib/structure/graph.js';
import { getClient } from '../helper.js';
import { cardinality } from '../../lib/process/traversal.js';

let client, clientCrew;

describe('Client', function () {
  before(function () {
    client = getClient('gmodern');
    clientCrew = getClient('gcrew')
    return client.open();
  });
  after(function () {
    clientCrew.close();
    return client.close();
  });
  describe('#submit()', function () {
    it('should send and parse a script', function () {
      return client.submit('g.V().tail()')
        .then(function (result) {
          assert.ok(result);
          assert.strictEqual(result.length, 1);
          assert.ok(result.first() instanceof Vertex);
        });
    });
    it('should send and parse a script with bindings', function () {
      return client.submit('g.V().has("name", x).values("age")', { x: 'marko' })
        .then(function (result) {
          assert.ok(result);
          assert.strictEqual(result.first(), 29);
        });
    });

    it('should retrieve the attributes', () => {
      return client.submit("g.V().tail()")
        .then(rs => {
          assert.ok(rs.attributes instanceof Map);
        });
    });

    it('should skip Vertex properties for request with tokens', function () {
      return client.submit('g.with("materializeProperties", "tokens").V(1)')
        .then(function (result) {
          assert.ok(result);
          assert.strictEqual(result.length, 1);
          const vertex = result.first();
          assert.ok(vertex instanceof Vertex);
          assert.ok(vertex.properties.length === 0);
        });
    });

    it('should handle Vertex properties for gremlin request', function () {
      return client.submit('g.V(1)')
        .then(function (result) {
          assert.ok(result);
          assert.strictEqual(result.length, 1);
          const vertex = result.first();
          assert.ok(vertex instanceof Vertex);
          const age = vertex.properties.find(p => p.key === 'age');
          const name = vertex.properties.find(p => p.key === 'name');
          assert.ok(age);
          assert.ok(name);
          assert.strictEqual(age.value, 29);
          assert.strictEqual(name.value, 'marko');
        });
    });

    it('should handle Edge properties for gremlin request', function () {
      return client.submit('g.E().has("weight", 0.5).limit(1)')
        .then(function (result) {
          assert.ok(result);
          assert.strictEqual(result.length, 1);
          const edge = result.first();
          assert.ok(edge instanceof Edge);
          assert.strictEqual(edge.label, 'knows');
          assert.strictEqual(edge.properties[0].value, 0.5);
          assert.ok(edge.inV);
          assert.ok(edge.outV);
        });
    });

    it('should handle VertexProperty metadata for gremlin request', function () {
      return clientCrew.submit('g.V(7).properties("location").limit(1)')
        .then(function (result) {
          assert.ok(result);
          assert.strictEqual(result.length, 1);
          const prop = result.first();
          assert.ok(prop instanceof VertexProperty);
          assert.strictEqual(prop.key, 'location');
          assert.strictEqual(prop.value, 'centreville');

          const startTime = prop.properties.find(p => p.key === 'startTime');
          const endTime = prop.properties.find(p => p.key === 'endTime');
          assert.ok(startTime);
          assert.ok(endTime);
          assert.strictEqual(startTime.value, 1990);
          assert.strictEqual(endTime.value, 2000);
        });
    });

    it('should skip Vertex properties for gremlin request with tokens', function () {
      return client.submit('g.with("materializeProperties", "tokens").V(1)')
        .then(function (result) {
          assert.ok(result);
          assert.strictEqual(result.length, 1);
          const vertex = result.first();
          assert.ok(vertex instanceof Vertex);
          assert.ok(vertex.properties.length === 0);
        });
    });

    it('should handle VertexProperties properties for gremlin request', async function () {
      const crewClient = getClient('gcrew');
      await crewClient.open();

      const result = await crewClient.submit('g.V(7)');

      assert.ok(result);
      assert.strictEqual(result.length, 1);
      const vertex = result.first();
      
      assertVertexProperties(vertex);

      await crewClient.close();
    });

    // TODO:: Revisit what it means to close a client in HTTP.
    // it("should reject pending traversal promises if connection closes", async () => {
    //   const closingClient = getClient('gmodern');
    //   await closingClient.open();
    //
    //   // verify the client works before closing
    //   const result = await closingClient.submit("g.V().tail()");
    //   assert.ok(result);
    //
    //   await closingClient.close();
    //   assert.ok(!closingClient.isOpen());
    // });
  });
});

function assertVertexProperties(vertex) {
  assert.ok(vertex instanceof Vertex);
  let locations;
  if (vertex.properties instanceof Array) {
    locations = vertex.properties.filter(p => p.key == 'location');
  } else {
    locations = vertex.properties.location
  }
  assert.strictEqual(locations.length, 3);

  const vertexProperty = locations[0];
  assert.strictEqual(vertexProperty.value, 'centreville');
  const start = vertexProperty.properties.find(p => p.key === 'startTime');
  const end = vertexProperty.properties.find(p => p.key === 'endTime');
  assert.ok(start);
  assert.ok(end);
  assert.strictEqual(start.value, 1990);
  assert.strictEqual(end.value, 2000);
}