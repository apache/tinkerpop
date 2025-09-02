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

'use strict';

const assert = require('assert');
const Bytecode = require('../../lib/process/bytecode');
const graphModule = require('../../lib/structure/graph');
const helper = require('../helper');
const t = require('../../lib/process/traversal');

let client, clientCrew;

describe('Client', function () {
  before(function () {
    client = helper.getClient('gmodern');
    clientCrew = helper.getClient('gcrew')
    return client.open();
  });
  after(function () {
    clientCrew.close();
    return client.close();
  });
  describe('#submit()', function () {
    it('should send bytecode', function () {
      return client.submit(new Bytecode().addStep('V', []).addStep('tail', []))
        .then(function (result) {
          assert.ok(result);
          assert.strictEqual(result.length, 1);
          assert.ok(result.first().object instanceof graphModule.Vertex);
        });
    });
    it('should send and parse a script', function () {
      return client.submit('g.V().tail()')
        .then(function (result) {
          assert.ok(result);
          assert.strictEqual(result.length, 1);
          assert.ok(result.first() instanceof graphModule.Vertex);
        });
    });
    it('should send and parse a script with bindings', function () {
      return client.submit('x + x', { x: 3 })
        .then(function (result) {
          assert.ok(result);
          assert.strictEqual(result.first(), 6);
        });
    });
    it('should send and parse a script with non-native javascript bindings', function () {
      return client.submit('card.class.simpleName + ":" + card', { card: t.cardinality.set } )
        .then(function (result) {
          assert.ok(result);
          assert.strictEqual(result.first(), 'Cardinality:set');
        });
    });

    it('should retrieve the attributes', () => {
      return client.submit(new Bytecode().addStep('V', []).addStep('tail', []))
        .then(rs => {
          assert.ok(rs.attributes instanceof Map);
          assert.ok(rs.attributes.get('host'));
        });
    });

    it('should handle Vertex properties for bytecode request', function () {
      return client.submit(new Bytecode().addStep('V', [1]))
        .then(function (result) {
          assert.ok(result);
          assert.strictEqual(result.length, 1);
          const vertex = result.first().object;
          assert.ok(vertex instanceof graphModule.Vertex);
          let age, name
          if (vertex.properties instanceof Array) {
            const ageProps = vertex.properties.filter(p => p.key === 'age');
            const nameProps = vertex.properties.filter(p => p.key === 'name');
            age = ageProps[0];
            name = nameProps[0];
          } else {
            age = vertex.properties.age[0]
            name = vertex.properties.name[0]
          }
          assert.ok(age);
          assert.ok(name);
          assert.strictEqual(age.value, 29);
          assert.strictEqual(name.value, 'marko');
        });
    });

    it('should skip Vertex properties for bytecode request with tokens', function () {
      return client.submit(new Bytecode().addStep('V', [1]), null, {'materializeProperties': 'tokens'})
        .then(function (result) {
          assert.ok(result);
          assert.strictEqual(result.length, 1);
          const vertex = result.first().object;
          assert.ok(vertex instanceof graphModule.Vertex);
          assert.ok(vertex.properties === undefined || vertex.properties.length === 0);
        });
    });

    it('should handle Vertex properties for gremlin request', function () {
      return client.submit('g.V(1)')
        .then(function (result) {
          assert.ok(result);
          assert.strictEqual(result.length, 1);
          const vertex = result.first();
          assert.ok(vertex instanceof graphModule.Vertex);
          // if/then until TINKERPOP-3186
          let age, name
          if (vertex.properties instanceof Array) {
            const ageProps = vertex.properties.filter(p => p.key === 'age');
            const nameProps = vertex.properties.filter(p => p.key === 'name');
            age = ageProps[0];
            name = nameProps[0];
          } else {
            age = vertex.properties.age[0]
            name = vertex.properties.name[0]
          }
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
          assert.ok(edge instanceof graphModule.Edge);
          assert.strictEqual(edge.label, 'knows');
          assert.strictEqual(edge.properties.weight, 0.5);
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
          assert.ok(prop instanceof graphModule.VertexProperty);
          assert.strictEqual(prop.key, 'location');
          assert.strictEqual(prop.value, 'centreville');

          // Check meta-properties - TINKERPOP-3186
          if (prop.properties instanceof Object && !(prop.properties instanceof Array)) {
            assert.strictEqual(prop.properties.startTime, 1990);
            assert.strictEqual(prop.properties.endTime, 2000);
          } else {
            const startTime = prop.properties.find(p => p.key === 'startTime');
            const endTime = prop.properties.find(p => p.key === 'endTime');
            assert.ok(startTime);
            assert.ok(endTime);
            assert.strictEqual(startTime.value, 1990);
            assert.strictEqual(endTime.value, 2000);
          }
        });
    });

    it('should skip Vertex properties for gremlin request with tokens', function () {
      return client.submit('g.with("materializeProperties", "tokens").V(1)')
        .then(function (result) {
          assert.ok(result);
          assert.strictEqual(result.length, 1);
          const vertex = result.first();
          assert.ok(vertex instanceof graphModule.Vertex);
          assert.ok(vertex.properties === undefined || vertex.properties.length === 0);
        });
    });

    it('should handle VertexProperties properties for gremlin request', async function () {
      const crewClient = helper.getClient('gcrew');
      await crewClient.open();

      const result = await crewClient.submit('g.V(7)');

      assert.ok(result);
      assert.strictEqual(result.length, 1);
      const vertex = result.first();
      
      assertVertexProperties(vertex);

      await crewClient.close();
    });

    it('should handle VertexProperties properties for bytecode request', async function () {
      const crewClient = helper.getClient('gcrew');
      await crewClient.open();

      const result = await crewClient.submit(new Bytecode().addStep('V', [7]));

      assert.ok(result);
      assert.strictEqual(result.length, 1);
      const vertex = result.first().object;
      
      assertVertexProperties(vertex);

      await crewClient.close();
    });

    it('should be able to stream results from the gremlin server', (done) => {
      const output = [];
      let calls = 0;
      const readable = client.stream('g.V().limit(3)', {}, { batchSize: 2 });

      readable.on('data', (data) => {
        calls += 1;
        data.toArray().forEach(v => output.push(v))
      })

      readable.on('end', () => {
        assert.strictEqual(calls, 2); // limit of 3 with batchSize of 2 should be two function calls
        assert.strictEqual(output.length, 3);
        assert.ok(output[0] instanceof graphModule.Vertex);
        done();
      })
    });

    it("should be able to iterate stream results async", async () => {
      const output = [];
      let calls = 0;
      const readable = client.stream("g.V().limit(3)", {}, { batchSize: 2 });

      for await (const result of readable) {
        calls += 1;
        result.toArray().forEach((v) => output.push(v));
      }

      assert.strictEqual(calls, 2); // limit of 3 with batchSize of 2 should be two function calls
      assert.strictEqual(output.length, 3);
      assert.ok(output[0] instanceof graphModule.Vertex);
    });

    it("should get error for malformed requestId for script stream", async () => {
      try {
        const readable = client.stream('g.V()', {}, {requestId: 'malformed'});
        for await (const result of readable) {
          assert.fail("malformed requestId should throw");
        }
      } catch (e) {
        assert.ok(e);
        assert.ok(e.message);
        assert.ok(e.message.includes("is not a valid UUID."));
      }
    });

    it("should get error for malformed requestId for script submit", async () => {
      try {
        await client.submit('g.V()', {}, {requestId: 'malformed'});
        assert.fail("malformed requestId should throw");
      } catch (e) {
        assert.ok(e);
        assert.ok(e.message);
        assert.ok(e.message.includes("is not a valid UUID."));
      }
    });

    it("should get error for malformed requestId for bytecode stream", async () => {
      try {
        const readable = client.stream(new Bytecode().addStep('V', []), {}, {requestId: 'malformed'});
        for await (const result of readable) {
          assert.fail("malformed requestId should throw");
        }
      } catch (e) {
        assert.ok(e);
        assert.ok(e.message);
        assert.ok(e.message.includes("is not a valid UUID."));
      }
    });

    it("should get error for malformed requestId for bytecode submit", async () => {
      try {
        await client.submit(new Bytecode().addStep('V', []), {}, {requestId: 'malformed'});
        assert.fail("malformed requestId should throw");
      } catch (e) {
        assert.ok(e);
        assert.ok(e.message);
        assert.ok(e.message.includes("is not a valid UUID."));
      }
    });

    it("should reject pending traversal promises if connection closes", async () => {
      const closingClient = helper.getClient('gmodern');
      await closingClient.open();
      const timeout = 10000;
      const startTime = Date.now();
      let isRejected = false;
      
      const pending = async function submitTraversals() {
        while (Date.now() < startTime + timeout) {
          try {
            await closingClient.submit(new Bytecode().addStep('V', []).addStep('tail', []));
          } catch (e) {
            isRejected = true;
            return;
          }
        }
      };
      const pendingPromise = pending();

      await closingClient.close();
      await pendingPromise;
      assert.strictEqual(isRejected, true);
    });

    it("should end streams on traversals if connection closes", async () => {
      const closingClient = helper.getClient('gmodern');
      await closingClient.open();
      let isRejected = false;

      const readable = client.stream('g.V().limit(3)', {}, { batchSize: 2 });

      readable.on('end', () => {
        isRejected = true;
      });

      await closingClient.close();
      for await (const result of readable) {
        // Consume the stream
      }

      assert.strictEqual(isRejected, true);
    });
  });
});

function assertVertexProperties(vertex) {
  assert.ok(vertex instanceof graphModule.Vertex);
  let locations;
  if (vertex.properties instanceof Array) {
    locations = vertex.properties.filter(p => p.key == 'location');
  } else {
    locations = vertex.properties.location
  }
  assert.strictEqual(locations.length, 3);

  const vertexProperty = locations[0];
  assert.strictEqual(vertexProperty.value, 'centreville');
  if (vertexProperty.properties instanceof Array) {
    const start = vertexProperty.properties.find(p => p.key === 'startTime');
    const end = vertexProperty.properties.find(p => p.key === 'endTime');
    assert.ok(start);
    assert.ok(end);
    assert.strictEqual(start.value, 1990);
    assert.strictEqual(end.value, 2000);
  } else {
    assert.strictEqual(vertexProperty.properties.startTime, 1990);
    assert.strictEqual(vertexProperty.properties.endTime, 2000);
  }
}