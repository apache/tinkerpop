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
import { Vertex, Edge, VertexProperty, CompositePDT, PrimitivePDT } from '../../lib/structure/graph.js';
import { getClient, serverUrl } from '../helper.js';
import { cardinality } from '../../lib/process/traversal.js';
import Client from '../../lib/driver/client.js';
import { Int, Double } from '../../lib/utils.js';

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

  describe('#submit() with preciseNumbers', function () {
    let preciseClient;

    before(async function () {
      preciseClient = new Client(serverUrl, { traversalSource: 'gmodern', preciseNumbers: true });
      await preciseClient.open();
    });

    after(async function () {
      await preciseClient.close();
    });

    it('should return Int wrapper for integer vertex property', async function () {
      const result = await preciseClient.submit('g.V().has("name", "marko").values("age")');
      assert.ok(result);
      assert.ok(result.first() instanceof Int);
      assert.strictEqual(result.first().value, 29);
    });

    it('should return Double wrapper for edge weight property', async function () {
      const result = await preciseClient.submit('g.E().has("weight", 0.5).limit(1)');
      assert.ok(result);
      const edge = result.first();
      assert.ok(edge instanceof Edge);
      assert.ok(edge.properties[0].value instanceof Double);
      assert.strictEqual(edge.properties[0].value.value, 0.5);
    });
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

describe('CompositePDT - Client', function () {
  let pdtClient;
  before(function () {
    pdtClient = getClient('gmodern');
    return pdtClient.open();
  });
  after(function () {
    return pdtClient.close();
  });

  it('should round-trip a simple Point PDT', function () {
    return pdtClient.submit('g.inject(PDT("Point", ["x":1, "y":2]))')
      .then(function (result) {
        assert.strictEqual(result.length, 1);
        const pdt = result.first();
        assert.ok(pdt instanceof CompositePDT);
        assert.strictEqual(pdt.name, 'Point');
        assert.strictEqual(pdt.fields.x, 1);
        assert.strictEqual(pdt.fields.y, 2);
      });
  });

  it('should round-trip a nested PDT (Person with Address)', function () {
    return pdtClient.submit(
      'g.inject(PDT("Person", ["name":"Alice", "age":30, ' +
      '"address":PDT("Address", ["street":"123 Main St", "city":"Springfield", "zip":"12345"])]))')
      .then(function (result) {
        assert.strictEqual(result.length, 1);
        const pdt = result.first();
        assert.ok(pdt instanceof CompositePDT);
        assert.strictEqual(pdt.name, 'Person');
        assert.strictEqual(pdt.fields.name, 'Alice');
        assert.strictEqual(pdt.fields.age, 30);

        const address = pdt.fields.address;
        assert.ok(address instanceof CompositePDT);
        assert.strictEqual(address.name, 'Address');
        assert.strictEqual(address.fields.street, '123 Main St');
        assert.strictEqual(address.fields.city, 'Springfield');
        assert.strictEqual(address.fields.zip, '12345');
      });
  });

  it('should handle PDTs in a collection', function () {
    return pdtClient.submit(
      'g.inject([PDT("Point", ["x":1, "y":2]), PDT("Point", ["x":3, "y":4])])')
      .then(function (result) {
        assert.strictEqual(result.length, 1);
        const list = result.first();
        assert.ok(Array.isArray(list));
        assert.strictEqual(list.length, 2);

        assert.ok(list[0] instanceof CompositePDT);
        assert.strictEqual(list[0].name, 'Point');
        assert.strictEqual(list[0].fields.x, 1);
        assert.strictEqual(list[0].fields.y, 2);

        assert.ok(list[1] instanceof CompositePDT);
        assert.strictEqual(list[1].name, 'Point');
        assert.strictEqual(list[1].fields.x, 3);
        assert.strictEqual(list[1].fields.y, 4);
      });
  });
});

describe('Client interceptor integration', function () {
  it('should auto serialize request message with interceptor mutation', async function () {
    const { RequestMessage } = await import('../../lib/driver/request-message.js');
    const interceptor = (request) => {
      if (request.body instanceof RequestMessage) {
        request.body = RequestMessage.build('g.inject(99)').addG('gmodern').create();
      }
    };
    const interceptorClient = new Client(serverUrl, {
      traversalSource: 'gmodern',
      interceptors: [interceptor],
    });
    await interceptorClient.open();
    try {
      const result = await interceptorClient.submit('g.inject(1)');
      assert.strictEqual(result.first(), 99);
    } finally {
      await interceptorClient.close();
    }
  });

  it('should propagate exception thrown during interceptor', async function () {
    let callCount = 0;
    const interceptor = () => {
      callCount++;
      if (callCount === 1) {
        throw new Error('interceptor broke');
      }
    };
    const interceptorClient = new Client(serverUrl, {
      traversalSource: 'gmodern',
      interceptors: [interceptor],
    });
    await interceptorClient.open();
    try {
      // First request should fail with interceptor error
      await assert.rejects(
        () => interceptorClient.submit('g.inject(1)'),
        (err) => {
          assert.ok(err.message.includes('interceptor') || err.message.includes('broke'),
            `Expected error about interceptor, got: ${err.message}`);
          return true;
        }
      );

      // Subsequent request should succeed, proving connection recovery
      const result = await interceptorClient.submit('g.inject(2)');
      assert.strictEqual(result.first(), 2);
    } finally {
      await interceptorClient.close();
    }
  });

  it('should propagate error when interceptor sets unsupported body type', async function () {
    let callCount = 0;
    const interceptor = (request) => {
      callCount++;
      if (callCount === 1) {
        request.body = 42;
      }
    };
    const interceptorClient = new Client(serverUrl, {
      traversalSource: 'gmodern',
      interceptors: [interceptor],
    });
    await interceptorClient.open();
    try {
      // First request should fail with serialization error
      await assert.rejects(
        () => interceptorClient.submit('g.inject(1)'),
        (err) => {
          assert.ok(err.message.includes('unsupported body type') || err.message.includes('serialize'),
            `Expected error about unsupported body type, got: ${err.message}`);
          return true;
        }
      );

      // Subsequent request should succeed, proving connection recovery
      const result = await interceptorClient.submit('g.inject(2)');
      assert.strictEqual(result.first(), 2);
    } finally {
      await interceptorClient.close();
    }
  });
});

describe('PrimitivePDT - Client', function () {
  let pdtClient;
  before(function () {
    pdtClient = getClient('gmodern');
    return pdtClient.open();
  });
  after(function () {
    return pdtClient.close();
  });

  it('should round-trip a simple primitive PDT', function () {
    return pdtClient.submit('g.inject(PDT("Uint32","42"))')
      .then(function (result) {
        assert.strictEqual(result.length, 1);
        const pdt = result.first();
        assert.ok(pdt instanceof PrimitivePDT);
        assert.strictEqual(pdt.name, 'Uint32');
        assert.strictEqual(pdt.value, '42');
      });
  });

  it('should round-trip a primitive PDT with leading zeros', function () {
    return pdtClient.submit('g.inject(PDT("TinkerId","007"))')
      .then(function (result) {
        assert.strictEqual(result.length, 1);
        const pdt = result.first();
        assert.ok(pdt instanceof PrimitivePDT);
        assert.strictEqual(pdt.name, 'TinkerId');
        assert.strictEqual(pdt.value, '007');
      });
  });

  it('should handle primitive PDTs in a collection', function () {
    return pdtClient.submit('g.inject([PDT("Uint32","1"), PDT("Uint32","2")])')
      .then(function (result) {
        assert.strictEqual(result.length, 1);
        const list = result.first();
        assert.ok(Array.isArray(list));
        assert.strictEqual(list.length, 2);
        assert.ok(list[0] instanceof PrimitivePDT);
        assert.strictEqual(list[0].value, '1');
        assert.ok(list[1] instanceof PrimitivePDT);
        assert.strictEqual(list[1].value, '2');
      });
  });
});