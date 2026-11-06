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

import assert from 'assert';
import { AssertionError } from 'assert';
import {Edge, Vertex, VertexProperty, CompositePDT, PrimitivePDT} from '../../lib/structure/graph.js';
import { PDTRegistry } from '../../lib/structure/PDTRegistry.js';
import anon from '../../lib/process/anonymous-traversal.js';
import { GraphTraversalSource, GraphTraversal, statics } from '../../lib/process/graph-traversal.js';
import {
  SubgraphStrategy, ReadOnlyStrategy, SeedStrategy, HaltedTraverserStrategy, FilterRankingStrategy,
  OptionsStrategy, ReservedKeysVerificationStrategy, EdgeLabelVerificationStrategy, MatchAlgorithmStrategy
} from '../../lib/process/traversal-strategy.js';
import GremlinLang from '../../lib/process/gremlin-lang.js';
import DriverRemoteConnection from '../../lib/driver/driver-remote-connection.js';
import { getConnection, getDriverRemoteConnection } from '../helper.js';
const __ = statics;

let connection;

class SocialTraversal extends GraphTraversal {
  constructor(graph, traversalStrategies, gremlinLang) {
    super(graph, traversalStrategies, gremlinLang);
  }

  aged(age) {
    return this.has('person', 'age', age);
  }
}

class SocialTraversalSource extends GraphTraversalSource {
  constructor(graph, traversalStrategies, gremlinLang) {
    super(graph, traversalStrategies, gremlinLang, SocialTraversalSource, SocialTraversal);
  }

  person(name) {
    return this.V().has('person', 'name', name);
  }
}

function anonymous() {
  return new SocialTraversal(null, null, new GremlinLang());
}

function aged(age) {
  return anonymous().aged(age);
}

describe('Traversal', function () {
  before(function () {
    connection = getConnection('gmodern');
    return connection.open();
  });
  after(function () {
    return connection.close();
  });
  describe("#construct", function () {
    it('should not hang if server not present', function() {
      const g = anon.traversal().with_(getDriverRemoteConnection('http://localhost:9998/gremlin', {traversalSource: 'g'}));
      return g.V().toList().then(function() {
        assert.fail("there is no server so an error should have occurred");
      }).catch(function(err) {
        if (err instanceof AssertionError) throw err;
        assert.ok(err);
      });
    });
  });
  describe('#toList()', function () {
    it('should submit the traversal and return a list', function () {
      var g = anon.traversal().with_(connection);
      return g.V().toList().then(function (list) {
        assert.ok(list);
        assert.strictEqual(list.length, 6);
        list.forEach(v => assert.ok(v instanceof Vertex));
      });
    });
  });
  describe('#clone()', function () {
    it('should reset a traversal when cloned', function () {
      var g = anon.traversal().with_(connection);
      var t = g.V().count();
      return t.next().then(function (item1) {
        assert.ok(item1);
        assert.strictEqual(item1.value, 6);
        t.clone().next().then(function (item2) {
          assert.ok(item2);
          assert.strictEqual(item2.value, 6);
        });
      });
    });
  });
  describe('#next()', function () {
    it('should submit the traversal and return an iterator', function () {
      var g = anon.traversal().with_(connection);
      var t = g.V().count();
      return t.hasNext()
        .then(function (more) {
          assert.ok(more);
          assert.strictEqual(more, true);
          return t.next();
        }).then(function (item) {
          assert.strictEqual(item.done, false);
          assert.strictEqual(typeof item.value, 'number');
          return t.next();
        }).then(function (item) {
          assert.ok(item);
          assert.strictEqual(item.done, true);
          assert.strictEqual(item.value, null);
        });
    });
  });
  describe('#next(amount)', function () {
    it('should submit the traversal and return a batch of up to amount results', function () {
      var g = anon.traversal().with_(connection);
      var t = g.V();
      return t.next(2)
        .then(function (batch) {
          assert.ok(Array.isArray(batch));
          assert.strictEqual(batch.length, 2);
          batch.forEach(v => assert.ok(v instanceof Vertex));
          return t.next(10);
        }).then(function (batch) {
          // gmodern has 6 vertices, 2 already consumed, so only 4 remain
          assert.strictEqual(batch.length, 4);
          batch.forEach(v => assert.ok(v instanceof Vertex));
          return t.next(2);
        }).then(function (batch) {
          assert.deepStrictEqual(batch, []);
        });
    });
  });
  describe('#materializeProperties()', function () {
    it('should skip vertex properties when tokens is set', function () {
      var g = anon.traversal().with_(connection);
      return g.with_("materializeProperties", "tokens").V().toList().then(function (list) {
        assert.ok(list);
        assert.strictEqual(list.length, 6);
        list.forEach(v => assert.ok(v instanceof Vertex));
        list.forEach(v => assert.strictEqual(v.properties.length, 0));
      });
    });
    it('should skip edge properties when tokens is set', function () {
      var g = anon.traversal().with_(connection);
      return g.with_("materializeProperties", "tokens").E().toList().then(function (list) {
        assert.ok(list);
        assert.strictEqual(list.length, 6);
        list.forEach(e => assert.ok(e instanceof Edge));
        // due to the way edge is constructed, edge properties will be {} or []
        list.forEach(e => assert.strictEqual(Object.keys(e.properties).length, 0));
      });
    });
    it('should skip vertex property properties when tokens is set', function () {
      var g = anon.traversal().with_(connection);
      return g.with_("materializeProperties", "tokens").V().properties().toList().then(function (list) {
        assert.ok(list);
        assert.strictEqual(list.length, 12);
        list.forEach(vp => assert.ok(vp instanceof VertexProperty));
        list.forEach(vp => assert.strictEqual(vp.properties.length, 0));
      });
    });
    it('should skip path element properties when tokens is set', function () {
      var g = anon.traversal().withRemote(connection);
      return g.with_("materializeProperties", "tokens").V().has('name','marko').outE().inV().hasLabel('software').path().next().then(function (item) {
        const p = item.value;
        assert.ok(p);
        assert.strictEqual(p.objects.length, 3);
        const a = p.objects[0];
        const b = p.objects[1];
        const c = p.objects[2];
        assert.ok(a instanceof Vertex);
        assert.ok(b instanceof Edge);
        assert.ok(c instanceof Vertex);
        assert.strictEqual(a.properties.length, 0);
        assert.strictEqual(Object.keys(b.properties).length, 0);
        assert.strictEqual(c.properties.length, 0);
      });
    });
    it('should materialize path element properties when all is set', function () {
      var g = anon.traversal().withRemote(connection);
      return g.with_("materializeProperties", "all").V().has('name','marko').outE().inV().hasLabel('software').path().next().then(function (item) {
        const p = item.value;
        assert.ok(p);
        assert.strictEqual(p.objects.length, 3);
        const a = p.objects[0];
        const b = p.objects[1];
        const c = p.objects[2];
        assert.ok(a instanceof Vertex);
        assert.ok(b instanceof Edge);
        assert.ok(c instanceof Vertex);
        assert.ok(a.properties);
        // these assertions are if/then because of how mixed up javascript is right now on serialization
        // standards as discussed on TINKERPOP-3186 - can fix that on a breaking change...until then we
        // just retain existing weirdness.
        let aNameProps, aAgeProps;
        if (a.properties instanceof Array) {
          aNameProps = a.properties.filter(p => p.key === 'name');
          aAgeProps = a.properties.filter(p => p.key === 'age');
        } else {
          aNameProps = a.properties['name'];
          aAgeProps = a.properties['age'];
        }
        assert.ok(aNameProps);
        assert.strictEqual(aNameProps.length, 1);
        assert.strictEqual(aNameProps[0].value, 'marko');
        assert.ok(aAgeProps);
        assert.strictEqual(aAgeProps.length, 1);
        assert.strictEqual(aAgeProps[0].value, 29);
        assert.ok(b.properties);
        const bWeight = b.properties[0].value
        assert.ok(bWeight !== undefined);
        assert.strictEqual(bWeight, 0.4);
        assert.ok(c.properties);
        let cNameProps, cLangProps;
        if (c.properties instanceof Array) {
          cNameProps = c.properties.filter(p => p.key === 'name');
          cLangProps = c.properties.filter(p => p.key === 'lang');
        } else {
          cNameProps = c.properties['name'];
          cLangProps = c.properties['lang'];
        }
        assert.ok(cNameProps);
        assert.strictEqual(cNameProps.length, 1);
        assert.strictEqual(cNameProps[0].value, 'lop');
        assert.ok(cLangProps);
        assert.strictEqual(cLangProps.length, 1);
        assert.strictEqual(cLangProps[0].value, 'java');
      });
    });
  });
  describe('dsl', function() {
    it('should expose DSL methods', function() {
      const g = anon.traversal(SocialTraversalSource).with_(connection);
      return g.person('marko').aged(29).values('name').toList().then(function (list) {
          assert.ok(list);
          assert.strictEqual(list.length, 1);
          assert.strictEqual(list[0], 'marko');
        });
    });

    it('should expose anonymous DSL methods', function() {
      const g = anon.traversal(SocialTraversalSource).with_(connection);
      return g.person('marko').filter(aged(29)).values('name').toList().then(function (list) {
        assert.ok(list);
        assert.strictEqual(list.length, 1);
        assert.strictEqual(list[0], 'marko');
      });
    });
  });
  describe("more complex traversals", function() {
    it('should return paths of value maps', function() {
      const g = anon.traversal().with_(connection);
      return g.V(1).out().order().in_().order().limit(1).path().by(__.valueMap('name')).toList().then(function (list) {
        assert.ok(list);
        assert.strictEqual(list.length, 1);
        assert.strictEqual(list[0].objects[0].get('name')[0], "marko");
        assert.strictEqual(list[0].objects[1].get('name')[0], "vadas");
        assert.strictEqual(list[0].objects[2].get('name')[0], "marko");
      });
    });
  });
  describe("should allow TraversalStrategy definition", function() {
    it('should allow SubgraphStrategy', function() {
      const g = anon.traversal().with_(connection).withStrategies(
          new SubgraphStrategy({vertices:__.hasLabel("person"), edges:__.hasLabel("created")}));
      g.V().count().next().then(function (item1) {
        assert.ok(item1);
        assert.strictEqual(item1.value, 4);
      }, (err) => assert.fail("tanked: " + err));
      g.E().count().next().then(function (item1) {
        assert.ok(item1);
        assert.strictEqual(item1.value, 0);
      }, (err) => assert.fail("tanked: " + err));
      g.V().label().dedup().count().next().then(function (item1) {
        assert.ok(item1);
        assert.strictEqual(item1.value, 1);
      }, (err) => assert.fail("tanked: " + err));
      g.V().label().dedup().next().then(function (item1) {
        assert.ok(item1);
        assert.strictEqual(item1.value, "person");
      }, (err) => assert.fail("tanked: " + err));
    });
    it('should allow ReadOnlyStrategy', function() {
      const g = anon.traversal().with_(connection).withStrategies(new ReadOnlyStrategy());
      return g.addV().iterate().then(() => assert.fail("should have tanked"), (err) => assert.ok(err));
    });
    it('should allow OptionsStrategy', function() {
      const g = anon.traversal().with_(connection).withStrategies(new OptionsStrategy());
      return g.V().count().next().then(function (item1) {
        assert.ok(item1);
        assert.strictEqual(item1.value, 6);
      });
    });
    it('should allow ReservedKeysVerificationStrategy', function() {
      const g = anon.traversal().with_(connection).withStrategies(new ReservedKeysVerificationStrategy({logWarnings: false, throwException: true}));
      return g.addV().property("id", "please-don't-use-id").iterate().then(() => assert.fail("should have tanked"), (err) => assert.ok(err));
    });
    it('should allow EdgeLabelVerificationStrategy', function() {
      const g = anon.traversal().with_(connection).withStrategies(new EdgeLabelVerificationStrategy({logWarnings: false, throwException: true}));
      g.V().outE("created", "knows").count().next().then(function (item1) {
        assert.ok(item1);
        assert.strictEqual(item1.value, 6);
      });
      return g.V().out().iterate().then(() => assert.fail("should have tanked"), (err) => assert.strictEqual(err.statusCode, 500));
    });
    it('should allow with_(timeoutMillis,10)', function() {
      const g = anon.traversal().with_(connection).with_('x').with_('timeoutMillis', 10);
      return g.V().repeat(__.both()).iterate().then(() => assert.fail("should have tanked"), (err) => assert.strictEqual(err.statusCode, 500));
    });
    it('should allow SeedStrategy', function () {
      const g = anon.traversal().with_(connection).withStrategies(new SeedStrategy({seed: 999999}));
      return g.V().coin(0.4).count().next().then(function (item1) {
        assert.ok(item1);
        assert.strictEqual(item1.value, 1);
      }, (err) => assert.fail("tanked: " + err));
    });
    it('should allow without HaltedTraverserStrategy', function() {
      const g = anon.traversal().with_(connection).withoutStrategies(HaltedTraverserStrategy);
      return g.V().count().next().then(function (item1) {
        assert.ok(item1);
        assert.strictEqual(item1.value, 6);
      });
    });
    it('should allow with FilterRankingStrategy', function() {
      const g = anon.traversal().with_(connection).withStrategies(new FilterRankingStrategy());
      return g.V().out().order().dedup().count().next().then(function (item1) {
        assert.ok(item1);
        assert.strictEqual(item1.value, 4);
      });
    });
  });
});

let serverUrl;
if (process.env.DOCKER_ENVIRONMENT === 'true') {
  serverUrl = 'http://gremlin-server-test-js:45940/gremlin';
} else {
  serverUrl = 'http://localhost:45940/gremlin';
}

describe('CompositePDT - Traversal API', function () {
  describe('raw PDT round-trip via Traversal API', function () {
    let pdtConnection;

    before(function () {
      pdtConnection = getConnection('gmodern');
      return pdtConnection.open();
    });
    after(function () {
      return pdtConnection.close();
    });

    it('should round-trip a PDT via g.inject()', async function () {
      const g = anon.traversal().with_(pdtConnection);
      const pdt = new CompositePDT('TestPoint', { x: 1, y: 2 });

      const results = await g.inject(pdt).toList();

      assert.strictEqual(results.length, 1);
      const result = results[0];
      assert.ok(result instanceof CompositePDT);
      assert.strictEqual(result.name, 'TestPoint');
      assert.strictEqual(result.fields.x, 1);
      assert.strictEqual(result.fields.y, 2);
    });
  });

  describe('registry-based round-trip via typed object', function () {
    let pdtConnection;

    class TestPoint {
      constructor(x, y) {
        this.x = x;
        this.y = y;
      }
    }

    before(function () {
      const registry = new PDTRegistry();
      registry.register('TestPoint', {
        serialize: (obj) => ({ x: obj.x, y: obj.y }),
        deserialize: (fields) => new TestPoint(fields.x, fields.y),
      }, TestPoint);
      pdtConnection = new DriverRemoteConnection(serverUrl, {
        traversalSource: 'gmodern',
        pdtRegistry: registry,
      });
      return pdtConnection.open();
    });
    after(function () {
      return pdtConnection.close();
    });

    it('should auto-dehydrate on send and auto-hydrate on receive via registry', async function () {
      const g = anon.traversal().with_(pdtConnection);
      const point = new TestPoint(5, 10);

      const results = await g.inject(point).toList();

      assert.strictEqual(results.length, 1);
      const result = results[0];
      assert.ok(result instanceof TestPoint);
      assert.strictEqual(result.x, 5);
      assert.strictEqual(result.y, 10);
    });
  });
});

describe('PrimitivePDT - Traversal API', function () {
  describe('raw primitive PDT round-trip via Traversal API', function () {
    let pdtConnection;

    before(function () {
      pdtConnection = getConnection('gmodern');
      return pdtConnection.open();
    });
    after(function () {
      return pdtConnection.close();
    });

    it('should round-trip a primitive PDT via g.inject()', async function () {
      const g = anon.traversal().with_(pdtConnection);
      const pdt = new PrimitivePDT('Uint32', '42');

      const results = await g.inject(pdt).toList();

      assert.strictEqual(results.length, 1);
      const result = results[0];
      assert.ok(result instanceof PrimitivePDT);
      assert.strictEqual(result.name, 'Uint32');
      assert.strictEqual(result.value, '42');
    });

    it('should round-trip an unregistered primitive PDT (raw)', async function () {
      const g = anon.traversal().with_(pdtConnection);
      const pdt = new PrimitivePDT('UnregisteredType', 'opaque-value');

      const results = await g.inject(pdt).toList();

      assert.strictEqual(results.length, 1);
      const result = results[0];
      assert.ok(result instanceof PrimitivePDT);
      assert.strictEqual(result.name, 'UnregisteredType');
      assert.strictEqual(result.value, 'opaque-value');
    });
  });

  describe('registry-based primitive round-trip via typed object', function () {
    let pdtConnection;

    class Uint32 {
      constructor(v) {
        this.v = v;
      }
    }

    before(function () {
      const registry = new PDTRegistry();
      registry.registerPrimitive('Uint32', {
        toValue: (obj) => String(obj.v),
        fromValue: (value) => new Uint32(parseInt(value, 10)),
      }, Uint32);
      pdtConnection = new DriverRemoteConnection(serverUrl, {
        traversalSource: 'gmodern',
        pdtRegistry: registry,
      });
      return pdtConnection.open();
    });
    after(function () {
      return pdtConnection.close();
    });

    it('should auto-dehydrate primitive on send and auto-hydrate on receive', async function () {
      const g = anon.traversal().with_(pdtConnection);
      const val = new Uint32(99);

      const results = await g.inject(val).toList();

      assert.strictEqual(results.length, 1);
      const result = results[0];
      assert.ok(result instanceof Uint32);
      assert.strictEqual(result.v, 99);
    });
  });

  describe('nested composite containing primitive PDT', function () {
    let pdtConnection;

    class Uint32 {
      constructor(v) {
        this.v = v;
      }
    }

    before(function () {
      const registry = new PDTRegistry();
      registry.registerPrimitive('Uint32', {
        toValue: (obj) => String(obj.v),
        fromValue: (value) => new Uint32(parseInt(value, 10)),
      }, Uint32);
      pdtConnection = new DriverRemoteConnection(serverUrl, {
        traversalSource: 'gmodern',
        pdtRegistry: registry,
      });
      return pdtConnection.open();
    });
    after(function () {
      return pdtConnection.close();
    });

    it('should hydrate nested primitive inside composite', async function () {
      const g = anon.traversal().with_(pdtConnection);
      const inner = new PrimitivePDT('Uint32', '55');
      const outer = new CompositePDT('Measurement', { unit: 'kg', amount: inner });

      const results = await g.inject(outer).toList();

      assert.strictEqual(results.length, 1);
      const result = results[0];
      assert.ok(result instanceof CompositePDT);
      assert.strictEqual(result.name, 'Measurement');
      assert.strictEqual(result.fields.unit, 'kg');
      // The nested primitive PDT should be hydrated to Uint32
      assert.ok(result.fields.amount instanceof Uint32);
      assert.strictEqual(result.fields.amount.v, 55);
    });
  });
});
