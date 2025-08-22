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

const Mocha = require('mocha');
const assert = require('assert');
const { AssertionError } = require('assert');
const DriverRemoteConnection = require('../../lib/driver/driver-remote-connection');
const { Vertex, Edge, VertexProperty} = require('../../lib/structure/graph');
const { traversal } = require('../../lib/process/anonymous-traversal');
const { GraphTraversalSource, GraphTraversal, statics } = require('../../lib/process/graph-traversal');
const { SubgraphStrategy, ReadOnlyStrategy, SeedStrategy,
        ReservedKeysVerificationStrategy, EdgeLabelVerificationStrategy } = require('../../lib/process/traversal-strategy');
const Bytecode = require('../../lib/process/bytecode');
const helper = require('../helper');
const __ = statics;

let connection;
let txConnection;

class SocialTraversal extends GraphTraversal {
  constructor(graph, traversalStrategies, bytecode) {
    super(graph, traversalStrategies, bytecode);
  }

  aged(age) {
    return this.has('person', 'age', age);
  }
}

class SocialTraversalSource extends GraphTraversalSource {
  constructor(graph, traversalStrategies, bytecode) {
    super(graph, traversalStrategies, bytecode, SocialTraversalSource, SocialTraversal);
  }

  person(name) {
    return this.V().has('person', 'name', name);
  }
}

function anonymous() {
  return new SocialTraversal(null, null, new Bytecode());
}

function aged(age) {
  return anonymous().aged(age);
}

describe('Traversal', function () {
  before(function () {
    connection = helper.getConnection('gmodern');
    return connection.open();
  });
  after(function () {
    return connection.close();
  });
  describe("#construct", function () {
    it('should not hang if server not present', function() {
      const g = traversal().withRemote(helper.getDriverRemoteConnection('ws://localhost:9998/gremlin', {traversalSource: 'g'}));
      return g.V().toList().then(function() {
        assert.fail("there is no server so an error should have occurred");
      }).catch(function(err) {
        if (err instanceof AssertionError) throw err;
        assert.strictEqual(err.code, "ECONNREFUSED");
      });
    });
  });
  describe('#toList()', function () {
    it('should submit the traversal and return a list', function () {
      var g = traversal().withRemote(connection);
      return g.V().toList().then(function (list) {
        assert.ok(list);
        assert.strictEqual(list.length, 6);
        list.forEach(v => assert.ok(v instanceof Vertex));
      });
    });
  });
  describe('#clone()', function () {
    it('should reset a traversal when cloned', function () {
      var g = traversal().withRemote(connection);
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
      var g = traversal().withRemote(connection);
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
  describe('#materializeProperties()', function () {
    it('should skip vertex properties when tokens is set', function () {
      var g = traversal().withRemote(connection);
      return g.with_("materializeProperties", "tokens").V().toList().then(function (list) {
        assert.ok(list);
        assert.strictEqual(list.length, 6);
        list.forEach(v => assert.ok(v instanceof Vertex));
        list.forEach(v => assert.ok(v.properties === undefined || v.properties.length === 0));
      });
    });
    it('should skip edge properties when tokens is set', function () {
      var g = traversal().withRemote(connection);
      return g.with_("materializeProperties", "tokens").E().toList().then(function (list) {
        assert.ok(list);
        assert.strictEqual(list.length, 6);
        list.forEach(e => assert.ok(e instanceof Edge));
        // due to the way edge is constructed, edge properties will be {} regardless if it's null or []
        list.forEach(e => assert.strictEqual(Object.keys(e.properties).length, 0));
      });
    });
    it('should skip vertex property properties when tokens is set', function () {
      var g = traversal().withRemote(connection);
      return g.with_("materializeProperties", "tokens").V().properties().toList().then(function (list) {
        assert.ok(list);
        assert.strictEqual(list.length, 12);
        list.forEach(vp => assert.ok(vp instanceof VertexProperty));
        list.forEach(vp => assert.ok(vp.properties === undefined || vp.properties.length === 0));
      });
    });
    it('should skip path element properties when tokens is set', function () {
      var g = traversal().withRemote(connection);
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
        assert.ok(a.properties === undefined || a.properties.length === 0);
        assert.strictEqual(Object.keys(b.properties).length, 0);
        assert.ok(c.properties === undefined || c.properties.length === 0);
      });
    });
    it('should materialize path element properties when all is set', function () {
      var g = traversal().withRemote(connection);
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
        let bWeight;
        if (b.properties instanceof Array) {
          bWeight = b.properties.filter(p => p.key === 'weight');
        } else {
          bWeight = b.properties['weight'];
        }
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
  describe('lambdas', function() {
    it('should handle 1-arg lambdas', function() {
      const g = traversal().withRemote(connection);
      return g.V().has('person','name','marko').values('name').map(() => "it.get()[1]").toList().then(function (s) {
        assert.ok(s);
        assert.strictEqual(s[0], 'a');
      })
    });
  });
  describe('dsl', function() {
    it('should expose DSL methods', function() {
      const g = traversal(SocialTraversalSource).withRemote(connection);
      return g.person('marko').aged(29).values('name').toList().then(function (list) {
          assert.ok(list);
          assert.strictEqual(list.length, 1);
          assert.strictEqual(list[0], 'marko');
        });
    });

    it('should expose anonymous DSL methods', function() {
      const g = traversal(SocialTraversalSource).withRemote(connection);
      return g.person('marko').filter(aged(29)).values('name').toList().then(function (list) {
        assert.ok(list);
        assert.strictEqual(list.length, 1);
        assert.strictEqual(list[0], 'marko');
      });
    });
  });
  describe("more complex traversals", function() {
    it('should return paths of value maps', function() {
      const g = traversal().withRemote(connection);
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
      const g = traversal().withRemote(connection).withStrategies(
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
      const g = traversal().withRemote(connection).withStrategies(new ReadOnlyStrategy());
      return g.addV().iterate().then(() => assert.fail("should have tanked"), (err) => assert.ok(err));
    });
    it('should allow ReservedKeysVerificationStrategy', function() {
      const g = traversal().withRemote(connection).withStrategies(new ReservedKeysVerificationStrategy(false, true));
      return g.addV().property("id", "please-don't-use-id").iterate().then(() => assert.fail("should have tanked"), (err) => assert.ok(err));
    });
    it('should allow EdgeLabelVerificationStrategy', function() {
      const g = traversal().withRemote(connection).withStrategies(new EdgeLabelVerificationStrategy(false, true));
      g.V().outE("created", "knows").count().next().then(function (item1) {
        assert.ok(item1);
        assert.strictEqual(item1.value, 6);
      });
      return g.V().out().iterate().then(() => assert.fail("should have tanked"), (err) => assert.strictEqual(err.statusCode, 500));
    });
    it('should allow with_(evaluationTimeout,10)', function() {
      const g = traversal().withRemote(connection).with_('x').with_('evaluationTimeout', 10);
      return g.V().repeat(__.both()).iterate().then(() => assert.fail("should have tanked"), (err) => assert.strictEqual(err.statusCode, 598));
    });
    it('should allow SeedStrategy', function () {
      const g = traversal().withRemote(connection).withStrategies(new SeedStrategy({seed: 999999}));
      return g.V().coin(0.4).count().next().then(function (item1) {
        assert.ok(item1);
        assert.strictEqual(item1.value, 1);
      }, (err) => assert.fail("tanked: " + err));
    });
  });
  describe("should handle tx errors if graph not support tx", function() {
    it('should throw exception on commit if graph not support tx', async function() {
      const g = traversal().withRemote(connection);
      const tx = g.tx();
      const gtx = tx.begin();
      const result = await g.V().count().next();
      assert.strictEqual(6, result.value);
      try {
        await tx.commit();
        assert.fail("should throw error");
      } catch (err) {
        assert.strictEqual("Server error: Graph does not support transactions (500)", err.message);
      }
    });
    it('should throw exception on rollback if graph not support tx', async function() {
      const g = traversal().withRemote(connection);
      const tx = g.tx();
      tx.begin();
      try {
        await tx.rollback();
        assert.fail("should throw error");
      } catch (err) {
        assert.strictEqual("Server error: Graph does not support transactions (500)", err.message);
      }
    });
  });
  describe('support remote transactions - commit', function() {
    before(function () {
      txConnection = helper.getConnection('gtx');
      return txConnection.open();
    });
    after(function () {
      const g = traversal().withRemote(txConnection);
      return g.V().drop().iterate().then(() => {
        return txConnection.close()
      });
    });
    it('should commit a simple transaction', async function () {
      const g = traversal().withRemote(txConnection);
      const tx = g.tx();
      const gtx = tx.begin();
      await Promise.all([
        gtx.addV("person").property("name", "jorge").iterate(),
        gtx.addV("person").property("name", "josh").iterate()
      ]);

      let r = await gtx.V().count().next();
      // assert within the transaction....
      assert.ok(r);
      assert.strictEqual(r.value, 2);

      // now commit changes to test outside of the transaction
      await tx.commit();

      r = await g.V().count().next();
      assert.ok(r);
      assert.strictEqual(r.value, 2);
      // connection closing async, so need to wait
      while (tx._sessionBasedConnection.isOpen) {
        await new Promise(resolve => setTimeout(resolve, 10));
      }
      assert.ok(!tx._sessionBasedConnection.isOpen);
    });
  });
  describe('support remote transactions - rollback', function() {
    before(function () {

      txConnection = helper.getConnection('gtx');
      return txConnection.open();
    });
    after(function () {
      const g = traversal().withRemote(txConnection);
      return g.V().drop().iterate().then(() => {
        return txConnection.close()
      });
    });
    it('should rollback a simple transaction', async function() {
      const g = traversal().withRemote(txConnection);
      const tx = g.tx();
      const gtx = tx.begin();
      await Promise.all([
        gtx.addV("person").property("name", "jorge").iterate(),
        gtx.addV("person").property("name", "josh").iterate()
      ]);

      let r = await gtx.V().count().next();
      // assert within the transaction....
      assert.ok(r);
      assert.strictEqual(r.value, 2);

      // now rollback changes to test outside of the transaction
      await tx.rollback();

      r = await g.V().count().next();
      assert.ok(r);
      assert.strictEqual(r.value, 0);
      // connection closing async, so need to wait
      while (tx._sessionBasedConnection.isOpen) {
        await new Promise(resolve => setTimeout(resolve, 10));
      }
      assert.ok(!tx._sessionBasedConnection.isOpen);
    });
  });
});