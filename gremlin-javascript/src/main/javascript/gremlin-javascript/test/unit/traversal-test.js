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
import { expect } from 'chai';
import { Graph } from '../../lib/structure/graph.js';
import anon from '../../lib/process/anonymous-traversal.js';
import { P, order, direction, Traverser, Traversal } from '../../lib/process/traversal.js';
import { statics } from '../../lib/process/graph-traversal.js';
const V = statics.V;
import Bytecode from '../../lib/process/bytecode.js';
import { TraversalStrategies } from '../../lib/process/traversal-strategy.js';
import { RemoteConnection } from '../../lib/driver/remote-connection.js';

describe('Traversal', function () {

  describe('#getByteCode()', function () {
    it('should add steps for with a string parameter', function () {
      const g = anon.traversal().with_(new Graph());
      const bytecode = g.V().out('created').getBytecode();
      assert.ok(bytecode);
      assert.strictEqual(bytecode.sourceInstructions.length, 0);
      assert.strictEqual(bytecode.stepInstructions.length, 2);
      assert.strictEqual(bytecode.stepInstructions[0][0], 'V');
      assert.strictEqual(bytecode.stepInstructions[1][0], 'out');
      assert.strictEqual(bytecode.stepInstructions[1][1], 'created');
    });

    it('should add steps with an enum value', function () {
      const g = anon.traversal().with_(new Graph());
      const bytecode = g.V().order().by('age', order.desc).getBytecode();
      assert.ok(bytecode);
      assert.strictEqual(bytecode.sourceInstructions.length, 0);
      assert.strictEqual(bytecode.stepInstructions.length, 3);
      assert.strictEqual(bytecode.stepInstructions[0][0], 'V');
      assert.strictEqual(bytecode.stepInstructions[1][0], 'order');
      assert.strictEqual(bytecode.stepInstructions[2][0], 'by');
      assert.strictEqual(bytecode.stepInstructions[2][1], 'age');
      assert.strictEqual(typeof bytecode.stepInstructions[2][2], 'object');
      assert.strictEqual(bytecode.stepInstructions[2][2].typeName, 'Order');
      assert.strictEqual(bytecode.stepInstructions[2][2].elementName, 'desc');
    });

    it('should add steps with Direction aliases from_ and to properly mapped to OUT and IN', function () {
      const g = anon.traversal().with_(new Graph());
      const bytecode = g.V().to(direction.from_, 'knows').to(direction.in, 'created').getBytecode();
      assert.ok(bytecode);
      assert.strictEqual(bytecode.sourceInstructions.length, 0);
      assert.strictEqual(bytecode.stepInstructions.length, 3);
      assert.strictEqual(bytecode.stepInstructions[0][0], 'V');
      assert.strictEqual(bytecode.stepInstructions[1][0], 'to');
      assert.strictEqual(typeof bytecode.stepInstructions[1][1], 'object');
      assert.strictEqual(bytecode.stepInstructions[1][1].typeName, 'Direction');
      assert.strictEqual(bytecode.stepInstructions[1][1].elementName, 'OUT');
      assert.strictEqual(bytecode.stepInstructions[1][2], 'knows');
      assert.strictEqual(bytecode.stepInstructions[2][1].typeName, 'Direction');
      assert.strictEqual(bytecode.stepInstructions[2][1].elementName, 'IN');
      assert.strictEqual(bytecode.stepInstructions[2][2], 'created');
    });

    it('should configure OptionStrategy for with_()', function () {
      const g = new Graph().traversal();
      const bytecode = g.with_('x','test').with_('y').V().getBytecode();
      assert.ok(bytecode);
      assert.strictEqual(bytecode.sourceInstructions.length, 1);
      assert.strictEqual(bytecode.sourceInstructions[0][0], 'withStrategies');
      const conf = bytecode.sourceInstructions[0][1].configuration;
      assert.strictEqual(conf.x, 'test');
      assert.strictEqual(conf.y, true);
      assert.strictEqual(bytecode.stepInstructions.length, 1);
      assert.strictEqual(bytecode.stepInstructions[0][0], 'V');
    });
  });

  describe('#next()', function () {
    it('should apply the strategies and return a Promise with the iterator item', function () {
      const strategyMock = {
        apply: function (traversal) {
          traversal.traversers = [ new Traverser(1, 1), new Traverser(2, 1) ];
          return Promise.resolve();
        }
      };
      const strategies = new TraversalStrategies();
      strategies.addStrategy(strategyMock);
      const traversal = new Traversal(null, strategies, null);
      return traversal.next()
        .then(function (item) {
          assert.strictEqual(item.value, 1);
          assert.strictEqual(item.done, false);
          return traversal.next();
        })
        .then(function (item) {
          assert.strictEqual(item.value, 2);
          assert.strictEqual(item.done, false);
          return traversal.next();
        })
        .then(function (item) {
          assert.strictEqual(item.value, null);
          assert.strictEqual(item.done, true);
          return traversal.next();
        });
    });

    it('should support bulk', function () {
      const strategyMock = {
        apply: function (traversal) {
          traversal.traversers = [ new Traverser(1, 2), new Traverser(2, 1) ];
          return Promise.resolve();
        }
      };
      const strategies = new TraversalStrategies();
      strategies.addStrategy(strategyMock);
      const traversal = new Traversal(null, strategies, null);
      return traversal.next()
        .then(function (item) {
          assert.strictEqual(item.value, 1);
          assert.strictEqual(item.done, false);
          return traversal.next();
        })
        .then(function (item) {
          assert.strictEqual(item.value, 1);
          assert.strictEqual(item.done, false);
          return traversal.next();
        })
        .then(function (item) {
          assert.strictEqual(item.value, 2);
          assert.strictEqual(item.done, false);
          return traversal.next();
        })
        .then(function (item) {
          assert.strictEqual(item.value, null);
          assert.strictEqual(item.done, true);
          return traversal.next();
        });
    });
  });

  if (Symbol.asyncIterator) {
    describe('@@asyncIterator', function () {
      it('should expose the async iterator', function () {
        const traversal = new Traversal(null, null, null);
        assert.strictEqual(typeof traversal[Symbol.asyncIterator], 'function');
      });
    });
  }

  describe('#toList()', function () {

    it('should apply the strategies and return a Promise with an array', function () {
      const strategyMock = {
        apply: function (traversal) {
          traversal.traversers = [ new Traverser('a', 1), new Traverser('b', 1) ];
          return Promise.resolve();
        }
      };
      const strategies = new TraversalStrategies();
      strategies.addStrategy(strategyMock);
      const traversal = new Traversal(null, strategies, null);
      return traversal.toList().then(function (list) {
        assert.ok(list);
        assert.deepEqual(list, [ 'a', 'b' ]);
      });
    });

    it('should return an empty array when traversers is empty', function () {
      const strategyMock = {
        apply: function (traversal) {
          traversal.traversers = [];
          return Promise.resolve();
        }
      };
      const strategies = new TraversalStrategies();
      strategies.addStrategy(strategyMock);
      const traversal = new Traversal(null, strategies, null);
      return traversal.toList().then(function (list) {
        assert.ok(Array.isArray(list));
        assert.strictEqual(list.length, 0);
      });
    });

    it('should support bulk', function () {
      const strategyMock = {
        apply: function (traversal) {
          traversal.traversers = [ new Traverser(1, 1), new Traverser(2, 3), new Traverser(3, 2),
            new Traverser(4, 1) ];
          return Promise.resolve();
        }
      };
      const strategies = new TraversalStrategies();
      strategies.addStrategy(strategyMock);
      const traversal = new Traversal(null, strategies, null);
      return traversal.toList()
        .then(list => {
          expect(list).to.have.members([1, 2, 2, 2, 3, 3, 4]);
        });
    });
  });

  describe('#iterate()', function () {
    it('should apply the strategies and return a Promise', function () {
      let applied = false;
      const strategyMock = {
        apply: function (traversal) {
          applied = true;
          traversal.traversers = [ new Traverser('a', 1), new Traverser('b', 1) ];
          return Promise.resolve();
        }
      };
      const strategies = new TraversalStrategies();
      strategies.addStrategy(strategyMock);
      const traversal = new Traversal(null, strategies, new Bytecode());
      return traversal.iterate().then(() => {
        assert.strictEqual(applied, true);
      });
    });
  });

  describe('P#toString()', function () {
    it('convert to string representation with P', function () {
      assert.strictEqual(P.gt(18).toString(), 'gt(18)');
    });

    it('convert to string representation with P.within', function () {
      assert.strictEqual(P.within('a', 'b').toString(), 'within(\'a\',\'b\')');
    });

    it('convert to string representation with P.within array', function () {
      assert.strictEqual(P.within(['a', 'b']).toString(), 'within(\'a\',\'b\')');
    });
  });

  describe("build", function() {
    it('should only allow anonymous child traversals', function() {
      const g = anon.traversal().with_(new Graph());
      assert.doesNotThrow(function() {
        g.V(0).addE("self").to(V(1))
      });

      assert.throws(function() {
        g.V(0).addE("self").to(g.V(1))
      });
    })
  });

  describe('child transactions', function() {
    it('should not support child transactions', function() {
      const g = anon.traversal().with_(new MockRemoteConnection());
      const tx = g.tx();
      assert.throws(function() {
        tx.begin().tx();
      });
    });
  });

  describe('not opened transactions', function() {
    it('should not allow commit for not opened transactions', async function() {
      const g = anon.traversal().withRemote(new MockRemoteConnection());
      const tx = g.tx();
      try {
        await tx.commit();
        assert.fail("should throw error");
      } catch (err) {
        assert.strictEqual('Cannot commit a transaction that is not started', err.message);
      }
    });
    it('should not allow rollback for not opened transactions', async function() {
      const g = anon.traversal().withRemote(new MockRemoteConnection());
      const tx = g.tx();
      try {
        await tx.rollback();
        assert.fail("should throw error");
      } catch (err) {
        assert.strictEqual('Cannot rollback a transaction that is not started', err.message);
      }
    });
  });

  describe('tx#begin()', function() {
    it("should not allow a transaction to begin more than once", function() {
      const g = anon.traversal().with_(new MockRemoteConnection());
      const tx = g.tx();
      tx.begin();
      assert.throws(function () {
        tx.begin();
      });
    });
  });
});

class MockRemoteConnection extends RemoteConnection {
  constructor(bound = false) {
    super('ws://localhost:9998/gremlin');
    this._bound = bound;
  }

  get isSessionBound() {
    return this._bound;
  }

  submit(bytecode) {
    return Promise.resolve(undefined);
  }

  createSession() {
    return new MockRemoteConnection(true);
  }
}