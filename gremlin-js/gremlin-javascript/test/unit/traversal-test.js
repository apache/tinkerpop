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
import { expect } from 'chai';
import { Graph, Vertex } from '../../lib/structure/graph.js';
import anon from '../../lib/process/anonymous-traversal.js';
import { P, order, direction, Traverser, Traversal } from '../../lib/process/traversal.js';
import { statics } from '../../lib/process/graph-traversal.js';
const V = statics.V;

import { TraversalStrategies } from '../../lib/process/traversal-strategy.js';
import { RemoteConnection } from '../../lib/driver/remote-connection.js';

async function* traversersToResultStream(traversers) {
  for (const t of traversers) {
    yield t;
  }
}

describe('Traversal', function () {


  describe('#next()', function () {
    it('should apply the strategies and return a Promise with the iterator item', function () {
      const strategyMock = {
        apply: function (traversal) {
          traversal._resultsStream = traversersToResultStream([ new Traverser(1, 1), new Traverser(2, 1) ]);
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
          traversal._resultsStream = traversersToResultStream([ new Traverser(1, 2), new Traverser(2, 1) ]);
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

  describe('#next(amount)', function () {
    function createTraversal(traversers) {
      const strategyMock = {
        apply: function (traversal) {
          traversal._resultsStream = traversersToResultStream(traversers);
          return Promise.resolve();
        }
      };
      const strategies = new TraversalStrategies();
      strategies.addStrategy(strategyMock);
      return new Traversal(null, strategies, null);
    }

    it('should return a Promise with an array of up to amount values', function () {
      const traversal = createTraversal([ new Traverser(1, 1), new Traverser(2, 1), new Traverser(3, 1) ]);
      return traversal.next(2)
        .then(function (batch) {
          assert.deepStrictEqual(batch, [ 1, 2 ]);
          return traversal.next(2);
        })
        .then(function (batch) {
          assert.deepStrictEqual(batch, [ 3 ]);
        });
    });

    it('should return an array of one when amount is one', function () {
      const traversal = createTraversal([ new Traverser(1, 1), new Traverser(2, 1) ]);
      return traversal.next(1).then(function (batch) {
        assert.deepStrictEqual(batch, [ 1 ]);
      });
    });

    it('should expand bulk into separate values', function () {
      const traversal = createTraversal([ new Traverser(1, 2), new Traverser(2, 1) ]);
      return traversal.next(2).then(function (batch) {
        assert.deepStrictEqual(batch, [ 1, 1 ]);
      });
    });

    it('should return only the remaining values when fewer than amount exist', function () {
      const traversal = createTraversal([ new Traverser(1, 1), new Traverser(2, 1) ]);
      return traversal.next(5).then(function (batch) {
        assert.deepStrictEqual(batch, [ 1, 2 ]);
      });
    });

    it('should return an empty array when amount is zero', function () {
      const traversal = createTraversal([ new Traverser(1, 1) ]);
      return traversal.next(0).then(function (batch) {
        assert.deepStrictEqual(batch, []);
      });
    });

    it('should return an empty array when amount is negative', function () {
      const traversal = createTraversal([ new Traverser(1, 1) ]);
      return traversal.next(-1).then(function (batch) {
        assert.deepStrictEqual(batch, []);
      });
    });

    it('should still return an iterator item when called without an amount', function () {
      const traversal = createTraversal([ new Traverser(1, 1) ]);
      return traversal.next().then(function (item) {
        assert.strictEqual(item.value, 1);
        assert.strictEqual(item.done, false);
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
          traversal._resultsStream = traversersToResultStream([ new Traverser('a', 1), new Traverser('b', 1) ]);
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
          traversal._resultsStream = traversersToResultStream([]);
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
          traversal._resultsStream = traversersToResultStream([ new Traverser(1, 1), new Traverser(2, 3), new Traverser(3, 2),
            new Traverser(4, 1) ]);
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
          traversal._resultsStream = traversersToResultStream([ new Traverser('a', 1), new Traverser('b', 1) ]);
          return Promise.resolve();
        }
      };
      const strategies = new TraversalStrategies();
      strategies.addStrategy(strategyMock);
      const traversal = new Traversal(null, strategies);
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
      assert.strictEqual(P.within('a', 'b').toString(), "within(['a', 'b'])");
    });

    it('convert to string representation with P.within array', function () {
      assert.strictEqual(P.within(['a', 'b']).toString(), "within(['a', 'b'])");
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

});

class MockRemoteConnection extends RemoteConnection {
  constructor(bound = false) {
    super('http://localhost:9998/gremlin');
    this._bound = bound;
  }

  get isSessionBound() {
    return this._bound;
  }

  submit(gremlinLang) {
    return Promise.resolve(undefined);
  }

  createSession() {
    return new MockRemoteConnection(true);
  }
}