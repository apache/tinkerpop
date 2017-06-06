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

var assert = require('assert');
var graph = require('../../lib/structure/graph');
var utils = require('../../lib/utils');
var t = require('../../lib/process/traversal');
var TraversalStrategies = require('../../lib/process/traversal-strategy').TraversalStrategies;

describe('Traversal', function () {
  describe('#getByteCode()', function () {
    it('should add steps for with a string parameter', function () {
      var g = new graph.Graph().traversal();
      var bytecode = g.V().out('created').getBytecode();
      assert.ok(bytecode);
      assert.strictEqual(bytecode.sourceInstructions.length, 0);
      assert.strictEqual(bytecode.stepInstructions.length, 2);
      assert.strictEqual(bytecode.stepInstructions[0][0], 'V');
      assert.strictEqual(bytecode.stepInstructions[1][0], 'out');
      assert.strictEqual(bytecode.stepInstructions[1][1], 'created');
    });
    it('should add steps with an enum value', function () {
      var g = new graph.Graph().traversal();
      var bytecode = g.V().order().by('age', t.order.decr).getBytecode();
      assert.ok(bytecode);
      assert.strictEqual(bytecode.sourceInstructions.length, 0);
      assert.strictEqual(bytecode.stepInstructions.length, 3);
      assert.strictEqual(bytecode.stepInstructions[0][0], 'V');
      assert.strictEqual(bytecode.stepInstructions[1][0], 'order');
      assert.strictEqual(bytecode.stepInstructions[2][0], 'by');
      assert.strictEqual(bytecode.stepInstructions[2][1], 'age');
      assert.strictEqual(typeof bytecode.stepInstructions[2][2], 'object');
      assert.strictEqual(bytecode.stepInstructions[2][2].typeName, 'Order');
      assert.strictEqual(bytecode.stepInstructions[2][2].elementName, 'decr');
    });
  });
  describe('#next()', function () {
    it('should apply the strategies and return a Promise with the iterator item', function () {
      var strategyMock = {
        apply: function (traversal) {
          traversal.traversers = [ new t.Traverser(1, 1), new t.Traverser(2, 1) ];
          return utils.resolvedPromise();
        }
      };
      var strategies = new TraversalStrategies();
      strategies.addStrategy(strategyMock);
      var traversal = new t.Traversal(null, strategies, null);
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
  });
  describe('#toList()', function () {
    it('should apply the strategies and return a Promise with an array', function () {
      var strategyMock = {
        apply: function (traversal) {
          traversal.traversers = [ new t.Traverser('a', 1), new t.Traverser('b', 1) ];
          return utils.resolvedPromise();
        }
      };
      var strategies = new TraversalStrategies();
      strategies.addStrategy(strategyMock);
      var traversal = new t.Traversal(null, strategies, null);
      return traversal.toList().then(function (list) {
        assert.ok(list);
        assert.deepEqual(list, [ 'a', 'b' ]);
      });
    });
    it('should return an empty array when traversers is empty', function () {
      var strategyMock = {
        apply: function (traversal) {
          traversal.traversers = [];
          return utils.resolvedPromise();
        }
      };
      var strategies = new TraversalStrategies();
      strategies.addStrategy(strategyMock);
      var traversal = new t.Traversal(null, strategies, null);
      return traversal.toList().then(function (list) {
        assert.ok(Array.isArray(list));
        assert.strictEqual(list.length, 0);
      });
    })
  });
});