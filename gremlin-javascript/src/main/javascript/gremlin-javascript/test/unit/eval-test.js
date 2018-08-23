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
const expect = require('chai').expect;
const graph = require('../../lib/structure/graph');
const t = require('../../lib/process/traversal');
const TraversalStrategies = require('../../lib/process/traversal-strategy').TraversalStrategies;
const Bytecode = require('../../lib/process/bytecode');

describe('Traversal', function () {

  describe('#getBytecode#toScript()', function () {
    it('should add steps and produce valid script representation', function () {
      const g = new graph.Graph().traversal();
      const script = g.V().out('created').getBytecode().toScript();
      assert.ok(script);
      assert.strictEqual(script.script, 'g.V().out(p1)');
    });

    it('should add steps and produce valid script representation with parameter bindings', function () {
      const g = new graph.Graph().traversal();
      const script = g.addV('name', 'Lilac').getBytecode().toScript();
      assert.ok(script);
      assert.strictEqual(script.script, 'g.addV(p1, p2)');
      assert.ok(script.bindings);
      assert.deepStrictEqual(script.bindings, { p1: 'name', p2: 'Lilac' });
    });

    it('should add steps containing enum and produce valid script representation', function () {
      const g = new graph.Graph().traversal();
      const script = g.V().order().by('age', t.order.decr).getBytecode().toScript();
      assert.ok(script);
      assert.strictEqual(script.script, 'g.V().order().by(p1, decr)');
    });

    it('should add steps containing a predicate and produce valid script representation', function () {
      const g = new graph.Graph().traversal();
      const script = g.V().hasLabel('person').has('age', t.P.gt(30)).getBytecode().toScript();
      assert.ok(script);
      assert.strictEqual(script.script, 'g.V().hasLabel(p1).has(p2, gt(30))');
    });

    it('should take a script and return that script along with passed in bindings', function () {
      const g = new graph.Graph().traversal();
      const bytecode = g.addV('name', 'Lilac').getBytecode();
      const script = bytecode.addStep('eval', [ 
        'g.addV(\'name\', name).property(\'created\', date)',
        { name: 'Lilac', created: '2018-01-01T00:00:00.000z' }
      ]).toScript();
      assert.ok(script);
      assert.strictEqual(script.script, 'g.addV(\'name\', name).property(\'created\', date)');
      assert.deepStrictEqual(script.bindings, { name: 'Lilac', created: '2018-01-01T00:00:00.000z' });
    });
  });

  describe('#eval()', function () {
    it('should apply the strategies and return a Promise with the iterator item', function () {
      const strategyMock = {
        apply: function (traversal) {
          traversal.traversers = [ new t.Traverser(1, 1), new t.Traverser(2, 1) ];
          return Promise.resolve();
        }
      };
      const strategies = new TraversalStrategies();
      strategies.addStrategy(strategyMock);
      const traversal = new t.Traversal(null, strategies, new Bytecode());
      return traversal.eval(null, null, null)
        .then(function (item) {
          assert.strictEqual(item.value, 1);
          assert.strictEqual(item.done, false);
          return traversal.eval(null, null, null);
        })
        .then(function (item) {
          assert.strictEqual(item.value, 2);
          assert.strictEqual(item.done, false);
          return traversal.eval(null, null, null);
        })
        .then(function (item) {
          assert.strictEqual(item.value, null);
          assert.strictEqual(item.done, true);
          return traversal.eval(null, null, null);
        });
    });
  });
});
