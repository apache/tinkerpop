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
import { GraphTraversalSource, GraphTraversal } from '../../lib/process/graph-traversal.js';
import { Graph } from '../../lib/structure/graph.js';
import { TraversalStrategies } from '../../lib/process/traversal-strategy.js';
import { Traverser } from '../../lib/process/traversal.js';
import GremlinLang from '../../lib/process/gremlin-lang.js';

const g = new GraphTraversalSource(new Graph(), new TraversalStrategies());

describe('match(String) - declarative pattern matching (ti-70m.13)', function () {
  describe('GraphTraversalSource#match()', function () {
    it('should generate correct Gremlin for g.match(query)', function () {
      const query = 'MATCH (p:person)-[e:knows]->(friend:person)';
      const t = g.match(query);
      assert.strictEqual(
        t.getGremlinLang().getGremlin(),
        "g.match('MATCH (p:person)-[e:knows]->(friend:person)')"
      );
    });

    it('should generate correct Gremlin for g.match(query, params)', function () {
      const query = 'MATCH (p:person)-[e:knows]->(friend:person) WHERE p.name = $name';
      const t = g.match(query, { name: 'marko' });
      assert.strictEqual(
        t.getGremlinLang().getGremlin(),
        "g.match('MATCH (p:person)-[e:knows]->(friend:person) WHERE p.name = $name',['name':'marko'])"
      );
    });

    it('should return a GraphTraversal', function () {
      const t = g.match('MATCH (p:person)');
      assert.ok(t instanceof GraphTraversal);
    });
  });

  describe('GraphTraversal#match()', function () {
    it('should generate correct Gremlin for traversal step match(query)', function () {
      const query = 'MATCH (p:person)-[e:knows]->(friend:person)';
      const t = g.inject(null).match(query);
      assert.strictEqual(
        t.getGremlinLang().getGremlin(),
        "g.inject(null).match('MATCH (p:person)-[e:knows]->(friend:person)')"
      );
    });

    it('should generate correct Gremlin for traversal step match(query, params)', function () {
      const query = 'MATCH (p:person)-[e:knows]->(friend:person)';
      const t = g.inject(null).match(query, { limit: 10 });
      assert.strictEqual(
        t.getGremlinLang().getGremlin(),
        "g.inject(null).match('MATCH (p:person)-[e:knows]->(friend:person)',['limit':10])"
      );
    });
  });

  describe('modern graph simulation', function () {
    // One-off execution test: simulates g.match('MATCH (p:person)-[e:knows]->(friend:person)')
    // as a start step returning binding Maps directly (no select() required).
    it('should return binding Maps with named variables from modern graph', function () {
      // The modern graph has marko knows josh and marko knows vadas.
      // match() returns one Map<variable, value> per result row.
      const row1 = new Map([['p', 'marko'], ['friend', 'josh']]);
      const row2 = new Map([['p', 'marko'], ['friend', 'vadas']]);

      const strategyMock = {
        apply: function (traversal) {
          traversal._resultsStream = (async function* () {
            yield* [new Traverser(row1, 1), new Traverser(row2, 1)];
          })();
          return Promise.resolve();
        }
      };
      const strategies = new TraversalStrategies();
      strategies.addStrategy(strategyMock);

      const gWithStrategy = new GraphTraversalSource(new Graph(), strategies);
      const traversal = gWithStrategy.match('MATCH (p:person)-[e:knows]->(friend:person)');

      assert.strictEqual(
        traversal.getGremlinLang().getGremlin(),
        "g.match('MATCH (p:person)-[e:knows]->(friend:person)')"
      );

      return traversal.toList().then(function (list) {
        assert.strictEqual(list.length, 2);
        assert.ok(list[0] instanceof Map);
        assert.ok(list[0].has('p'));
        assert.ok(list[0].has('friend'));
        assert.deepStrictEqual(list[0], row1);
        assert.deepStrictEqual(list[1], row2);
      });
    });
  });
});
