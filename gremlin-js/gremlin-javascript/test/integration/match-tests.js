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
import { Vertex } from '../../lib/structure/graph.js';
import anon from '../../lib/process/anonymous-traversal.js';
import { getConnection } from '../helper.js';

let connection;

describe('match(String) - declarative pattern matching integration', function () {
  before(function () {
    connection = getConnection('gmodern');
    return connection.open();
  });

  after(function () {
    return connection.close();
  });

  it('should return person-knows-person pairs from the modern graph', function () {
    // The modern graph: marko knows vadas and josh — 2 matching pairs.
    const g = anon.traversal().with_(connection);
    return g.match('MATCH (p:person)-[:knows]->(friend:person)')
      .select('p', 'friend')
      .toList()
      .then(function (list) {
        assert.strictEqual(list.length, 2);
        list.forEach(function (row) {
          assert.ok(row instanceof Map || typeof row === 'object', 'each row should be a Map');
          const p = row instanceof Map ? row.get('p') : row['p'];
          const friend = row instanceof Map ? row.get('friend') : row['friend'];
          assert.ok(p instanceof Vertex, "'p' must be a Vertex");
          assert.ok(friend instanceof Vertex, "'friend' must be a Vertex");
        });
      });
  });

  it('should select only the seeded person vertex', function () {
    // marko is the only person with outgoing knows edges — select('p') returns 2 Vertex rows.
    const g = anon.traversal().with_(connection);
    return g.match('MATCH (p:person)-[:knows]->(friend:person)')
      .select('p')
      .toList()
      .then(function (list) {
        assert.strictEqual(list.length, 2);
        list.forEach(function (v) {
          assert.ok(v instanceof Vertex, "each selected result should be a Vertex");
        });
      });
  });

  it('should support parameterized queries via $name reference', function () {
    // Use a parameterized query to match only marko's friends.
    const g = anon.traversal().with_(connection);
    return g.match('MATCH (p:person {name: $name})-[:knows]->(friend:person)', { name: 'marko' })
      .select('friend')
      .toList()
      .then(function (list) {
        assert.strictEqual(list.length, 2, 'marko has exactly 2 friends (vadas and josh)');
        list.forEach(function (v) {
          assert.ok(v instanceof Vertex, "each selected result should be a Vertex");
        });
      });
  });
});
