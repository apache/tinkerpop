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
import { Tree } from '../../lib/structure/graph.js';
import anon from '../../lib/process/anonymous-traversal.js';
import { getConnection } from '../helper.js';

let connection;

describe('Tree', function () {
  before(function () {
    connection = getConnection('gmodern');
    return connection.open();
  });
  after(function () {
    return connection.close();
  });

  it('should return a navigable Tree from tree()', async function () {
    const g = anon.traversal().with_(connection);
    const item = await g.V(1).out().out().tree().by('name').next();
    const tree = item.value;
    assert.ok(tree instanceof Tree);
    // read API (camelCase) confirmed against lib/structure/graph.ts
    assert.strictEqual(tree.rootNodes().length, 1);
    assert.strictEqual(tree.nodeCount(), 4); // marko/josh/lop/ripple
    assert.deepStrictEqual(tree.getNodesAtDepth(0), ['marko']);
    // siblings order-insensitive:
    assert.deepStrictEqual(tree.getNodesAtDepth(2).sort(), ['lop', 'ripple']);
    assert.deepStrictEqual(tree.getLeafNodes().sort(), ['lop', 'ripple']);
    assert.strictEqual(tree.isLeaf(), false);
    // whole-output equality; lop/ripple siblings at depth 2 have unspecified order
    const optionA = '|--marko\n   |--josh\n      |--lop\n      |--ripple';
    const optionB = '|--marko\n   |--josh\n      |--ripple\n      |--lop';
    const pretty = tree.prettyPrint();
    assert.ok(pretty === optionA || pretty === optionB, `Unexpected prettyPrint output:\n${pretty}`);
  });

  it('should return a vertex-keyed Tree by default', async function () {
    const g = anon.traversal().with_(connection);
    const item = await g.V(1).out().out().tree().next();
    const tree = item.value;
    assert.ok(tree instanceof Tree);
    assert.strictEqual(tree.nodeCount(), 4);
    assert.strictEqual(tree.rootNodes().length, 1);
  });
});
