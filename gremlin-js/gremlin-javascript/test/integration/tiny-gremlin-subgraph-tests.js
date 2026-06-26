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
import anon from '../../lib/process/anonymous-traversal.js';
import { getConnection } from '../helper.js';
import { Graph, Vertex, Edge } from '../../lib/structure/graph.js';
import { t } from '../../lib/process/traversal.js';

/**
 * Exercises a core Tiny Gremlin use case end-to-end: pull a subgraph() from a remote
 * graph, then run local in-process traversals over the detached Graph returned to the client.
 *
 * The "created" subgraph of the modern graph contains 5 vertices
 * (marko, josh, peter — person; lop, ripple — software; vadas is excluded) and 4 created
 * edges (marko->lop, josh->ripple, josh->lop, peter->lop).
 */
describe('Tiny Gremlin over subgraph()', function () {
  let connection;
  let sg;        // detached Graph returned by subgraph()
  let lg;        // local Tiny Gremlin traversal source over sg
  let markoId;   // marko's id from the remote graph, for id-preservation checks

  const sorted = (arr) => [...arr].sort();
  const pathObjects = (paths) => paths.map((p) => p.objects);

  before(async function () {
    connection = getConnection('gmodern');
    await connection.open();
    const g = anon.traversal().with_(connection);
    sg = (await g.E().hasLabel('created').subgraph('sg').cap('sg').next()).value;
    lg = anon.traversal().with_(sg);
    markoId = (await g.V().has('name', 'marko').id().next()).value;
  });

  after(function () {
    return connection.close();
  });

  it('returns a detached Graph from subgraph()', function () {
    assert.ok(sg instanceof Graph);
  });

  it('captures the created-subgraph vertex set', async function () {
    const vertices = await lg.V().toList();
    assert.strictEqual(vertices.length, 5);
    vertices.forEach((v) => assert.ok(v instanceof Vertex));
    const names = sorted(await lg.V().values('name').toList());
    assert.deepStrictEqual(names, ['josh', 'lop', 'marko', 'peter', 'ripple']);
    assert.ok(!names.includes('vadas'), 'vadas has no created edges and must be excluded');
  });

  it('captures the created edges', async function () {
    const edges = await lg.E().toList();
    assert.strictEqual(edges.length, 4);
    edges.forEach((e) => assert.ok(e instanceof Edge));
    const labels = await lg.E().label().toList();
    assert.deepStrictEqual(sorted(labels), ['created', 'created', 'created', 'created']);
  });

  it('filters by label locally', async function () {
    const software = sorted(await lg.V().hasLabel('software').values('name').toList());
    assert.deepStrictEqual(software, ['lop', 'ripple']);
    const people = sorted(await lg.V().hasLabel('person').values('name').toList());
    assert.deepStrictEqual(people, ['josh', 'marko', 'peter']);
  });

  it('preserves vertex properties through the round-trip', async function () {
    assert.deepStrictEqual(await lg.V().has('name', 'marko').values('age').toList(), [29]);
    assert.deepStrictEqual(await lg.V().has('name', 'josh').values('age').toList(), [32]);
    const langs = await lg.V().hasLabel('software').values('lang').toList();
    assert.deepStrictEqual(sorted(langs), ['java', 'java']);
  });

  it('navigates out-edges locally', async function () {
    const joshOut = sorted(await lg.V().has('name', 'josh').out().values('name').toList());
    assert.deepStrictEqual(joshOut, ['lop', 'ripple']);
    const markoOut = await lg.V().has('name', 'marko').out().values('name').toList();
    assert.deepStrictEqual(markoOut, ['lop']);
  });

  it('navigates in-edges locally', async function () {
    const lopIn = sorted(await lg.V().has('name', 'lop').in_().values('name').toList());
    assert.deepStrictEqual(lopIn, ['josh', 'marko', 'peter']);
    const rippleIn = await lg.V().has('name', 'ripple').in_().values('name').toList();
    assert.deepStrictEqual(rippleIn, ['josh']);
  });

  it('preserves edge properties', async function () {
    // weight may serialize as float; compare within tolerance rather than exact equality
    const near = (actual, expected) => assert.ok(Math.abs(actual - expected) < 1e-6, `${actual} ~ ${expected}`);
    const markoWeight = await lg.V().has('name', 'marko').outE().values('weight').toList();
    assert.strictEqual(markoWeight.length, 1);
    near(markoWeight[0], 0.4);
    const weights = (await lg.E().values('weight').toList()).map(Number).sort((a, b) => a - b);
    assert.strictEqual(weights.length, 4);
    [0.2, 0.4, 0.4, 1.0].forEach((expected, i) => near(weights[i], expected));
  });

  it('orders results locally', async function () {
    const ordered = await lg.V().hasLabel('person').values('name').order().toList();
    assert.deepStrictEqual(ordered, ['josh', 'marko', 'peter']);
  });

  it('projects paths with by() over the subgraph', async function () {
    const byName = pathObjects(await lg.V().has('name', 'josh').out().path().by('name').toList());
    assert.strictEqual(byName.length, 2);
    byName.forEach((p) => assert.strictEqual(p[0], 'josh'));
    assert.deepStrictEqual(sorted(byName.map((p) => p[1])), ['lop', 'ripple']);
    const byLabel = pathObjects(await lg.V().has('name', 'marko').out().path().by(t.label).toList());
    assert.deepStrictEqual(byLabel, [['person', 'software']]);
  });

  it('preserves element ids for direct lookup', async function () {
    const localMarkoId = (await lg.V().has('name', 'marko').id().next()).value;
    assert.deepStrictEqual(localMarkoId, markoId);
    const byId = await lg.V(markoId).values('name').toList();
    assert.deepStrictEqual(byId, ['marko']);
  });

  it('returns elementMap with tokens and properties', async function () {
    const m = (await lg.V().has('name', 'marko').elementMap().next()).value;
    assert.strictEqual(m.get('name'), 'marko');
    assert.strictEqual(m.get('age'), 29);
    assert.strictEqual(m.get(t.label), 'person');
    assert.deepStrictEqual(m.get(t.id), markoId);
  });

  // Tiny Gremlin is a subset — steps like project(), group(), groupCount(), sum() and fold()
  // are unavailable. The pattern is to do everything the subset DOES support inside the
  // traversal (here, sort with order().by(...)), terminate with toList(), then finish in
  // ordinary JavaScript with the array methods the subset lacks. forEach()/reduce() are
  // native Tiny Gremlin terminals; map()/filter()/flat() run on the materialized array.
  describe('post-processing with JavaScript closures', function () {
    it('projects with Array.map() in place of project()', async function () {
      // order() does the sorting; only the projection has no Tiny Gremlin equivalent
      const projected = (await lg.V().hasLabel('person').order().by('name').elementMap().toList())
        .map((m) => ({ name: m.get('name'), age: m.get('age') }));
      assert.deepStrictEqual(projected, [
        { name: 'josh', age: 32 },
        { name: 'marko', age: 29 },
        { name: 'peter', age: 35 },
      ]);
    });

    it('filters with Array.filter() on a JS-only predicate', async function () {
      // name length has no Tiny Gremlin predicate, so the filter must live in JS
      const longNames = (await lg.V().hasLabel('person').order().by('name').elementMap().toList())
        .filter((m) => m.get('name').length > 4)
        .map((m) => m.get('name'));
      assert.deepStrictEqual(longNames, ['marko', 'peter']);
    });

    it('fans out and flattens with Array.flat() in place of flatMap()', async function () {
      const people = await lg.V().hasLabel('person').order().by('name').values('name').toList();
      const created = (
        await Promise.all(
          people.map(async (name) => {
            const items = await lg.V().has('name', name).out().order().by('name').values('name').toList();
            return items.map((item) => `${name}->${item}`);
          }),
        )
      ).flat();
      assert.deepStrictEqual(created, ['josh->lop', 'josh->ripple', 'marko->lop', 'peter->lop']);
    });

    it('aggregates with the reduce() terminal in place of sum()', async function () {
      const totalAge = await lg.V().hasLabel('person').values('age').reduce((acc, age) => acc + age, 0);
      assert.strictEqual(totalAge, 29 + 32 + 35);
    });

    it('group-counts with Array.reduce() in place of groupCount()', async function () {
      const labels = await lg.V().label().toList();
      const counts = labels.reduce((acc, label) => {
        acc[label] = (acc[label] || 0) + 1;
        return acc;
      }, {});
      assert.deepStrictEqual(counts, { person: 3, software: 2 });
    });

    it('groups into buckets with Array.reduce() in place of group()', async function () {
      // order().by('name') means each bucket comes out name-sorted with no JS sort
      const maps = await lg.V().order().by('name').elementMap().toList();
      const byLabel = maps.reduce((acc, m) => {
        const label = m.get(t.label);
        (acc[label] = acc[label] || []).push(m.get('name'));
        return acc;
      }, {});
      assert.deepStrictEqual(byLabel, {
        person: ['josh', 'marko', 'peter'],
        software: ['lop', 'ripple'],
      });
    });

    it('collects via the forEach() terminal for side-effecting consumption', async function () {
      const collected = [];
      await lg.V().hasLabel('software').order().by('name').values('name')
        .forEach((name) => collected.push(name.toUpperCase()));
      assert.deepStrictEqual(collected, ['LOP', 'RIPPLE']);
    });
  });
});
