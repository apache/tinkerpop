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

import { assert } from 'chai';
import anon from '../../lib/process/anonymous-traversal.js';
import { statics as __ } from '../../lib/process/graph-traversal.js';
import { t, P } from '../../lib/process/traversal.js';
import { buildModernGraph } from '../cucumber/local-fixtures.js';

/**
 * Unit coverage for repeat() and its times()/until()/emit() modulators in the
 * Tiny Gremlin local executor, including modulator position (before vs. after the
 * repeat), emit side-output semantics, and folding of modulators inside the loop body.
 *
 * Modern graph out-edges: marko(1)->{vadas(2),josh(4),lop(3)}, josh(4)->{ripple(5),lop(3)},
 * peter(6)->{lop(3)}; vadas/lop/ripple have no out-edges.
 */
describe('Tiny Gremlin repeat()', () => {
  let g;
  const markoId = 1;

  beforeEach(() => {
    g = anon.traversal().with_(buildModernGraph().graph);
  });

  it('times(n) after repeat loops the body n times', async () => {
    const names = await g.V(markoId).repeat(__.out()).times(2).values('name').toList();
    // marko -> {vadas,josh,lop} -> only josh has out -> {ripple,lop}
    assert.sameMembers(names, ['lop', 'ripple']);
  });

  it('times(n) before repeat has the same loop-count semantics', async () => {
    const names = await g.V(markoId).times(2).repeat(__.out()).values('name').toList();
    assert.sameMembers(names, ['lop', 'ripple']);
  });

  it('times(0) after repeat still runs the body once (do-while)', async () => {
    const names = await g.V(markoId).repeat(__.out('created')).times(0).values('name').toList();
    assert.deepEqual(names, ['lop']);
  });

  it('times(0) before repeat skips the body entirely (identity)', async () => {
    const names = await g.V(markoId).times(0).repeat(__.out('created')).values('name').toList();
    assert.deepEqual(names, ['marko']);
  });

  it('emit() after repeat side-outputs each post-body traverser', async () => {
    const names = await g.V(markoId).repeat(__.out()).times(2).emit().values('name').toList();
    // 1-hop set emitted after body 1 (vadas,josh,lop) plus the times-bounded 2-hop exit (ripple,lop)
    assert.sameMembers(names, ['vadas', 'josh', 'lop', 'ripple', 'lop']);
  });

  it('until(traversal) after repeat is do-while (exit when the body output matches)', async () => {
    const names = await g.V(markoId).repeat(__.out()).until(__.has('name', 'ripple')).values('name').toList();
    // only the marko->josh->ripple branch reaches a vertex named ripple
    assert.deepEqual(names, ['ripple']);
  });

  it('until(traversal) before repeat is while (condition tested before the body)', async () => {
    // lop is already software, so while-semantics yields it without ever running out()
    const lopId = 3;
    const fromLop = await g.V(lopId).until(__.hasLabel('software')).repeat(__.out()).values('name').toList();
    assert.deepEqual(fromLop, ['lop']);
    // from marko (not software) the loop advances to the first software vertex on each path
    const fromMarko = await g.V(markoId).until(__.hasLabel('software')).repeat(__.out()).values('name').toList();
    assert.sameMembers(fromMarko, ['lop', 'ripple', 'lop']);
  });

  it('emit(traversal) before repeat side-outputs matching traversers ahead of the body', async () => {
    const names = await g.V(markoId).emit(__.has(t.label, 'person')).repeat(__.out()).values('name').toList();
    // person vertices reached while looping out(): marko, then vadas and josh
    assert.sameMembers(names, ['marko', 'vadas', 'josh']);
  });

  it('supports a nested repeat() inside the loop body', async () => {
    // body is a step followed by a bounded inner repeat (out().repeat(out()).times(1)),
    // i.e. two hops expressed as embedded loops
    const names = await g.V(markoId).repeat(__.out().repeat(__.out()).times(1)).times(1).values('name').toList();
    assert.sameMembers(names, ['lop', 'ripple']);
  });

  it('folds modulators inside the repeat body (order().by())', async () => {
    // order().by('name') inside the body proves the body sub-pipeline is folded too;
    // an unfolded body would fail on the bare by() step.
    const names = await g.V(markoId).repeat(__.out().order().by('name')).times(2).values('name').toList();
    assert.sameMembers(names, ['lop', 'ripple']);
  });

  it('order() inside repeat() barriers globally over the whole loop frontier', async () => {
    // The body runs over the entire frontier at once, so order() sorts all of g.V().both()
    // together (global), not within each start vertex's neighbours (which would be unsorted
    // once concatenated). 12 both()-endpoints, globally ordered by name.
    const names = await g.V().repeat(__.both().order().by('name')).times(1).values('name').toList();
    assert.deepEqual(names, [
      'josh', 'josh', 'josh', 'lop', 'lop', 'lop', 'marko', 'marko', 'marko', 'peter', 'ripple', 'vadas',
    ]);
  });

  it('preserves global order() semantics through a nested repeat()', async () => {
    // Same globalness, with the order() buried one repeat() deeper:
    // repeat(both().repeat(order().by(name)).times(1)).times(1).
    const names = await g.V()
      .repeat(__.both().repeat(__.order().by('name')).times(1)).times(1)
      .values('name').toList();
    assert.deepEqual(names, [
      'josh', 'josh', 'josh', 'lop', 'lop', 'lop', 'marko', 'marko', 'marko', 'peter', 'ripple', 'vadas',
    ]);
  });

  it('rejects repeat() with a loop label (no step labels in Tiny Gremlin)', async () => {
    try {
      await g.V(markoId).repeat('a', __.out()).times(2).values('name').toList();
      assert.fail('expected a LocalExecutionError for a labeled repeat()');
    } catch (err) {
      assert.match(err.message, /loop label is not supported/);
    }
  });

  it('rejects the predicate form of until() (only the traversal form is supported)', async () => {
    try {
      await g.V(markoId).repeat(__.out()).until(P.gt(100)).values('name').toList();
      assert.fail('expected a LocalExecutionError for until(P)');
    } catch (err) {
      assert.match(err.message, /until\(Predicate\) is not supported/);
    }
  });

  it('rejects a mutation step inside a repeat() body', async () => {
    try {
      await g.V(markoId).repeat(__.addV('x')).times(1).toList();
      assert.fail('expected a LocalExecutionError for addV() inside repeat()');
    } catch (err) {
      assert.match(err.message, /Mutation step 'addV\(\)' is not supported inside a repeat\(\) body/);
    }
  });

  it('rejects a mutation step inside an until() condition', async () => {
    try {
      await g.V(markoId).repeat(__.out()).until(__.property('k', 'v')).toList();
      assert.fail('expected a LocalExecutionError for property() inside until()');
    } catch (err) {
      assert.match(err.message, /Mutation step 'property\(\)' is not supported inside an until\(\) condition/);
    }
  });

  it('rejects a mutation step nested inside an inner repeat() body', async () => {
    try {
      await g.V(markoId).repeat(__.repeat(__.addE('x')).times(1)).times(1).toList();
      assert.fail('expected a LocalExecutionError for addE() nested in repeat()');
    } catch (err) {
      assert.match(err.message, /Mutation step 'addE\(\)' is not supported inside a repeat\(\) body/);
    }
  });
});
