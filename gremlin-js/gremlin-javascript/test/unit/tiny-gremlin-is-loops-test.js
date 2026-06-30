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
import { P } from '../../lib/process/traversal.js';
import { buildModernGraph } from '../cucumber/local-fixtures.js';

/**
 * Unit coverage for is() (value/predicate filter) and loops() (the enclosing repeat()'s
 * loop counter) in the Tiny Gremlin local executor. loops() is exercised through repeat()
 * since that is where it carries meaning; the two steps are paired because loops() is inert
 * without a following is() to test the count.
 */
describe('Tiny Gremlin is() and loops()', () => {
  let g;
  const markoId = 1;

  beforeEach(() => {
    g = anon.traversal().with_(buildModernGraph().graph);
  });

  // ── is() ──────────────────────────────────────────────────────────────────

  it('is(value) keeps only equal values', async () => {
    const ages = await g.V().values('age').is(32).toList();
    assert.deepEqual(ages, [32]);
  });

  it('is(P) filters by predicate', async () => {
    const ages = await g.V().values('age').is(P.lte(30)).toList();
    assert.sameMembers(ages, [27, 29]);
  });

  it('chains is() predicates as a conjunction', async () => {
    const ages = await g.V().values('age').is(P.gte(29)).is(P.lt(34)).toList();
    assert.sameMembers(ages, [29, 32]);
  });

  // ── loops() ───────────────────────────────────────────────────────────────

  it('until(loops().is(n)) before repeat() is while-bounded like times(n)', async () => {
    // condition tested before the body: exits once the current loop count reaches 2
    const names = await g.V().until(__.loops().is(2)).repeat(__.out()).values('name').toList();
    assert.sameMembers(names, ['lop', 'ripple']);
  });

  it('until(loops().is(n)) after repeat() is do-while and matches times(n)', async () => {
    const viaLoops = await g.V(markoId).repeat(__.out()).until(__.loops().is(2)).values('name').toList();
    const viaTimes = await g.V(markoId).repeat(__.out()).times(2).values('name').toList();
    assert.sameMembers(viaLoops, viaTimes);
    assert.sameMembers(viaLoops, ['lop', 'ripple']);
  });

  it('reports the inner loop count inside a nested repeat()', async () => {
    // inner repeat exits when ITS own loops() reaches 1 (one out() hop), then the outer
    // repeat takes a second hop; loops() resolves to the nearest enclosing repeat().
    const names = await g.V(markoId)
      .repeat(__.repeat(__.out()).until(__.loops().is(1))).until(__.loops().is(2))
      .values('name').toList();
    assert.sameMembers(names, ['lop', 'ripple']);
  });

  it('is zero outside any repeat()', async () => {
    const loops = await g.V().loops().toList();
    assert.deepEqual(loops, [0, 0, 0, 0, 0, 0]);
  });

  it('rejects the loops("label") reference (no step labels in Tiny Gremlin)', async () => {
    try {
      await g.V(markoId).repeat(__.out()).until(__.loops('a').is(2)).toList();
      assert.fail('expected a rejection for loops("label")');
    } catch (err) {
      assert.match(err.message, /loops\("label"\) is not supported/);
    }
  });
});
