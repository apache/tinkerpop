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
import { t } from '../../lib/process/traversal.js';
import { buildModernGraph } from '../cucumber/local-fixtures.js';

/**
 * Unit coverage for path().by() projection forms that the @TinyGremlin Gherkin
 * scenarios do not exercise: by(T), the limited by(Traversal), and the rejection
 * of unsupported by(Traversal) shapes. by(String) round-robin and non-productive
 * filtering are covered by Path.feature.
 */
describe('Tiny Gremlin path().by()', () => {
  let g;
  const markoId = 1; // modern graph: makeVertex(1, 'person', ...)

  beforeEach(() => {
    g = anon.traversal().with_(buildModernGraph().graph);
  });

  const objectsOf = (paths) => paths.map((p) => p.objects);

  it('by(String) filters non-productive elements (lop has no age)', async () => {
    const result = await g.V(markoId).out().path().by('age').toList();
    // marko(29) -> {vadas(27), josh(32), lop(no age -> path filtered)}
    assert.sameDeepMembers(objectsOf(result), [[29, 27], [29, 32]]);
  });

  it('by(T) projects element id/label', async () => {
    const result = await g.V(markoId).out().path().by(t.label).toList();
    // marko=person, out-neighbours: vadas/josh=person, lop=software
    assert.sameDeepMembers(objectsOf(result), [
      ['person', 'person'],
      ['person', 'person'],
      ['person', 'software'],
    ]);
  });

  it('by(Traversal) with a single value-extraction step (values) takes the first value', async () => {
    const result = await g.V(markoId).out().path().by(__.values('name')).toList();
    assert.sameDeepMembers(objectsOf(result), [
      ['marko', 'vadas'],
      ['marko', 'josh'],
      ['marko', 'lop'],
    ]);
  });

  it('rejects by(Traversal) that is not a single value-extraction step', async () => {
    try {
      await g.V(markoId).out().path().by(__.out().values('name')).toList();
      assert.fail('expected a LocalExecutionError for a multi-step by(Traversal)');
    } catch (err) {
      assert.match(err.message, /single value-extraction step/);
    }
  });
});
