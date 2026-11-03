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

import { describe, it, expect } from '@jest/globals';
import {
  extractRelativeLinks,
  isPageId,
  resolveRelative,
  selectPages,
  sourceOf,
} from '../src/docs/selection.js';
import { docsTree } from './docs-fixtures.js';

describe('docs page selection', () => {
  it('extracts relative link targets without fragments', () => {
    const md = [
      '> see [llms.txt](https://tinkerpop.apache.org/docs/3.8.2/llms.txt)',
      '[Steps](_steps.md) and [all()](all-step.md#all-step) and [top](#top)',
      '![logo](../../images/logo.png) [site](/docs/index.html)',
    ].join('\n');
    expect(extractRelativeLinks(md)).toEqual(['_steps.md', 'all-step.md', '../../images/logo.png']);
  });

  it('resolves relative targets against the linking page', () => {
    expect(resolveRelative('dev/provider/_steps.md', 'all-step.md')).toBe(
      'dev/provider/all-step.md'
    );
    expect(resolveRelative('dev/provider/_steps.md', '../../reference/a.md')).toBe(
      'reference/a.md'
    );
    expect(resolveRelative('reference/a.md', '../../escape.md')).toBeUndefined();
  });

  it('accepts only page ids that stay under the version directory', () => {
    expect(isPageId('reference/the-traversal.md')).toBe(true);
    expect(isPageId('dev/provider/all-step.md')).toBe(true);
    expect(isPageId('')).toBe(false);
    expect(isPageId('reference/../../../x.md')).toBe(false);
    expect(isPageId('reference/..\\..\\x.md')).toBe(false);
    expect(isPageId('/etc/x.md')).toBe(false);
    expect(isPageId('reference//x.md')).toBe(false);
    expect(isPageId('reference/./x.md')).toBe(false);
  });

  it('selects every reference page and the Gremlin Semantics pages reached by links', async () => {
    const tree = docsTree();
    const selected = await selectPages(Object.keys(tree), async p => tree[p] ?? '');
    expect(selected).toEqual([
      'dev/provider/_steps.md',
      'dev/provider/all-step.md',
      'dev/provider/gremlin-semantics.md',
      'dev/provider/merge-v-step.md',
      'reference/index.md',
      'reference/merge-v-step.md',
      'reference/the-traversal.md',
    ]);
  });

  it('selects only reference pages when the Gremlin Semantics page is absent', async () => {
    const tree = docsTree();
    delete tree['dev/provider/gremlin-semantics.md'];
    const selected = await selectPages(Object.keys(tree), async p => tree[p] ?? '');
    expect(selected.every(p => p.startsWith('reference/'))).toBe(true);
  });

  it('only reads Gremlin Semantics pages', async () => {
    const tree = docsTree();
    const read: string[] = [];
    await selectPages(Object.keys(tree), async p => {
      read.push(p);
      return tree[p] ?? '';
    });
    expect(read.every(p => p.startsWith('dev/provider/'))).toBe(true);
    expect(read).not.toContain('dev/provider/providers.md');
  });

  it('labels the source of a page', () => {
    expect(sourceOf('reference/index.md')).toBe('reference');
    expect(sourceOf('dev/provider/all-step.md')).toBe('gremlin-semantics');
  });
});
