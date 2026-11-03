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
import { parseSections } from '../src/docs/sections.js';
import { parsePage } from '../src/docs/store.js';
import { DocsSearchIndex } from '../src/docs/search.js';
import { fragmentOf, pageContent } from '../src/docs/tools.js';

// Filler prose that pushes a page past the size at which search points into sections.
const filler = (words: number) =>
  Array.from({ length: words }, (_, i) => `word${i % 50}`).join(' ');

/** A large hub page shaped like reference/traversal.md. */
const hubPage = [
  '> For the complete documentation index, see [llms.txt](https://tinkerpop.apache.org/docs/4.0.0/llms.txt)',
  '',
  '<a id="traversal"></a>',
  '# The Traversal',
  '',
  '<!-- llms-summary: How traversals are built and configured. -->',
  '',
  filler(1200),
  '',
  '<a id="configuration-steps"></a>',
  '## Configuration Steps',
  '',
  'Configuration steps set up the traversal source.',
  '',
  '<a id="configuration-steps-withstrategies"></a>',
  '### WithStrategies Configuration',
  '',
  'The `withStrategies()` configuration adds TraversalStrategy instances.',
  '',
  '```text',
  '## not a heading inside code',
  '```',
  '',
  '<a id="configuration-steps-withoutstrategies"></a>',
  '### WithoutStrategies Configuration',
  '',
  'The `withoutStrategies()` configuration removes strategies.',
  '',
  '<a id="start-steps"></a>',
  '## Start Steps',
  '',
  filler(600),
].join('\n');

describe('docs sections', () => {
  const page = parsePage('reference/traversal.md', hubPage, '4.0.0', new Set());

  it('splits a page into anchored sections that include their sub-sections', () => {
    const sections = parseSections(hubPage);
    expect(sections.map(s => [s.anchor, s.level])).toEqual([
      ['traversal', 1],
      ['configuration-steps', 2],
      ['configuration-steps-withstrategies', 3],
      ['configuration-steps-withoutstrategies', 3],
      ['start-steps', 2],
    ]);
    const config = sections[1];
    expect(config?.markdown).toContain('### WithoutStrategies Configuration');
    expect(config?.markdown).not.toContain('Start Steps');
    expect(config?.markdown.endsWith('<a id="start-steps"></a>')).toBe(false);
    expect(config?.ownText).not.toContain('withStrategies');
  });

  it('ignores heading-like lines inside code fences', () => {
    const withStrategies = parseSections(hubPage)[2];
    expect(withStrategies?.markdown).toContain('## not a heading inside code');
    expect(withStrategies?.markdown).not.toContain('WithoutStrategies');
  });

  it('returns only the requested section under a line naming the full page', () => {
    const text = pageContent(page, 'configuration-steps-withstrategies');
    expect(text.split('\n')[0]).toContain(
      'Section "WithStrategies Configuration" of reference/traversal.md'
    );
    expect(text).toContain('adds TraversalStrategy instances');
    expect(text).not.toContain('Start Steps');
    expect(text.length).toBeLessThan(600);
  });

  it('returns the whole page for no fragment, the top anchor, or an unknown anchor', () => {
    expect(pageContent(page, undefined)).toBe(page.markdown);
    expect(pageContent(page, 'traversal')).toBe(page.markdown);
    const unknown = pageContent(page, 'nope');
    expect(unknown).toContain('No section #nope');
    expect(unknown).toContain(page.markdown);
  });

  it('reads the fragment of a page reference', () => {
    expect(fragmentOf('reference/traversal.md#start-steps')).toBe('start-steps');
    expect(fragmentOf('reference/traversal.md')).toBeUndefined();
    expect(fragmentOf('reference/traversal.md#')).toBeUndefined();
  });

  it('points search at the deepest matching section of a large page', () => {
    const index = new DocsSearchIndex([page]);
    const [result] = index.search('withStrategies');
    expect(result?.section).toBe('reference/traversal.md#configuration-steps-withstrategies');
    expect(result?.sectionTitle).toBe('WithStrategies Configuration');
    expect(result?.snippet).toContain('withStrategies()');
  });

  it('does not point into small pages', () => {
    const small = parsePage(
      'reference/small.md',
      '<a id="small"></a>\n# Small\n\n<a id="part"></a>\n## Part\n\nwithStrategies here.',
      '4.0.0',
      new Set()
    );
    const [result] = new DocsSearchIndex([small]).search('withStrategies');
    expect(result?.page).toBe('reference/small.md');
    expect(result?.section).toBeUndefined();
  });
});
