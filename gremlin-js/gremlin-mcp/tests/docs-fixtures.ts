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

/**
 * A small Markdown docs tree modeled on the layout published for TinkerPop 3.8.2: split pages
 * named by anchor, an llms.txt pointer on every page, llms-summary comments, and Gremlin
 * Semantics pages sitting in dev/provider/ beside unrelated provider pages.
 */

import { selectPages } from '../src/docs/selection.js';
import { writeVersion } from '../src/docs/store.js';

const pointer = (version: string) =>
  `> For the complete documentation index, see [llms.txt](https://tinkerpop.apache.org/docs/${version}/llms.txt)\n`;

// The license header the docs build writes as an HTML comment after the pointer on every page.
export const LICENSE_COMMENT =
  '<!--\nLicensed to the Apache Software Foundation (ASF) under one\nor more contributor license ' +
  'agreements.  See the NOTICE file\n...\nunder the License.\n-->\n\n';

const page = (version: string, anchor: string, heading: string, summary: string, body: string) =>
  `${pointer(version)}\n${LICENSE_COMMENT}<a id="${anchor}"></a>\n${heading}\n\n<!-- llms-summary: ${summary} -->\n\n${body}\n`;

export const docsTree = (version = '3.8.2'): Record<string, string> => ({
  'reference/index.md': page(
    version,
    '_tinkerpop_documentation',
    '# TinkerPop Documentation',
    'The main TinkerPop reference guide.',
    '![logo](../images/apache-tinkerpop-logo.png)\nSee [The Traversal](the-traversal.md).'
  ),
  'reference/the-traversal.md': page(
    version,
    'traversal',
    '## The Traversal',
    'The core of Gremlin: how traversals are constructed from steps.',
    'A traversal is a sequence of steps. Use [mergeV()](merge-v-step.md#mergev-step) to upsert.\n' +
      'Upgrades are described in the [upgrade docs](../upgrade/index.md).'
  ),
  'reference/merge-v-step.md': page(
    version,
    'mergev-step',
    '### MergeV Step',
    'The mergeV() step: create a vertex if it does not exist, otherwise match it.',
    'The `mergeV()` step upserts a vertex. It is often paired with `coalesce()` in older code.\n' +
      'See the [semantics](../dev/provider/merge-v-step.md).'
  ),
  'dev/provider/index.md': page(
    version,
    '_introduction',
    '# Introduction',
    'Provider documentation overview.',
    'The [Gremlin Semantics](gremlin-semantics.md) and [providers](providers.md) sections.'
  ),
  'dev/provider/providers.md': page(
    version,
    '_provider_documentation',
    '# Provider Documentation',
    'Guidance for those building on TinkerPop.',
    'See [Graph System Provider Requirements](graph-system-provider-requirements.md).'
  ),
  'dev/provider/graph-system-provider-requirements.md': page(
    version,
    'graph-system-provider-requirements',
    '## Graph System Provider Requirements',
    'What a graph system provider must implement.',
    'Implement the structure API. Numeric type promotion is not covered here.'
  ),
  'dev/provider/gremlin-semantics.md': page(
    version,
    'gremlin-semantics',
    '# Gremlin Semantics',
    'The formal semantics of the Gremlin language.',
    '![room](../../images/tinkerpop-meeting-room.png)\n' +
      'Numeric type promotion follows these rules. See [Steps](_steps.md) and the ' +
      '[reference](../../reference/the-traversal.md#traversal).'
  ),
  'dev/provider/_steps.md': page(
    version,
    '_steps',
    '## Steps',
    'Per-step semantic definitions.',
    '- [all()](all-step.md)\n- [mergeV()](merge-v-step.md#mergev-step)'
  ),
  'dev/provider/all-step.md': page(
    version,
    'all-step',
    '### all()',
    'The formal semantics of the all() step.',
    'Filters array data if all items match the predicate. Back to [Steps](_steps.md).'
  ),
  'dev/provider/merge-v-step.md': page(
    version,
    'merge-v-step',
    '### mergeV()',
    'The formal semantics of the mergeV() step.',
    'Matches on the search map and creates the vertex when there is no match.'
  ),
});

/** An llms.txt index for the fixture tree, with absolute URLs as published. */
export const llmsTxt = (version = '3.8.2'): string => {
  const base = `https://tinkerpop.apache.org/docs/${version}/`;
  const lines = Object.keys(docsTree(version))
    .sort()
    .map(p => `- [${p}](${base}${p}): summary`);
  return `# Apache TinkerPop Documentation\n\n> Documentation.\n\n${LICENSE_COMMENT}## all\n\n${lines.join('\n')}\n`;
};

/** Writes the selected pages of the fixture tree as a docs version under a root. */
export const writeFixtureVersion = async (root: string, version: string): Promise<string[]> => {
  const tree = docsTree(version);
  const pages = await selectPages(Object.keys(tree), async p => tree[p] ?? '');
  await writeVersion(
    root,
    { version, source: 'test', created: '2026-11-03T00:00:00Z', pages },
    new Map(pages.map(p => [p, tree[p] ?? '']))
  );
  return pages;
};
