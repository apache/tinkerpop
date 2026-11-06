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

import { afterAll, beforeAll, describe, it, expect } from '@jest/globals';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js';
import { loadDocsStore, type DocsStore } from '../src/docs/store.js';
import { registerDocsTools, resolvePageId } from '../src/docs/tools.js';
import { DocsSearchIndex, tokenize } from '../src/docs/search.js';
import { writeFixtureVersion } from './docs-fixtures.js';

type TextResult = { content: Array<{ type: string; text: string }>; isError?: boolean };

describe('docs search', () => {
  let root: string;
  let store: DocsStore;

  beforeAll(async () => {
    root = await mkdtemp(join(tmpdir(), 'mcp-docs-'));
    await writeFixtureVersion(root, '3.8.2');
    store = await loadDocsStore({ bundledRoot: root, installRoot: join(root, 'none') });
  });

  afterAll(async () => {
    await rm(root, { recursive: true, force: true });
  });

  it('splits step names into whole and parts', () => {
    expect(tokenize('g.V().hasLabel("person")')).toEqual([
      'g',
      'v',
      'haslabel',
      'has',
      'label',
      'person',
    ]);
    expect(tokenize('mergeV')).toEqual(['mergev', 'merge', 'v']);
  });

  it('ranks the step pages first for a step name', () => {
    const index = new DocsSearchIndex(store.versions.get('3.8.2')?.pages.values() ?? []);
    const pages = index.search('mergeV').map(r => r.page);
    expect(pages.slice(0, 2).sort()).toEqual([
      'dev/provider/merge-v-step.md',
      'reference/merge-v-step.md',
    ]);
  });

  it('matches longer terms by prefix', () => {
    const index = new DocsSearchIndex(store.versions.get('3.8.2')?.pages.values() ?? []);
    expect(index.search('merg').map(r => r.page)).toContain('reference/merge-v-step.md');
  });

  it('matches shorter indexed terms that prefix a query term', () => {
    const index = new DocsSearchIndex(store.versions.get('3.8.2')?.pages.values() ?? []);
    expect(index.search('predicates').map(r => r.page)).toContain('dev/provider/all-step.md');
  });

  it('returns a snippet around the match and respects the limit', () => {
    const index = new DocsSearchIndex(store.versions.get('3.8.2')?.pages.values() ?? []);
    const results = index.search('type promotion', 1);
    expect(results).toHaveLength(1);
    expect(results[0]?.snippet).toContain('Numeric type promotion');
    expect(index.search('zzzzqqq')).toEqual([]);
  });
});

describe('docs tools', () => {
  let root: string;
  let client: Client;
  let store: DocsStore;

  beforeAll(async () => {
    root = await mkdtemp(join(tmpdir(), 'mcp-docs-'));
    await writeFixtureVersion(join(root, 'bundled'), '4.0.0');
    await writeFixtureVersion(join(root, 'installed'), '3.8.2');
    store = await loadDocsStore({
      bundledRoot: join(root, 'bundled'),
      installRoot: join(root, 'installed'),
    });

    const server = new McpServer({ name: 'test', version: '0' });
    expect(registerDocsTools(server, store)).toEqual(['4.0.0', '3.8.2']);
    const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
    await server.connect(serverTransport);
    client = new Client({ name: 'test-client', version: '0' });
    await client.connect(clientTransport);
  });

  afterAll(async () => {
    await client.close();
    await rm(root, { recursive: true, force: true });
  });

  it('publishes the available versions as a required enum', async () => {
    const { tools } = await client.listTools();
    for (const name of ['search_gremlin_docs', 'get_gremlin_doc_page']) {
      const tool = tools.find(t => t.name === name);
      const schema = tool?.inputSchema as {
        required?: string[];
        properties: Record<string, { enum?: string[] }>;
      };
      expect(schema.required).toContain('version');
      expect(schema.properties['version']?.enum).toEqual(['4.0.0', '3.8.2']);
    }
  });

  it('searches one version', async () => {
    const result = (await client.callTool({
      name: 'search_gremlin_docs',
      arguments: { query: 'all step predicate', version: '3.8.2' },
    })) as TextResult;
    const body = JSON.parse(result.content[0]?.text ?? '{}');
    expect(body.version).toBe('3.8.2');
    expect(body.results[0]).toMatchObject({
      page: 'dev/provider/all-step.md',
      title: 'all()',
      source: 'gremlin-semantics',
    });
  });

  it('rejects a version that is not available', async () => {
    const result = (await client.callTool({
      name: 'search_gremlin_docs',
      arguments: { query: 'mergeV', version: '3.7.7' },
    })) as TextResult;
    expect(result.isError).toBe(true);
  });

  it('rejects a call without a version', async () => {
    const result = (await client.callTool({
      name: 'search_gremlin_docs',
      arguments: { query: 'mergeV' },
    })) as TextResult;
    expect(result.isError).toBe(true);
  });

  it('returns a page as Markdown with links rewritten to page ids', async () => {
    const result = (await client.callTool({
      name: 'get_gremlin_doc_page',
      arguments: { page: 'dev/provider/_steps.md', version: '3.8.2' },
    })) as TextResult;
    expect(result.isError).toBeFalsy();
    expect(result.content[0]?.text).toContain('[all()](dev/provider/all-step.md)');
  });

  it('never passes the license header to the agent', async () => {
    for (const page of store.versions.get('3.8.2')?.pages.keys() ?? []) {
      const result = (await client.callTool({
        name: 'get_gremlin_doc_page',
        arguments: { page, version: '3.8.2' },
      })) as TextResult;
      expect(result.content[0]?.text).not.toContain('Licensed to the Apache');
    }
    for (const query of ['licensed apache software foundation', 'mergeV', 'License']) {
      const result = (await client.callTool({
        name: 'search_gremlin_docs',
        arguments: { query, version: '3.8.2', limit: 25 },
      })) as TextResult;
      expect(result.content[0]?.text).not.toContain('Licensed to the Apache');
    }
  });

  it('reports an unknown page with suggestions', async () => {
    const result = (await client.callTool({
      name: 'get_gremlin_doc_page',
      arguments: { page: 'reference/merge-step.md', version: '3.8.2' },
    })) as TextResult;
    expect(result.isError).toBe(true);
    expect(result.content[0]?.text).toContain('Similar pages:');
  });

  it('resolves links, fragments, site URLs and unique file names', () => {
    const docs = store.versions.get('3.8.2');
    if (docs === undefined) throw new Error('missing fixture');
    expect(resolvePageId(docs, 'reference/the-traversal.md#traversal')).toBe(
      'reference/the-traversal.md'
    );
    expect(resolvePageId(docs, 'reference/the-traversal')).toBe('reference/the-traversal.md');
    expect(
      resolvePageId(docs, 'https://tinkerpop.apache.org/docs/3.8.2/dev/provider/all-step.md')
    ).toBe('dev/provider/all-step.md');
    expect(resolvePageId(docs, 'all-step.md')).toBe('dev/provider/all-step.md');
    // merge-v-step.md exists in both reference and dev/provider, so a bare name is ambiguous
    expect(resolvePageId(docs, 'merge-v-step.md')).toBeUndefined();
  });

  it('registers nothing when no docs are available', () => {
    const server = new McpServer({ name: 'test', version: '0' });
    expect(registerDocsTools(server, { versions: new Map(), problems: [] })).toEqual([]);
  });
});
