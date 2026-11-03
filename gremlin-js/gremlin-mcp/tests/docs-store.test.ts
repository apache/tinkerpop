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

import { afterEach, beforeEach, describe, it, expect } from '@jest/globals';
import { mkdtemp, rm, writeFile, unlink, readdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  compareVersions,
  isDocsVersion,
  loadDocsStore,
  parsePage,
  writeVersion,
} from '../src/docs/store.js';
import { docsTree, writeFixtureVersion } from './docs-fixtures.js';

describe('docs store', () => {
  let bundledRoot: string;
  let installRoot: string;

  beforeEach(async () => {
    bundledRoot = await mkdtemp(join(tmpdir(), 'mcp-bundled-'));
    installRoot = await mkdtemp(join(tmpdir(), 'mcp-installed-'));
  });

  afterEach(async () => {
    await rm(bundledRoot, { recursive: true, force: true });
    await rm(installRoot, { recursive: true, force: true });
  });

  it('parses the title and summary of a page', () => {
    const tree = docsTree();
    const page = parsePage(
      'dev/provider/all-step.md',
      tree['dev/provider/all-step.md'] ?? '',
      '3.8.2',
      new Set()
    );
    expect(page.title).toBe('all()');
    expect(page.summary).toBe('The formal semantics of the all() step.');
    expect(page.source).toBe('gremlin-semantics');
  });

  it('unescapes --> in a summary', () => {
    const page = parsePage(
      'reference/a.md',
      '# A\n\n<!-- llms-summary: Maps a --&gt; b. -->\n',
      '3.8.2',
      new Set()
    );
    expect(page.summary).toBe('Maps a --> b.');
  });

  it('removes the license header comment but keeps the page', () => {
    const tree = docsTree();
    const raw = tree['dev/provider/all-step.md'] ?? '';
    expect(raw).toContain('Licensed to the Apache');
    const page = parsePage('dev/provider/all-step.md', raw, '3.8.2', new Set());
    expect(page.markdown).not.toContain('Licensed to the Apache');
    expect(page.markdown).not.toContain('under the License.');
    expect(page.markdown).toContain('llms.txt');
    expect(page.sections[0]?.anchor).toBe('all-step');
  });

  it('rewrites relative links to page ids or absolute site URLs', () => {
    const tree = docsTree();
    const page = parsePage(
      'reference/the-traversal.md',
      tree['reference/the-traversal.md'] ?? '',
      '3.8.2',
      new Set(['reference/merge-v-step.md'])
    );
    expect(page.markdown).toContain('[mergeV()](reference/merge-v-step.md#mergev-step)');
    expect(page.markdown).toContain(
      '[upgrade docs](https://tinkerpop.apache.org/docs/3.8.2/upgrade/index.md)'
    );
    expect(page.markdown).toContain('(https://tinkerpop.apache.org/docs/3.8.2/llms.txt)');
  });

  it('loads bundled and installed versions, newest first', async () => {
    await writeFixtureVersion(bundledRoot, '4.0.0-beta.4');
    const pages = await writeFixtureVersion(installRoot, '3.8.2');
    await writeFixtureVersion(installRoot, '3.7.7');

    const store = await loadDocsStore({ bundledRoot, installRoot });
    expect(store.problems).toEqual([]);
    expect([...store.versions.keys()]).toEqual(['4.0.0-beta.4', '3.8.2', '3.7.7']);
    expect(store.versions.get('4.0.0-beta.4')?.origin).toBe('bundled');
    expect([...(store.versions.get('3.8.2')?.pages.keys() ?? [])]).toEqual(pages);
  });

  it('prefers an installed copy over the bundled copy of the same version', async () => {
    await writeFixtureVersion(bundledRoot, '4.0.0');
    await writeFixtureVersion(installRoot, '4.0.0');
    const store = await loadDocsStore({ bundledRoot, installRoot });
    expect(store.versions.get('4.0.0')?.origin).toBe('installed');
  });

  it('reports a version with a missing page and leaves it out', async () => {
    await writeFixtureVersion(installRoot, '3.8.2');
    await unlink(join(installRoot, '3.8.2', 'reference', 'index.md'));
    const store = await loadDocsStore({ bundledRoot, installRoot });
    expect(store.versions.has('3.8.2')).toBe(false);
    expect(store.problems).toEqual([
      'Could not load installed docs 3.8.2 (missing page reference/index.md); reinstall it with: gremlin-mcp docs install 3.8.2',
    ]);
  });

  it('reports a version with an unreadable manifest', async () => {
    await writeFixtureVersion(installRoot, '3.8.2');
    await writeFile(join(installRoot, '3.8.2', 'manifest.json'), '{not json', 'utf8');
    const store = await loadDocsStore({ bundledRoot, installRoot });
    expect(store.versions.size).toBe(0);
    expect(store.problems).toHaveLength(1);
  });

  it('is empty when the roots do not exist', async () => {
    const store = await loadDocsStore({
      bundledRoot: join(bundledRoot, 'nope'),
      installRoot: join(installRoot, 'nope'),
    });
    expect(store.versions.size).toBe(0);
    expect(store.problems).toEqual([]);
  });

  it('replaces an existing copy and leaves no staging directory', async () => {
    await writeFixtureVersion(installRoot, '3.8.2');
    await writeFixtureVersion(installRoot, '3.8.2');
    expect(await readdir(installRoot)).toEqual(['3.8.2']);
  });

  it('keeps the previous copy when a write fails', async () => {
    await writeFixtureVersion(installRoot, '3.8.2');
    await expect(
      writeVersion(
        installRoot,
        { version: '3.8.2', source: 'test', created: '', pages: ['reference/index.md'] },
        new Map()
      )
    ).rejects.toThrow('no content for page reference/index.md');
    const store = await loadDocsStore({ bundledRoot, installRoot });
    expect(store.versions.has('3.8.2')).toBe(true);
    expect(await readdir(installRoot)).toEqual(['3.8.2']);
  });

  it('refuses to write a page id that escapes the version directory', async () => {
    const id = 'reference/../../escaped.md';
    await expect(
      writeVersion(
        installRoot,
        { version: '3.8.2', source: 'test', created: '', pages: [id] },
        new Map([[id, '# escaped']])
      )
    ).rejects.toThrow(`invalid page path ${id}`);
    expect(await readdir(installRoot)).toEqual([]);
    await expect(readdir(join(installRoot, '..'))).resolves.not.toContain('escaped.md');
  });

  it('reports a version whose manifest lists a page id with backslashes', async () => {
    await writeFixtureVersion(installRoot, '3.8.2');
    await writeFile(
      join(installRoot, '3.8.2', 'manifest.json'),
      JSON.stringify({ version: '3.8.2', pages: ['reference/..\\..\\x.md'] }),
      'utf8'
    );
    const store = await loadDocsStore({ bundledRoot, installRoot });
    expect(store.versions.size).toBe(0);
    expect(store.problems[0]).toContain('invalid page path reference/..\\..\\x.md');
  });

  it('validates version names', () => {
    expect(isDocsVersion('3.8.2')).toBe(true);
    expect(isDocsVersion('4.0.0-beta.3')).toBe(true);
    expect(isDocsVersion('4.0.0-SNAPSHOT')).toBe(true);
    expect(isDocsVersion('3.8')).toBe(false);
    expect(isDocsVersion('../3.8.2')).toBe(false);
    expect(isDocsVersion('3.8.2-..')).toBe(false);
  });

  it('orders versions newest first with pre-releases before their release', () => {
    const sorted = ['3.7.7', '4.0.0-beta.3', '4.0.0', '3.8.2', '4.0.0-beta.10'].sort(
      compareVersions
    );
    expect(sorted).toEqual(['4.0.0', '4.0.0-beta.10', '4.0.0-beta.3', '3.8.2', '3.7.7']);
  });
});
