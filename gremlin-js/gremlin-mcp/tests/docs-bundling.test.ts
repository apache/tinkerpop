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
import { mkdir, mkdtemp, readFile, readdir, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { bundleDocs } from '../src/docs/bundling.js';
import { loadDocsStore } from '../src/docs/store.js';
import { docsTree } from './docs-fixtures.js';

/** Writes the whole fixture tree, as a docs build would leave it, plus non-page files. */
const writeDocsBuild = async (dir: string, version: string) => {
  const files = { ...docsTree(version), 'llms.txt': '# index', 'upgrade/index.md': '# Upgrade' };
  for (const [path, body] of Object.entries(files)) {
    const file = join(dir, ...path.split('/'));
    await mkdir(join(file, '..'), { recursive: true });
    await writeFile(file, body, 'utf8');
  }
};

describe('docs bundling', () => {
  let root: string;
  let source: string;
  let dest: string;

  beforeEach(async () => {
    root = await mkdtemp(join(tmpdir(), 'mcp-bundle-'));
    source = join(root, 'markdown');
    dest = join(root, 'gremlin-docs');
  });

  afterEach(async () => {
    await rm(root, { recursive: true, force: true });
  });

  it('bundles the selected pages under the version stamped in the docs', async () => {
    await writeDocsBuild(source, '4.0.0-SNAPSHOT');
    const message = await bundleDocs(source, dest, '4.0.0-SNAPSHOT', true);
    expect(message).toContain('Bundled 7 docs pages for 4.0.0-SNAPSHOT');

    const store = await loadDocsStore({ bundledRoot: dest, installRoot: join(root, 'none') });
    expect(store.problems).toEqual([]);
    const pages = [...(store.versions.get('4.0.0-SNAPSHOT')?.pages.keys() ?? [])];
    expect(pages).toHaveLength(7);
    expect(pages).not.toContain('upgrade/index.md');
    expect(pages).not.toContain('dev/provider/providers.md');

    // The distributed files keep the license header the docs build wrote.
    const bundled = await readFile(join(dest, '4.0.0-SNAPSHOT', 'reference', 'index.md'), 'utf8');
    expect(bundled).toContain('Licensed to the Apache');
  });

  it('warns and bundles nothing when there is no docs build', async () => {
    await mkdir(join(dest, 'stale'), { recursive: true });
    const message = await bundleDocs(source, dest, '4.0.0', false);
    expect(message).toMatch(/^WARNING: No Markdown docs found/);
    await expect(readdir(dest)).rejects.toThrow();
  });

  it('fails when docs are required but missing', async () => {
    await expect(bundleDocs(source, dest, '4.0.0', true)).rejects.toThrow('No Markdown docs found');
  });

  it('refuses docs built for another version', async () => {
    await writeDocsBuild(source, '3.8.2');
    expect(await bundleDocs(source, dest, '4.0.0', false)).toMatch(/^WARNING: .*are for 3\.8\.2/);
    await expect(bundleDocs(source, dest, '4.0.0', true)).rejects.toThrow('are for 3.8.2');
  });
});
