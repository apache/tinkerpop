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
import { mkdtemp, readdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { installDocs, pagesInLlmsTxt, runDocsCommand, type Fetch } from '../src/docs/install.js';
import { loadDocsStore, type DocsLocations } from '../src/docs/store.js';
import { docsTree, llmsTxt, writeFixtureVersion } from './docs-fixtures.js';

const SITE = 'https://tinkerpop.apache.org/docs';

/** Serves the fixture tree for the given versions as the TinkerPop site would. */
const fakeSite = (versions: string[], requested: string[] = [], failOn?: string): Fetch => {
  return async (url: string) => {
    requested.push(url);
    const respond = (status: number, body = '') => ({
      ok: status === 200,
      status,
      text: async () => body,
    });
    if (failOn !== undefined && url.endsWith(failOn)) return respond(500);
    for (const version of versions) {
      const base = `${SITE}/${version}/`;
      if (!url.startsWith(base)) continue;
      const path = url.slice(base.length);
      if (path === 'llms.txt') return respond(200, llmsTxt(version));
      const body = docsTree(version)[path];
      return body === undefined ? respond(404) : respond(200, body);
    }
    return respond(404);
  };
};

describe('docs install', () => {
  let locations: DocsLocations;
  let root: string;

  beforeEach(async () => {
    root = await mkdtemp(join(tmpdir(), 'mcp-install-'));
    locations = { bundledRoot: join(root, 'bundled'), installRoot: join(root, 'installed') };
  });

  afterEach(async () => {
    await rm(root, { recursive: true, force: true });
  });

  it('reads page paths for the version from llms.txt', () => {
    const pages = pagesInLlmsTxt(
      `- [a](${SITE}/3.8.2/reference/a.md): x\n- [b](${SITE}/3.8.1/reference/b.md)\n` +
        `- [c](${SITE}/3.8.2/dev/provider/c.md#frag)\n- [d](${SITE}/3.8.2/images/d.png)\n` +
        `- [e](${SITE}/3.8.2/reference/../../../e.md)\n- [f](${SITE}/3.8.2/reference/..\\f.md)`,
      `${SITE}/3.8.2/`
    );
    expect(pages).toEqual(['reference/a.md', 'dev/provider/c.md']);
  });

  it('installs the selected pages without fetching unrelated provider pages', async () => {
    const requested: string[] = [];
    const { dir, pages } = await installDocs(
      '3.8.2',
      locations.installRoot,
      fakeSite(['3.8.2'], requested)
    );
    expect(dir).toBe(join(locations.installRoot, '3.8.2'));
    expect(pages).toBe(7);
    expect(requested).not.toContain(`${SITE}/3.8.2/dev/provider/providers.md`);
    expect(new Set(requested).size).toBe(requested.length);

    const store = await loadDocsStore(locations);
    expect(store.problems).toEqual([]);
    expect(store.versions.get('3.8.2')?.origin).toBe('installed');
    expect(store.versions.get('3.8.2')?.pages.size).toBe(7);
  });

  it('reports a version with no published Markdown docs', async () => {
    await expect(installDocs('3.8.1', locations.installRoot, fakeSite(['3.8.2']))).rejects.toThrow(
      'No Markdown docs are published for TinkerPop 3.8.1'
    );
  });

  it('rejects an invalid version without fetching', async () => {
    const requested: string[] = [];
    await expect(
      installDocs('../3.8.2', locations.installRoot, fakeSite(['3.8.2'], requested))
    ).rejects.toThrow('is not a TinkerPop version');
    expect(requested).toEqual([]);
  });

  it('keeps an existing install when a download fails', async () => {
    await writeFixtureVersion(locations.installRoot, '3.8.2');
    await expect(
      installDocs('3.8.2', locations.installRoot, fakeSite(['3.8.2'], [], 'reference/index.md'))
    ).rejects.toThrow('HTTP 500');
    expect(await readdir(locations.installRoot)).toEqual(['3.8.2']);
    expect((await loadDocsStore(locations)).versions.has('3.8.2')).toBe(true);
  });

  it('runs install and list as commands', async () => {
    await writeFixtureVersion(locations.bundledRoot, '4.0.0');
    const out: string[] = [];
    const err: string[] = [];
    const run = (args: string[]) =>
      runDocsCommand(
        args,
        locations,
        fakeSite(['3.8.2']),
        l => out.push(l),
        l => err.push(l)
      );

    expect(await run(['install', '3.8.2'])).toBe(0);
    expect(out.join('\n')).toContain('Restart gremlin-mcp');
    out.length = 0;

    expect(await run(['list'])).toBe(0);
    expect(out).toEqual(['4.0.0\tbundled\t7 pages', '3.8.2\tinstalled\t7 pages']);

    expect(await run(['install', '3.8.1'])).toBe(1);
    expect(err.pop()).toContain('No Markdown docs are published');

    expect(await run(['remove', '3.8.2'])).toBe(2);
    expect(err.pop()).toContain('Usage:');
  });
});
