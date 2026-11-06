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
 * @fileoverview The `gremlin-mcp docs` commands: install a docs version from the TinkerPop
 * site, and list the versions available.
 *
 * Installing reads the version's published llms.txt, selects the reference and Gremlin
 * Semantics pages, downloads them and writes them under the install root, replacing any
 * existing copy. These are operator commands; the MCP tools only read what is already present.
 */

import { selectPages } from './selection.js';
import {
  DOCS_SITE_URL,
  isDocsVersion,
  loadDocsStore,
  writeVersion,
  type DocsLocations,
} from './store.js';

/** The fetch function used for downloads; injectable for tests. */
export type Fetch = (url: string) => Promise<Pick<Response, 'ok' | 'status' | 'text'>>;

const DOWNLOAD_CONCURRENCY = 8;

/** A failure the docs commands report to the user without a stack trace. */
export class DocsCommandError extends Error {}

/** The docs-relative page paths listed in a version's llms.txt. */
export const pagesInLlmsTxt = (llmsTxt: string, versionBase: string): string[] => {
  const pages = new Set<string>();
  for (const match of llmsTxt.matchAll(/\]\(([^)\s]+)\)/g)) {
    const url = match[1];
    if (url === undefined || !url.startsWith(versionBase)) continue;
    const path = url.slice(versionBase.length).replace(/[?#].*$/, '');
    if (path.endsWith('.md')) pages.add(path);
  }
  return [...pages];
};

/**
 * Downloads and installs one docs version.
 *
 * @returns where the version was written and how many pages it has
 */
export const installDocs = async (
  version: string,
  installRoot: string,
  fetchFn: Fetch,
  siteUrl = DOCS_SITE_URL
): Promise<{ dir: string; pages: number }> => {
  if (!isDocsVersion(version)) {
    throw new DocsCommandError(
      `"${version}" is not a TinkerPop version; give a full version such as 3.8.2 or 4.0.0`
    );
  }

  const versionBase = `${siteUrl}/${version}/`;
  const download = async (url: string): Promise<string> => {
    let response: Awaited<ReturnType<Fetch>>;
    try {
      response = await fetchFn(url);
    } catch (error) {
      throw new DocsCommandError(
        `Could not download ${url}: ${error instanceof Error ? error.message : String(error)}`
      );
    }
    if (!response.ok) {
      throw Object.assign(
        new DocsCommandError(`Could not download ${url}: HTTP ${response.status}`),
        {
          status: response.status,
        }
      );
    }
    return response.text();
  };

  let llmsTxt: string;
  try {
    llmsTxt = await download(`${versionBase}llms.txt`);
  } catch (error) {
    if ((error as { status?: number }).status === 404) {
      throw new DocsCommandError(
        `No Markdown docs are published for TinkerPop ${version}. Markdown docs are available ` +
          'from 3.7.7 and 3.8.2 onward.'
      );
    }
    throw error;
  }

  const fetched = new Map<string, Promise<string>>();
  const readPage = (path: string): Promise<string> => {
    let page = fetched.get(path);
    if (page === undefined) {
      page = download(`${versionBase}${path}`);
      fetched.set(path, page);
    }
    return page;
  };

  const pages = await selectPages(pagesInLlmsTxt(llmsTxt, versionBase), readPage);
  if (pages.length === 0) {
    throw new DocsCommandError(`The ${version} docs index lists no reference pages`);
  }

  const contents = new Map<string, string>();
  const queue = [...pages];
  await Promise.all(
    Array.from({ length: Math.min(DOWNLOAD_CONCURRENCY, queue.length) }, async () => {
      for (let page = queue.shift(); page !== undefined; page = queue.shift()) {
        contents.set(page, await readPage(page));
      }
    })
  );

  const dir = await writeVersion(
    installRoot,
    { version, source: versionBase, created: new Date().toISOString(), pages },
    contents
  );
  return { dir, pages: pages.length };
};

/** One line per docs version: version, where it comes from, and page count or problem. */
export const listDocs = async (locations: DocsLocations): Promise<string[]> => {
  const store = await loadDocsStore(locations);
  const lines = [...store.versions.values()].map(
    v => `${v.version}\t${v.origin}\t${v.pages.size} pages`
  );
  return [...lines, ...store.problems];
};

const USAGE = `Usage:
  gremlin-mcp docs install <version>   Download the docs for a TinkerPop version
  gremlin-mcp docs list                List the docs versions gremlin-mcp can serve

Installed docs are written to ~/.gremlin-mcp/docs/<version>/. Restart gremlin-mcp after
installing so the new version is offered to agents.`;

/**
 * Runs a `gremlin-mcp docs` command.
 *
 * @param args - the arguments after `docs`
 * @returns the process exit code
 */
export const runDocsCommand = async (
  args: readonly string[],
  locations: DocsLocations,
  fetchFn: Fetch,
  out: (line: string) => void,
  err: (line: string) => void
): Promise<number> => {
  const [command, ...rest] = args;
  try {
    if (command === 'install' && rest.length === 1) {
      const version = rest[0] as string;
      out(`Installing TinkerPop ${version} docs...`);
      const { dir, pages } = await installDocs(version, locations.installRoot, fetchFn);
      out(`Installed ${pages} pages to ${dir}`);
      out('Restart gremlin-mcp to make this version available to agents.');
      return 0;
    }
    if (command === 'list' && rest.length === 0) {
      const lines = await listDocs(locations);
      if (lines.length === 0) {
        out('No docs versions available. Install one with: gremlin-mcp docs install <version>');
      }
      lines.forEach(out);
      return 0;
    }
    err(USAGE);
    return command === undefined || command === 'help' || command === '--help' ? 0 : 2;
  } catch (error) {
    err(error instanceof DocsCommandError ? error.message : String(error));
    return 1;
  }
};
