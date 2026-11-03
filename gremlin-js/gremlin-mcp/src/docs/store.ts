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
 * @fileoverview Loads the TinkerPop docs versions gremlin-mcp can serve.
 *
 * A docs version is a directory named for the TinkerPop version, holding the selected
 * Markdown pages at their docs-relative paths plus a `manifest.json` listing them. Versions
 * come from two roots: the docs bundled in the gremlin-mcp package, and those added with
 * `gremlin-mcp docs install` under `~/.gremlin-mcp/docs/`. The set is read once at startup;
 * a version installed later is served after a restart.
 */

import { mkdir, readFile, readdir, rename, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { parseSections, type DocSection } from './sections.js';
import {
  isPageId,
  isRelativeTarget,
  resolveRelative,
  sourceOf,
  stripFragment,
  type DocSource,
} from './selection.js';

export const DOCS_SITE_URL = 'https://tinkerpop.apache.org/docs';
export const MANIFEST_FILE = 'manifest.json';

// A released or snapshot TinkerPop version, e.g. 3.8.2, 4.0.0-beta.3 or 4.0.0-SNAPSHOT.
const VERSION_PATTERN = /^\d+\.\d+\.\d+(?:-[0-9A-Za-z.]+)?$/;

/** True if the string is a TinkerPop version usable as a docs directory name. */
export const isDocsVersion = (version: string): boolean =>
  VERSION_PATTERN.test(version) && !version.includes('..');

export type DocsOrigin = 'bundled' | 'installed';

/** The manifest written beside a version's pages. */
export interface DocsManifest {
  readonly version: string;
  readonly source: string;
  readonly created: string;
  readonly pages: readonly string[];
}

/** One served docs page. */
export interface DocPage {
  /** Docs-relative path, e.g. `reference/the-traversal.md`; the id the tools use. */
  readonly id: string;
  readonly title: string;
  readonly summary: string;
  readonly source: DocSource;
  /** Page Markdown with links rewritten to page ids or absolute site URLs. */
  readonly markdown: string;
  /** The page's anchored sections, in page order; the first is usually the whole page. */
  readonly sections: readonly DocSection[];
}

/** The pages of one docs version. */
export interface DocsVersion {
  readonly version: string;
  readonly origin: DocsOrigin;
  readonly pages: ReadonlyMap<string, DocPage>;
}

/** Where docs versions are read from. */
export interface DocsLocations {
  /** Docs bundled in the gremlin-mcp package. */
  readonly bundledRoot: string;
  /** Docs added with `gremlin-mcp docs install`. */
  readonly installRoot: string;
}

/** The docs versions available to the tools, and any that could not be loaded. */
export interface DocsStore {
  readonly versions: ReadonlyMap<string, DocsVersion>;
  readonly problems: readonly string[];
}

const HEADING = /^#{1,6}\s+(.+?)\s*#*\s*$/;
// The ASF license header the docs build writes as an HTML comment at the top of every page.
const LICENSE_COMMENT = /<!--\s*Licensed to the Apache Software Foundation[\s\S]*?-->\n*/;

/**
 * Removes the license header comment from a page. The header stays in the files gremlin-mcp
 * distributes, but it is not documentation, so the tools never hand it to an agent.
 */
export const stripLicenseComment = (raw: string): string => raw.replace(LICENSE_COMMENT, '');
const SUMMARY = /^<!-- llms-summary: (.*) -->$/;
const LINK = /(\]\()([^)\s]+)(\))/g;

/**
 * Builds a page from its Markdown: the first heading is the title and the first llms-summary
 * comment the summary. The license header comment is removed. Relative links to other served pages become their page ids, and other
 * relative links become absolute URLs on the TinkerPop site for this version.
 */
export const parsePage = (
  id: string,
  raw: string,
  version: string,
  servedIds: ReadonlySet<string>
): DocPage => {
  const text = stripLicenseComment(raw);
  let title = '';
  let summary = '';
  for (const line of text.split('\n')) {
    if (title === '') {
      const heading = HEADING.exec(line);
      if (heading?.[1] !== undefined) title = heading[1];
    }
    if (summary === '') {
      const match = SUMMARY.exec(line.trim());
      // The docs build escapes `-->` inside the summary comment; show agents the original text.
      if (match?.[1] !== undefined) summary = match[1].replaceAll('--&gt;', '-->');
    }
    if (title !== '' && summary !== '') break;
  }

  const markdown = text.replace(LINK, (whole, open: string, target: string, close: string) => {
    if (!isRelativeTarget(target)) return whole;
    const path = stripFragment(target);
    const fragment = target.slice(path.length);
    const resolved = path === '' ? undefined : resolveRelative(id, path);
    if (resolved === undefined) return whole;
    const rewritten = servedIds.has(resolved)
      ? `${resolved}${fragment}`
      : `${DOCS_SITE_URL}/${version}/${resolved}${fragment}`;
    return `${open}${rewritten}${close}`;
  });

  return {
    id,
    title: title || id,
    summary,
    source: sourceOf(id),
    markdown,
    sections: parseSections(markdown),
  };
};

const readManifest = async (dir: string): Promise<DocsManifest> => {
  const parsed = JSON.parse(
    await readFile(join(dir, MANIFEST_FILE), 'utf8')
  ) as Partial<DocsManifest>;
  if (
    typeof parsed.version !== 'string' ||
    !Array.isArray(parsed.pages) ||
    !parsed.pages.every(p => typeof p === 'string')
  ) {
    throw new Error(`invalid ${MANIFEST_FILE}`);
  }
  return {
    version: parsed.version,
    source: String(parsed.source ?? ''),
    created: String(parsed.created ?? ''),
    pages: parsed.pages,
  };
};

/** Loads one version directory, failing if its manifest or any listed page is unusable. */
export const loadVersion = async (
  dir: string,
  version: string,
  origin: DocsOrigin
): Promise<DocsVersion> => {
  const manifest = await readManifest(dir);
  if (manifest.version !== version) {
    throw new Error(`manifest is for version ${manifest.version}`);
  }
  if (manifest.pages.length === 0) throw new Error('manifest lists no pages');

  const servedIds = new Set(manifest.pages);
  const pages = new Map<string, DocPage>();
  for (const id of manifest.pages) {
    if (!isPageId(id)) throw new Error(`invalid page path ${id}`);
    let raw: string;
    try {
      raw = await readFile(join(dir, ...id.split('/')), 'utf8');
    } catch {
      throw new Error(`missing page ${id}`);
    }
    pages.set(id, parsePage(id, raw, version, servedIds));
  }
  return { version, origin, pages };
};

/** Lists the version directory names under a root, ignoring anything that is not a version. */
export const listVersionDirs = async (root: string): Promise<string[]> => {
  try {
    const entries = await readdir(root, { withFileTypes: true });
    return entries.filter(e => e.isDirectory() && isDocsVersion(e.name)).map(e => e.name);
  } catch {
    return [];
  }
};

/**
 * Loads every docs version from the bundled and installed roots. An installed version
 * replaces a bundled one of the same version. A version that fails to load is left out and
 * reported in `problems`.
 */
export const loadDocsStore = async (locations: DocsLocations): Promise<DocsStore> => {
  const versions = new Map<string, DocsVersion>();
  const problems: string[] = [];

  const roots: Array<[string, DocsOrigin]> = [
    [locations.bundledRoot, 'bundled'],
    [locations.installRoot, 'installed'],
  ];
  for (const [root, origin] of roots) {
    for (const version of await listVersionDirs(root)) {
      try {
        versions.set(version, await loadVersion(join(root, version), version, origin));
      } catch (error) {
        const reason = error instanceof Error ? error.message : String(error);
        const fix =
          origin === 'installed' ? `; reinstall it with: gremlin-mcp docs install ${version}` : '';
        problems.push(`Could not load ${origin} docs ${version} (${reason})${fix}`);
      }
    }
  }

  return { versions: new Map([...versions].sort(([a], [b]) => compareVersions(a, b))), problems };
};

/** Orders versions numerically, newest first; a pre-release sorts before its release. */
export const compareVersions = (a: string, b: string): number => {
  const parse = (v: string) => {
    const [core = '', pre] = v.split(/-(.*)/s);
    return { nums: core.split('.').map(Number), pre };
  };
  const pa = parse(a);
  const pb = parse(b);
  for (let i = 0; i < 3; i++) {
    const diff = (pb.nums[i] ?? 0) - (pa.nums[i] ?? 0);
    if (diff !== 0) return diff;
  }
  if (pa.pre === pb.pre) return 0;
  if (pa.pre === undefined) return -1;
  if (pb.pre === undefined) return 1;
  return pb.pre.localeCompare(pa.pre, undefined, { numeric: true });
};

/**
 * Writes a docs version under a root, replacing any existing copy only once every page has
 * been written. The existing copy is moved aside before the new one is renamed into place and
 * moved back if that rename fails, so a failed write leaves the previous copy in place.
 */
export const writeVersion = async (
  root: string,
  manifest: DocsManifest,
  contents: ReadonlyMap<string, string>
): Promise<string> => {
  if (!isDocsVersion(manifest.version)) throw new Error(`invalid version ${manifest.version}`);
  const badId = manifest.pages.find(id => !isPageId(id));
  if (badId !== undefined) throw new Error(`invalid page path ${badId}`);
  const target = join(root, manifest.version);
  const staging = join(root, `.${manifest.version}.${process.pid}.tmp`);
  const previous = join(root, `.${manifest.version}.${process.pid}.old`);
  await rm(staging, { recursive: true, force: true });
  await rm(previous, { recursive: true, force: true });
  try {
    for (const id of manifest.pages) {
      const body = contents.get(id);
      if (body === undefined) throw new Error(`no content for page ${id}`);
      const file = join(staging, ...id.split('/'));
      await mkdir(join(file, '..'), { recursive: true });
      await writeFile(file, body, 'utf8');
    }
    await writeFile(join(staging, MANIFEST_FILE), `${JSON.stringify(manifest, null, 2)}\n`, 'utf8');
    let movedAside = false;
    try {
      await rename(target, previous);
      movedAside = true;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
    }
    try {
      await rename(staging, target);
    } catch (error) {
      if (movedAside) await rename(previous, target);
      throw error;
    }
    await rm(previous, { recursive: true, force: true });
  } finally {
    await rm(staging, { recursive: true, force: true });
  }
  return target;
};
