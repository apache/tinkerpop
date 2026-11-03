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
 * @fileoverview Chooses which pages of a TinkerPop Markdown docs tree gremlin-mcp serves.
 *
 * gremlin-mcp serves the reference documentation plus the Gremlin Semantics section of the
 * provider documentation. Gremlin Semantics is not its own book: its pages sit in
 * `dev/provider/` beside unrelated provider pages, so they are found by starting at
 * `dev/provider/gremlin-semantics.md` and following the relative links it (and the pages it
 * reaches) make within `dev/provider/`. The same rule applies to a local docs build and to the
 * pages published for a version on the TinkerPop site.
 *
 * Page paths are always relative to the docs root and use `/` separators, for example
 * `reference/the-traversal.md` or `dev/provider/all-step.md`.
 */

export const REFERENCE_DIR = 'reference/';
export const SEMANTICS_DIR = 'dev/provider/';
export const SEMANTICS_ROOT = `${SEMANTICS_DIR}gremlin-semantics.md`;

/** The part of the docs a served page belongs to. */
export type DocSource = 'reference' | 'gremlin-semantics';

// Markdown inline link or image target: [label](target) / ![alt](target). Titles are not used
// by the docs converter, so the target runs to the closing parenthesis.
const LINK_TARGET = /\]\(([^)\s]+)\)/g;

/**
 * Returns the relative link targets in a Markdown page, without fragments. Absolute URLs,
 * site-absolute paths and pure fragment links are skipped.
 */
export const extractRelativeLinks = (markdown: string): string[] => {
  const targets: string[] = [];
  for (const match of markdown.matchAll(LINK_TARGET)) {
    const target = match[1];
    if (target === undefined || !isRelativeTarget(target)) continue;
    const path = stripFragment(target);
    if (path.length > 0) targets.push(path);
  }
  return targets;
};

/** True for a link target that is relative to the page holding it. */
export const isRelativeTarget = (target: string): boolean =>
  !/^[a-z][a-z0-9+.-]*:/i.test(target) && !target.startsWith('/') && !target.startsWith('#');

/** Removes a `#fragment` (and any `?query`) from a link target. */
export const stripFragment = (target: string): string => target.replace(/[?#].*$/, '');

/**
 * Resolves a relative link target against the path of the page that holds it. Returns
 * `undefined` if the result would escape the docs root.
 */
export const resolveRelative = (fromPage: string, target: string): string | undefined => {
  const segments = fromPage.split('/').slice(0, -1);
  for (const part of target.split('/')) {
    if (part === '' || part === '.') continue;
    if (part === '..') {
      if (segments.length === 0) return undefined;
      segments.pop();
    } else {
      segments.push(part);
    }
  }
  return segments.join('/');
};

/**
 * True for a page id that is safe to use as a path under a docs version directory: `/`
 * separated, with no `\`, no empty, `.` or `..` segments, and no leading `/`. Page ids come
 * from the TinkerPop site's llms.txt and from manifests on disk, so every place that turns one
 * into a file path checks it first.
 */
export const isPageId = (id: string): boolean =>
  id !== '' &&
  !id.includes('\\') &&
  id.split('/').every(part => part !== '' && part !== '.' && part !== '..');

/** The served part of the docs that a page path belongs to, if any. */
export const sourceOf = (path: string): DocSource =>
  path.startsWith(REFERENCE_DIR) ? 'reference' : 'gremlin-semantics';

/**
 * Selects the pages gremlin-mcp serves from the pages available in a docs tree.
 *
 * @param available - every Markdown page path in the tree, relative to the docs root
 * @param readPage - reads a page's Markdown; only called for Gremlin Semantics pages, whose
 *   links decide which provider pages belong to the section
 * @returns the selected page paths, sorted
 */
export const selectPages = async (
  available: Iterable<string>,
  readPage: (path: string) => Promise<string>
): Promise<string[]> => {
  const pages = new Set([...available].filter(p => p.endsWith('.md')));
  const selected = new Set([...pages].filter(p => p.startsWith(REFERENCE_DIR)));

  if (pages.has(SEMANTICS_ROOT)) {
    const queue = [SEMANTICS_ROOT];
    const seen = new Set(queue);
    while (queue.length > 0) {
      const page = queue.shift() as string;
      selected.add(page);
      for (const target of extractRelativeLinks(await readPage(page))) {
        const linked = resolveRelative(page, target);
        if (
          linked !== undefined &&
          linked.startsWith(SEMANTICS_DIR) &&
          pages.has(linked) &&
          !seen.has(linked)
        ) {
          seen.add(linked);
          queue.push(linked);
        }
      }
    }
  }

  return [...selected].sort();
};
