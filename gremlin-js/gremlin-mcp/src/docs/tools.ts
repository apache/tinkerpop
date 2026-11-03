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
 * @fileoverview MCP tools serving TinkerPop reference and Gremlin Semantics docs.
 *
 * Both tools take a required `version` whose allowed values are the docs versions found at
 * startup, published as an enum in the tool schema so the agent can see what is available.
 * There is no default: the agent must pass the version of its target server, asking the user
 * when it does not know, because docs for the wrong version are the failure these tools exist
 * to prevent.
 */

import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { z } from 'zod';
import { TOOL_NAMES } from '../constants.js';
import {
  createErrorResponse,
  createStringResponse,
  createSuccessResponse,
} from '../handlers/tool-patterns.js';
import { DocsSearchIndex } from './search.js';
import { DOCS_SITE_URL, type DocPage, type DocsStore, type DocsVersion } from './store.js';
import { stripFragment } from './selection.js';

const DEFAULT_LIMIT = 10;
const MAX_LIMIT = 25;

/**
 * Resolves what an agent passed as a page to a page id. Accepts a page id, a link copied
 * from a page (with or without a fragment), a site URL for the version, or a bare file name
 * when only one page has it.
 */
export const resolvePageId = (docs: DocsVersion, input: string): string | undefined => {
  let candidate = stripFragment(input.trim());
  const siteBase = `${DOCS_SITE_URL}/${docs.version}/`;
  if (candidate.startsWith(siteBase)) candidate = candidate.slice(siteBase.length);
  candidate = candidate.replace(/^(\.\/|\/)+/, '');
  if (candidate === '') return undefined;
  if (!candidate.endsWith('.md')) candidate = `${candidate}.md`;
  if (docs.pages.has(candidate)) return candidate;

  const name = candidate.split('/').pop() as string;
  const matches = [...docs.pages.keys()].filter(id => id.split('/').pop() === name);
  return matches.length === 1 ? matches[0] : undefined;
};

/**
 * Registers the docs tools. Does nothing when no docs version is available, since the
 * version enum cannot be empty.
 *
 * @returns the versions the tools were registered with
 */
/** The `#fragment` of a page reference, without the `#`, if it has one. */
export const fragmentOf = (input: string): string | undefined => {
  const at = input.indexOf('#');
  const fragment = at < 0 ? '' : input.slice(at + 1).trim();
  return fragment === '' ? undefined : fragment;
};

/**
 * The Markdown to return for a page. With an anchor naming one of its sections (other than
 * the section that opens the page), only that section is returned, under a line naming the
 * full page. An anchor the page does not have returns the whole page with a note.
 */
export const pageContent = (page: DocPage, anchor: string | undefined): string => {
  if (anchor === undefined || anchor === page.sections[0]?.anchor) return page.markdown;
  const section = page.sections.find(s => s.anchor === anchor);
  if (section === undefined) {
    return `> No section #${anchor} on this page; showing the whole page.\n\n${page.markdown}`;
  }
  return (
    `> Section "${section.title}" of ${page.id} (${page.title}). Fetch ${page.id} without ` +
    `the #${anchor} fragment for the whole page.\n\n${section.markdown}\n`
  );
};

export function registerDocsTools(server: McpServer, store: DocsStore): string[] {
  const versions = [...store.versions.keys()];
  if (versions.length === 0) return [];

  const versionSchema = z
    .enum(versions as [string, ...string[]])
    .describe(
      'TinkerPop version of the target Gremlin Server or graph. Required. If you do not know ' +
        'it, ask the user rather than guessing; docs differ between versions. Only the listed ' +
        'versions are available here; the user can add others with ' +
        '"gremlin-mcp docs install <version>" and restarting the server.'
    );

  const searchInputSchema = z
    .object({
      query: z
        .string()
        .min(1, 'The search query cannot be empty')
        .max(500, 'The search query cannot exceed 500 characters')
        .describe(
          'Words to search for, such as a step name (mergeV) or a concept (type promotion)'
        ),
      version: versionSchema,
      limit: z
        .number()
        .int()
        .min(1)
        .max(MAX_LIMIT)
        .optional()
        .describe(`Maximum number of results (default ${DEFAULT_LIMIT})`),
    })
    .strict();

  const pageInputSchema = z
    .object({
      page: z
        .string()
        .min(1, 'The page cannot be empty')
        .describe(
          'Page id from search_gremlin_docs results (e.g. reference/the-traversal.md), or a ' +
            'link copied from a page. Add a #fragment, as in the section field of search ' +
            'results, to get only that section of a large page.'
        ),
      version: versionSchema,
    })
    .strict();

  const indexes = new Map<string, DocsSearchIndex>();
  const indexFor = (docs: DocsVersion): DocsSearchIndex => {
    let index = indexes.get(docs.version);
    if (index === undefined) {
      index = new DocsSearchIndex(docs.pages.values());
      indexes.set(docs.version, index);
    }
    return index;
  };

  server.registerTool(
    TOOL_NAMES.SEARCH_GREMLIN_DOCS,
    {
      title: 'Search Gremlin Docs',
      description:
        'Search the TinkerPop reference documentation and Gremlin Semantics for a specific ' +
        'TinkerPop version. Use it to check step syntax, behavior and semantics before writing ' +
        'or fixing Gremlin. Returns matching pages with a summary and snippet, and for large ' +
        'pages the best-matching section. Read a page or section with get_gremlin_doc_page.',
      inputSchema: searchInputSchema.shape,
    },
    async (args: unknown) => {
      const { query, version, limit } = searchInputSchema.parse(args);
      const docs = store.versions.get(version) as DocsVersion;
      const results = indexFor(docs).search(query, limit ?? DEFAULT_LIMIT);
      return createSuccessResponse({ version, query, results });
    }
  );

  server.registerTool(
    TOOL_NAMES.GET_GREMLIN_DOC_PAGE,
    {
      title: 'Get Gremlin Doc Page',
      description:
        'Read one page of the TinkerPop reference documentation or Gremlin Semantics for a ' +
        'specific TinkerPop version, as Markdown. Links in the page to other served pages are ' +
        'page ids that can be passed back to this tool. A page id with a #fragment returns ' +
        'only that section.',
      inputSchema: pageInputSchema.shape,
    },
    async (args: unknown) => {
      const { page, version } = pageInputSchema.parse(args);
      const docs = store.versions.get(version) as DocsVersion;
      const id = resolvePageId(docs, page);
      if (id === undefined) {
        const suggestions = indexFor(docs)
          .search(page.replace(/[-_/.]/g, ' '), 5)
          .map(r => r.page);
        return createErrorResponse(
          `No page "${page}" in the ${version} docs.` +
            (suggestions.length > 0 ? ` Similar pages: ${suggestions.join(', ')}` : '') +
            ' Use search_gremlin_docs to find page ids.'
        );
      }
      return createStringResponse(pageContent(docs.pages.get(id) as DocPage, fragmentOf(page)));
    }
  );

  return versions;
}
