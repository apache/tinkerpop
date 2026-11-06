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
 * @fileoverview Keyword search over the pages of one docs version.
 *
 * BM25 ranking over three weighted fields (title, llms-summary, body). Tokenization knows
 * about Gremlin step names: `hasLabel` is indexed as `haslabel`, `has` and `label`, so both
 * the step name and its parts match. At a discount, a query term of three or more characters
 * also matches longer terms it prefixes (`merge` finds `mergev` and `mergee`), and indexed
 * terms of five or more characters that prefix it (`concatenation` finds `concat`). A page
 * whose title is exactly a step named in the query gets a bonus. For a large page, a result
 * also names the section whose own text best matches, so it can be fetched alone.
 */

import type { DocSource } from './selection.js';
import type { DocSection } from './sections.js';
import type { DocPage } from './store.js';

/** One search hit. */
export interface SearchResult {
  readonly page: string;
  readonly title: string;
  readonly summary: string;
  readonly source: DocSource;
  readonly snippet: string;
  /**
   * For a large page, the best-matching section as `page#anchor`; passing it to
   * get_gremlin_doc_page returns only that section.
   */
  readonly section?: string;
  readonly sectionTitle?: string;
}

type Field = 'title' | 'summary' | 'body';

const FIELD_WEIGHTS: Record<Field, number> = { title: 4, summary: 2, body: 1 };
const K1 = 1.2;
const B = 0.75;
const PREFIX_WEIGHT = 0.5;
const MAX_PREFIX_EXPANSIONS = 25;
const MIN_PREFIX_LENGTH = 3;
const MIN_STEM_LENGTH = 5;
const STEP_TITLE_BONUS = 10;
const SNIPPET_LENGTH = 240;
// Pages smaller than this are returned whole, so search does not point into them.
const SECTION_PAGE_MIN_BYTES = 10_000;
const SECTION_HEADING_WEIGHT = 4;

/**
 * Splits text into lowercase search terms. A camelCase or mixed word yields the whole word
 * and its parts, so `mergeV` gives `mergev`, `merge` and `v`.
 */
export const tokenize = (text: string): string[] => {
  const terms: string[] = [];
  for (const word of text.match(/[A-Za-z0-9]+/g) ?? []) {
    const whole = word.toLowerCase();
    terms.push(whole);
    const parts = word
      .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
      .replace(/([A-Z])([A-Z][a-z])/g, '$1 $2')
      .split(' ');
    if (parts.length > 1) terms.push(...parts.map(p => p.toLowerCase()));
  }
  return terms;
};

/** A page title reduced to a step name: `MergeV Step` and `mergeV()` both give `mergev`. */
const stepNameOf = (title: string): string | undefined => {
  const name = title
    .replace(/\(\)/g, '')
    .replace(/\s+step$/i, '')
    .trim()
    .toLowerCase();
  return /^[a-z][a-z0-9]*$/.test(name) ? name : undefined;
};

// Removes lines that carry no prose for snippets: anchors, comments, images, the llms pointer.
const isProseLine = (line: string): boolean => {
  const t = line.trim();
  return (
    t.length > 0 &&
    !t.startsWith('<a id=') &&
    !t.startsWith('<!--') &&
    !t.startsWith('![') &&
    !t.startsWith('> For the complete documentation index') &&
    !/^#{1,6}\s/.test(t) &&
    !/^```/.test(t)
  );
};

interface IndexedSection {
  readonly section: DocSection;
  readonly heading: Map<string, number>;
  readonly own: Map<string, number>;
}

interface IndexedPage {
  readonly page: DocPage;
  /** Sections after the page's first, for pages large enough to serve in part. */
  readonly sections: readonly IndexedSection[];
  readonly lengths: Record<Field, number>;
  readonly counts: Record<Field, Map<string, number>>;
  readonly stepName: string | undefined;
}

/** A search index over the pages of one docs version. */
export class DocsSearchIndex {
  private readonly pages: IndexedPage[];
  private readonly avgLength: Record<Field, number>;
  private readonly documentFrequency = new Map<string, number>();
  private readonly vocabulary: string[];

  constructor(pages: Iterable<DocPage>) {
    this.pages = [...pages].map(page => {
      const text: Record<Field, string> = {
        title: page.title,
        summary: page.summary,
        body: page.markdown,
      };
      const counts = {} as Record<Field, Map<string, number>>;
      const lengths = {} as Record<Field, number>;
      for (const field of Object.keys(FIELD_WEIGHTS) as Field[]) {
        const terms = tokenize(text[field]);
        const map = new Map<string, number>();
        for (const term of terms) map.set(term, (map.get(term) ?? 0) + 1);
        counts[field] = map;
        lengths[field] = terms.length;
      }
      const sections =
        page.markdown.length < SECTION_PAGE_MIN_BYTES
          ? []
          : page.sections.slice(1).map(section => ({
              section,
              heading: termCounts(section.title),
              own: termCounts(section.ownText),
            }));
      return { page, counts, lengths, sections, stepName: stepNameOf(page.title) };
    });

    for (const indexed of this.pages) {
      const terms = new Set<string>();
      for (const field of Object.keys(FIELD_WEIGHTS) as Field[]) {
        for (const term of indexed.counts[field].keys()) terms.add(term);
      }
      for (const term of terms) {
        this.documentFrequency.set(term, (this.documentFrequency.get(term) ?? 0) + 1);
      }
    }
    this.vocabulary = [...this.documentFrequency.keys()].sort();

    const total = Math.max(this.pages.length, 1);
    this.avgLength = { title: 0, summary: 0, body: 0 };
    for (const field of Object.keys(FIELD_WEIGHTS) as Field[]) {
      const sum = this.pages.reduce((acc, p) => acc + p.lengths[field], 0);
      this.avgLength[field] = Math.max(sum / total, 1);
    }
  }

  /** Ranks pages for a query, best first. */
  search(query: string, limit = 10): SearchResult[] {
    const queryTerms = [...new Set(tokenize(query))];
    if (queryTerms.length === 0) return [];

    // Each query term matches itself at full weight, and at a discount both the longer terms
    // it prefixes and the shorter indexed terms that prefix it (concatenation -> concat).
    const expansions = queryTerms.map(term => {
      const matches: Array<[string, number]> = [];
      if (this.documentFrequency.has(term)) matches.push([term, 1]);
      if (term.length >= MIN_PREFIX_LENGTH) {
        let found = 0;
        for (let i = lowerBound(this.vocabulary, term); i < this.vocabulary.length; i++) {
          const candidate = this.vocabulary[i] as string;
          if (!candidate.startsWith(term)) break;
          if (candidate === term) continue;
          matches.push([candidate, PREFIX_WEIGHT]);
          if (++found >= MAX_PREFIX_EXPANSIONS) break;
        }
      }
      for (let len = MIN_STEM_LENGTH; len < term.length; len++) {
        const stem = term.slice(0, len);
        if (this.documentFrequency.has(stem)) matches.push([stem, PREFIX_WEIGHT]);
      }
      return matches;
    });
    const queryWords = new Set((query.match(/[A-Za-z0-9]+/g) ?? []).map(w => w.toLowerCase()));

    const n = this.pages.length;
    const idfOf = (term: string): number => {
      const df = this.documentFrequency.get(term) ?? 0;
      return Math.log(1 + (n - df + 0.5) / (df + 0.5));
    };
    const scored = this.pages
      .map(indexed => {
        let score = 0;
        for (const matches of expansions) {
          for (const [term, weight] of matches) {
            const idf = idfOf(term);
            for (const field of Object.keys(FIELD_WEIGHTS) as Field[]) {
              const tf = indexed.counts[field].get(term) ?? 0;
              if (tf === 0) continue;
              const norm = 1 - B + B * (indexed.lengths[field] / this.avgLength[field]);
              score += weight * FIELD_WEIGHTS[field] * idf * ((tf * (K1 + 1)) / (tf + K1 * norm));
            }
          }
        }
        if (score > 0 && indexed.stepName !== undefined && queryWords.has(indexed.stepName)) {
          score += STEP_TITLE_BONUS;
        }
        return { indexed, score };
      })
      .filter(s => s.score > 0)
      .sort((a, b) => b.score - a.score || a.indexed.page.id.localeCompare(b.indexed.page.id));

    const matchedTerms = new Set(expansions.flat().map(([term]) => term));
    return scored.slice(0, Math.max(limit, 0)).map(({ indexed }) => {
      const best = bestSection(indexed.sections, expansions, idfOf);
      const result: SearchResult = {
        page: indexed.page.id,
        title: indexed.page.title,
        summary: indexed.page.summary,
        source: indexed.page.source,
        snippet: snippetFor(best?.markdown ?? indexed.page.markdown, matchedTerms),
      };
      return best === undefined
        ? result
        : { ...result, section: `${indexed.page.id}#${best.anchor}`, sectionTitle: best.title };
    });
  }
}

const termCounts = (text: string): Map<string, number> => {
  const counts = new Map<string, number>();
  for (const term of tokenize(text)) counts.set(term, (counts.get(term) ?? 0) + 1);
  return counts;
};

/**
 * The section whose own text best matches the query terms, scoring heading matches above
 * body matches and damping repeated body matches. Sections are scored on their own text, not
 * their sub-sections, so the deepest section holding the match wins over its parents.
 */
const bestSection = (
  sections: readonly IndexedSection[],
  expansions: ReadonlyArray<ReadonlyArray<[string, number]>>,
  idfOf: (term: string) => number
): DocSection | undefined => {
  let best: DocSection | undefined;
  let bestScore = 0;
  for (const indexed of sections) {
    let score = 0;
    for (const matches of expansions) {
      for (const [term, weight] of matches) {
        const headingTf = indexed.heading.get(term) ?? 0;
        const ownTf = indexed.own.get(term) ?? 0;
        const tf = SECTION_HEADING_WEIGHT * headingTf + (ownTf > 0 ? 1 + Math.log(ownTf) : 0);
        score += weight * idfOf(term) * tf;
      }
    }
    if (score > bestScore) {
      best = indexed.section;
      bestScore = score;
    }
  }
  return best;
};

const lowerBound = (sorted: readonly string[], value: string): number => {
  let lo = 0;
  let hi = sorted.length;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    if ((sorted[mid] as string) < value) lo = mid + 1;
    else hi = mid;
  }
  return lo;
};

/** The first prose line containing a matched term, trimmed to a short window around it. */
const snippetFor = (markdown: string, terms: ReadonlySet<string>): string => {
  const lines = markdown.split('\n').filter(isProseLine);
  const hit = lines.find(line => tokenize(line).some(t => terms.has(t))) ?? lines[0] ?? '';
  const text = hit.trim().replace(/\s+/g, ' ');
  if (text.length <= SNIPPET_LENGTH) return text;
  const lower = text.toLowerCase();
  const at = Math.max(
    0,
    Math.min(...[...terms].map(t => lower.indexOf(t)).filter(i => i >= 0), text.length)
  );
  const start = Math.max(0, Math.min(at - SNIPPET_LENGTH / 3, text.length - SNIPPET_LENGTH));
  const window = text.slice(start, start + SNIPPET_LENGTH);
  return `${start > 0 ? '…' : ''}${window}${start + SNIPPET_LENGTH < text.length ? '…' : ''}`;
};
