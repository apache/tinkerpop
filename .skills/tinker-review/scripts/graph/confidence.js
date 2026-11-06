/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 * Edge confidence vocabulary. Every edge written into the knowledge graph
 * carries a `confidence` property so downstream analysis (and the human
 * reviewer) can tell what was observed directly from source versus what was
 * deduced or guessed.
 *
 *   EXTRACTED  — explicitly present in the source: an AST definition, a git
 *                diff fact, a discussion link stated in the PR body/diff, a
 *                comment returned by the issue API for its parent thread.
 *   INFERRED   — a reasonable deduction: a call/test edge resolved by name
 *                match, a cross-referenced discussion, an agent's step or doc
 *                mapping backed by evidence.
 *   AMBIGUOUS  — uncertain; surfaced for human review. Keyword-search matches
 *                and low-confidence agent guesses land here.
 */
export const CONFIDENCE = Object.freeze({
  EXTRACTED: "EXTRACTED",
  INFERRED: "INFERRED",
  AMBIGUOUS: "AMBIGUOUS",
});

const VALID = new Set(Object.values(CONFIDENCE));

export function isValidConfidence(value) {
  return VALID.has(value);
}

/**
 * Normalize a caller-supplied confidence value, falling back to a default when
 * missing or invalid. Enrichment write commands use this so a bad `--confidence`
 * flag degrades to a sane default rather than poisoning the graph.
 *
 * @param {string|undefined} value
 * @param {string} [fallback=CONFIDENCE.INFERRED]
 * @returns {string}
 */
export function normalizeConfidence(value, fallback = CONFIDENCE.INFERRED) {
  if (typeof value === "string") {
    const upper = value.toUpperCase();
    if (VALID.has(upper)) return upper;
  }
  return fallback;
}

/**
 * Map a discussion-link `found_in` provenance to a confidence level.
 * A link stated in the PR body or diff is EXTRACTED; a cross-reference found in
 * another discussion's body is INFERRED; a keyword-search hit is AMBIGUOUS.
 *
 * @param {string|undefined} foundIn
 * @returns {string}
 */
export function confidenceForFoundIn(foundIn) {
  switch (foundIn) {
    case "pr":
    case "diff":
      return CONFIDENCE.EXTRACTED;
    case "search":
      return CONFIDENCE.AMBIGUOUS;
    case "jira_body":
    case "devlist_body":
      return CONFIDENCE.INFERRED;
    default:
      return CONFIDENCE.INFERRED;
  }
}
