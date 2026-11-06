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
 * Change-level vocabulary. Every File, Function and Type vertex carries a
 * `changeLevel` recording how much the PR moved it, computed by diffing the base
 * version of each changed file against the PR-head version. It replaces the old
 * boolean `changed` flag, which — being stamped per file — could not tell an
 * untouched helper in a changed file from the code the PR actually rewrote.
 *
 *   NONE        — byte-identical to the base (untouched; also context /
 *                 hierarchy-neighborhood files and external stubs).
 *   FORMATTING  — only comments or whitespace moved; the tokens are identical.
 *   BEHAVIORAL  — the body changed for real, but the signature is stable.
 *   STRUCTURAL  — the signature/declaration changed, or the member/file was
 *                 added or removed, or imports/exports changed.
 *
 * Risk checks (centrality, blast-radius, coverage-gaps) default to the two
 * meaningful tiers (BEHAVIORAL, STRUCTURAL); inclusion-oriented checks (orphans,
 * cluster-analysis, architecture) count anything that is not NONE.
 */

import gremlin from "gremlin";

const { process: { P } } = gremlin;

export const CHANGE_LEVEL = Object.freeze({
  NONE: "NONE",
  FORMATTING: "FORMATTING",
  BEHAVIORAL: "BEHAVIORAL",
  STRUCTURAL: "STRUCTURAL",
});

// Ascending severity. Index in this array is the rank used to combine levels.
export const ORDER = Object.freeze([
  CHANGE_LEVEL.NONE,
  CHANGE_LEVEL.FORMATTING,
  CHANGE_LEVEL.BEHAVIORAL,
  CHANGE_LEVEL.STRUCTURAL,
]);

const RANK = new Map(ORDER.map((level, i) => [level, i]));

const VALID = new Set(ORDER);

export function isValidChangeLevel(value) {
  return VALID.has(value);
}

/**
 * Normalize a caller-supplied change level (e.g. a `--changeLevel` flag),
 * upper-casing and falling back when missing or invalid.
 *
 * @param {string|undefined} value
 * @param {string|null} [fallback=null]
 * @returns {string|null}
 */
export function normalizeChangeLevel(value, fallback = null) {
  if (typeof value === "string") {
    const upper = value.toUpperCase();
    if (VALID.has(upper)) return upper;
  }
  return fallback;
}

/**
 * The most severe of the given levels (NONE if none supplied). Used to roll a
 * file's members up into a single File.changeLevel.
 *
 * @param {...string} levels
 * @returns {string}
 */
export function maxLevel(...levels) {
  let best = CHANGE_LEVEL.NONE;
  for (const level of levels) {
    if (!VALID.has(level)) continue;
    if (RANK.get(level) > RANK.get(best)) best = level;
  }
  return best;
}

/**
 * True if `level` is at least as severe as `floor`.
 *
 * @param {string} level
 * @param {string} floor
 * @returns {boolean}
 */
export function atLeast(level, floor) {
  return VALID.has(level) && VALID.has(floor) && RANK.get(level) >= RANK.get(floor);
}

/**
 * @typedef {Object} MemberFingerprint
 * @property {string} signature  the member's signature (name+params for a
 *                                function; kind+visibility+supertypes+member-set
 *                                for a type)
 * @property {string} rawHash    hash of the member's exact source text
 * @property {string} normHash   hash of the member's leaf tokens, comments removed
 */

/**
 * Grade one head member against its matched base member. A missing base member
 * (no match by name/signature) is a new member — STRUCTURAL.
 *
 * @param {MemberFingerprint|null|undefined} base
 * @param {MemberFingerprint} head
 * @returns {string} a CHANGE_LEVEL value
 */
export function gradeMember(base, head) {
  if (!base) return CHANGE_LEVEL.STRUCTURAL;              // added member
  if (base.rawHash === head.rawHash) return CHANGE_LEVEL.NONE;
  if (base.signature !== head.signature) return CHANGE_LEVEL.STRUCTURAL;
  if (base.normHash === head.normHash) return CHANGE_LEVEL.FORMATTING;
  return CHANGE_LEVEL.BEHAVIORAL;
}

/**
 * Roll a file's members and its import/export delta into a single File level.
 * An import/export set change is STRUCTURAL (a dependency-surface change). If the
 * raw file bytes differ but every member is NONE and imports are stable, the
 * change is top-level trivia outside any member — a FORMATTING floor.
 *
 * @param {object} params
 * @param {string[]} params.memberLevels     changeLevel of every Function/Type in the file
 * @param {boolean}  params.importExportChanged  whether the import/export set moved
 * @param {boolean}  params.rawFileChanged     whether the file's raw bytes differ from base
 * @returns {string} a CHANGE_LEVEL value
 */
export function gradeFile({ memberLevels = [], importExportChanged = false, rawFileChanged = false }) {
  let level = maxLevel(...memberLevels);
  if (importExportChanged) level = maxLevel(level, CHANGE_LEVEL.STRUCTURAL);
  if (level === CHANGE_LEVEL.NONE && rawFileChanged) return CHANGE_LEVEL.FORMATTING;
  return level;
}

// === Gremlin predicate helpers ===
// Keep the "what counts as changed" definition here so every check filters the
// same way rather than open-coding `within(...)`.

/** Any real change — everything but NONE. Inclusion-oriented checks use this. */
export function changedAny() {
  return P.within(CHANGE_LEVEL.FORMATTING, CHANGE_LEVEL.BEHAVIORAL, CHANGE_LEVEL.STRUCTURAL);
}

/** Meaningful change — BEHAVIORAL or STRUCTURAL. Risk checks default to this. */
export function changedMeaningful() {
  return P.within(CHANGE_LEVEL.BEHAVIORAL, CHANGE_LEVEL.STRUCTURAL);
}

/**
 * A predicate matching every level at least as severe as `floor`. Backs the
 * `minChangeLevel` opt-in on centrality/blast-radius.
 *
 * @param {string} floor - a CHANGE_LEVEL value
 * @returns {object} a gremlin P predicate
 */
export function atLeastP(floor) {
  const from = RANK.has(floor) ? RANK.get(floor) : 1;
  return P.within(...ORDER.slice(from));
}
