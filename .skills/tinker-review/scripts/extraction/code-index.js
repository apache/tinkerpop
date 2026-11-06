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
 * @typedef {object} CodeSymbol
 * @property {string} name - function, type or field name as extracted
 * @property {"function"|"type"|"field"} kind
 * @property {number} start - first line (1-based, PR head side)
 * @property {number} end - last line (inclusive)
 */

/**
 * @typedef {object} CodeIndexFile
 * @property {Array<[number, number]>} hunks - changed line ranges on the PR head side, ascending
 * @property {CodeSymbol[]} symbols
 */

/**
 * @typedef {object} CodeIndex
 * @property {Object<string, CodeIndexFile>} files - keyed by repo-relative path
 */

/**
 * Parse `git diff --unified=0` output into the changed line ranges on the new
 * (PR head) side of each file. A pure deletion (`+N,0`) has no new-side lines, so it
 * is recorded as `[N, N]`, the head line it follows, which GitHub shows as context
 * beside the removed lines; that lets a symbol whose only change is a removal still
 * link into the diff. A file deleted by the PR has no entry.
 *
 * @param {string} diffText
 * @returns {Object<string, Array<[number, number]>>}
 */
export function parseHunks(diffText) {
  const hunks = {};
  let current = null;
  for (const line of (diffText || "").split("\n")) {
    if (line.startsWith("+++ ")) {
      const target = line.slice(4).trim();
      current = target === "/dev/null" ? null : target.replace(/^b\//, "");
      if (current && !hunks[current]) hunks[current] = [];
      continue;
    }
    if (!current || !line.startsWith("@@")) continue;
    const m = /^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@/.exec(line);
    if (!m) continue;
    const start = Number(m[1]);
    const count = m[2] === undefined ? 1 : Number(m[2]);
    if (count > 0) hunks[current].push([start, start + count - 1]);
    else hunks[current].push([Math.max(start, 1), Math.max(start, 1)]);
  }
  for (const ranges of Object.values(hunks)) ranges.sort((a, b) => a[0] - b[0]);
  return hunks;
}

/**
 * Build the code index the renderer uses to turn a `{file, symbol}` reference into
 * a link: for every changed file, its changed hunks and the line range of each
 * function, type and field extracted from it. Files outside `changedFiles` (such as the
 * hierarchy neighborhood parsed for context) are left out.
 *
 * @param {object} params
 * @param {string[]} params.changedFiles
 * @param {{functions: object[], types: object[], fields?: object[]}} params.extraction
 * @param {string} params.diffText - `git diff --unified=0` of the PR against its merge base
 * @returns {CodeIndex}
 */
export function buildCodeIndex({ changedFiles, extraction, diffText }) {
  const hunks = parseHunks(diffText);
  const files = {};
  for (const path of changedFiles) {
    if (!hunks[path]) continue;
    files[path] = { hunks: hunks[path], symbols: [] };
  }
  const add = (record, kind) => {
    const entry = files[record.filePath];
    if (!entry || !record.linesStart || !record.linesEnd) return;
    entry.symbols.push({ name: record.name, kind, start: record.linesStart, end: record.linesEnd });
  };
  for (const t of extraction.types || []) add(t, "type");
  for (const f of extraction.functions || []) add(f, "function");
  for (const f of extraction.fields || []) add(f, "field");
  for (const entry of Object.values(files)) entry.symbols.sort((a, b) => a.start - b.start);
  return { files };
}
