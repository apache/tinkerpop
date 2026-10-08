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

import { createHash } from "node:crypto";

const REPO = "https://github.com/apache/tinkerpop";

/**
 * A reference from the report narrative to code in the PR. The agent names the
 * code; the renderer finds the lines, so no line number or URL is ever written by
 * hand.
 *
 * @typedef {object} CodeRef
 * @property {string} file - repo-relative path, or a unique suffix of one (e.g. "TinkerTransaction.java")
 * @property {string} [symbol] - function or type name; "Type.member" picks a member of that type
 *   (so "TinkerStorageGraph.TinkerStorageGraph" is the constructor, bare "TinkerStorageGraph" the class)
 * @property {[number, number]} [lines] - explicit line range on the PR head side, used instead of symbol
 * @property {string} [label] - chip text; defaults to the symbol, else the file name
 */

/**
 * @typedef {object} ResolvedRef
 * @property {string} label
 * @property {string|null} url - null when the reference could not be resolved
 * @property {string} title - tooltip: path and lines, or why it did not resolve
 * @property {"diff"|"blob"|"file"|null} target - which kind of link was produced
 */

/**
 * The anchor GitHub gives a file in a pull request's "Files changed" view.
 */
export function diffAnchor(path) {
  return "diff-" + createHash("sha256").update(path).digest("hex");
}

function findFile(codeIndex, file) {
  const files = (codeIndex && codeIndex.files) || {};
  if (files[file]) return { path: file };
  const matches = Object.keys(files).filter((p) => p.endsWith("/" + file));
  if (matches.length === 1) return { path: matches[0] };
  return { error: matches.length === 0 ? `no changed file matches "${file}"` : `"${file}" matches ${matches.length} changed files` };
}

function findSymbol(entry, symbol) {
  const dot = symbol.lastIndexOf(".");
  if (dot > 0) {
    const owner = symbol.slice(0, dot);
    const member = symbol.slice(dot + 1);
    const types = entry.symbols.filter((s) => s.kind === "type" && s.name === owner);
    for (const t of types) {
      const fn = entry.symbols.find((s) => s.kind === "function" && s.name === member && s.start >= t.start && s.end <= t.end);
      if (fn) return fn;
    }
    return null;
  }
  // a bare name means the type when one exists (a constructor shares its class's name), else the first function
  return entry.symbols.find((s) => s.kind === "type" && s.name === symbol)
    || entry.symbols.find((s) => s.kind === "function" && s.name === symbol)
    || null;
}

/**
 * Resolve a reference to a link. With the reviewed commit recorded, every reference
 * links to the file pinned at that commit, highlighting the symbol's lines (or the
 * whole file when there is no symbol or lines). The PR's "Files changed" view is not
 * used because GitHub loads most files there only after the page opens, so its line
 * anchors rarely land. Without a recorded commit, links fall back to that view: at the
 * first changed lines inside the range, else the file.
 *
 * @param {CodeRef} ref
 * @param {{pr: number|string, headSha?: string|null, codeIndex?: object}} ctx
 * @returns {ResolvedRef}
 */
export function resolveRef(ref, ctx) {
  const label = ref.label || ref.symbol || (ref.file || "").split("/").pop() || "code";
  const fail = (why) => ({ label, url: null, title: `unresolved: ${why}`, target: null });
  if (!ref.file) return fail("reference has no file");

  const found = findFile(ctx.codeIndex, ref.file);
  if (found.error) return fail(found.error);
  const path = found.path;
  const entry = ctx.codeIndex.files[path];
  const filesUrl = `${REPO}/pull/${ctx.pr}/files#${diffAnchor(path)}`;
  const blobUrl = ctx.headSha ? `${REPO}/blob/${ctx.headSha}/${path}` : null;

  let range = null;
  if (Array.isArray(ref.lines) && ref.lines.length === 2) {
    range = [Number(ref.lines[0]), Number(ref.lines[1])];
  } else if (ref.symbol) {
    const sym = findSymbol(entry, ref.symbol);
    if (!sym) return fail(`no symbol "${ref.symbol}" in ${path}`);
    range = [sym.start, sym.end];
  }
  if (!range) return { label, url: blobUrl || filesUrl, title: path, target: "file" };

  const [start, end] = range;
  const hunk = entry.hunks.find(([hs, he]) => hs <= end && he >= start);
  const changed = hunk ? `changed at ${Math.max(start, hunk[0])}` : "unchanged";
  if (blobUrl) {
    return { label, url: `${blobUrl}#L${start}-L${end}`, title: `${path}:${start}-${end} (${changed})`, target: "blob" };
  }
  if (hunk) {
    const a = Math.max(start, hunk[0]);
    const b = Math.min(end, hunk[1]);
    return { label, url: `${filesUrl}R${a}${b > a ? `-R${b}` : ""}`, title: `${path}:${a}-${b} (changed lines)`, target: "diff" };
  }
  return { label, url: filesUrl, title: `${path}:${start}-${end} (no commit recorded; linking the file)`, target: "file" };
}
