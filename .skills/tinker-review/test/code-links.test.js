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

import { test } from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";

import { parseHunks, buildCodeIndex } from "../scripts/extraction/code-index.js";
import { resolveRef, diffAnchor } from "../scripts/renderer/code-links.js";
import { render } from "../scripts/renderer/render.js";

const TX = "tinkergraph-gremlin/src/main/java/org/x/TinkerTransaction.java";
const GRAPH = "tinkergraph-gremlin/src/main/java/org/x/TinkerStorageGraph.java";
const OTHER_GRAPH = "gremlin-core/src/main/java/org/y/TinkerStorageGraph.java";

const DIFF = [
  `diff --git a/${TX} b/${TX}`,
  `--- a/${TX}`,
  `+++ b/${TX}`,
  "@@ -174,3 +181,40 @@ class X {",
  "@@ -300 +340 @@",
  "@@ -400,5 +379,0 @@",
  `diff --git a/${GRAPH} b/${GRAPH}`,
  "--- /dev/null",
  `+++ b/${GRAPH}`,
  "@@ -0,0 +1,120 @@",
  "diff --git a/gone.java b/gone.java",
  "--- a/gone.java",
  "+++ /dev/null",
  "@@ -1,10 +0,0 @@",
].join("\n");

const extraction = {
  types: [
    { name: "TinkerTransaction", filePath: TX, linesStart: 30, linesEnd: 400 },
    { name: "TinkerStorageGraph", filePath: GRAPH, linesStart: 10, linesEnd: 120 },
    { name: "Neighbor", filePath: "not/changed/Neighbor.java", linesStart: 1, linesEnd: 9 },
  ],
  functions: [
    { name: "doCommit", filePath: TX, linesStart: 147, linesEnd: 245 },
    { name: "doRollback", filePath: TX, linesStart: 290, linesEnd: 320 },
    { name: "TinkerStorageGraph", filePath: GRAPH, linesStart: 40, linesEnd: 60 },
  ],
};

const codeIndex = buildCodeIndex({ changedFiles: [TX, GRAPH, "gone.java"], extraction, diffText: DIFF });
const ctx = { pr: 3639, headSha: "abc123", codeIndex };

test("parseHunks keeps new-side ranges, marks pure deletions at the line they follow, and drops deleted files", () => {
  const h = parseHunks(DIFF);
  assert.deepEqual(h[TX], [[181, 220], [340, 340], [379, 379]]);
  assert.deepEqual(h[GRAPH], [[1, 120]]);
  assert.equal(h["gone.java"], undefined);
});

test("buildCodeIndex indexes only changed files, with sorted symbols", () => {
  assert.deepEqual(Object.keys(codeIndex.files).sort(), [GRAPH, TX].sort());
  assert.deepEqual(codeIndex.files[TX].symbols.map((s) => s.name), ["TinkerTransaction", "doCommit", "doRollback"]);
  assert.equal(codeIndex.files[TX].symbols[0].kind, "type");
});

test("diffAnchor matches GitHub's sha256-of-path anchor", () => {
  assert.equal(diffAnchor(TX), "diff-" + createHash("sha256").update(TX).digest("hex"));
});

test("a symbol links to the file pinned at the reviewed commit with its lines highlighted", () => {
  const r = resolveRef({ file: "TinkerTransaction.java", symbol: "doCommit" }, ctx);
  assert.equal(r.target, "blob");
  assert.equal(r.url, `https://github.com/apache/tinkerpop/blob/abc123/${TX}#L147-L245`);
  assert.equal(r.label, "doCommit");
  assert.match(r.title, /changed at 181/);
  const unchanged = resolveRef({ file: TX, symbol: "doRollback", label: "rollback" }, ctx);
  assert.equal(unchanged.url, `https://github.com/apache/tinkerpop/blob/abc123/${TX}#L290-L320`);
  assert.equal(unchanged.label, "rollback");
  assert.match(unchanged.title, /unchanged/);
});

test("without a recorded head commit links fall back to the PR diff", () => {
  const noSha = { ...ctx, headSha: null };
  const changed = resolveRef({ file: TX, symbol: "doCommit" }, noSha);
  assert.equal(changed.target, "diff");
  assert.equal(changed.url, `https://github.com/apache/tinkerpop/pull/3639/files#${diffAnchor(TX)}R181-R220`);
  const unchanged = resolveRef({ file: TX, symbol: "doRollback" }, noSha);
  assert.equal(unchanged.target, "file");
  assert.equal(unchanged.url, `https://github.com/apache/tinkerpop/pull/3639/files#${diffAnchor(TX)}`);
});

test("a bare name means the type; Type.member picks the member inside it", () => {
  const type = resolveRef({ file: GRAPH, symbol: "TinkerStorageGraph" }, ctx);
  assert.match(type.url, /#L10-L120$/);
  const ctor = resolveRef({ file: GRAPH, symbol: "TinkerStorageGraph.TinkerStorageGraph" }, ctx);
  assert.match(ctor.url, /#L40-L60$/);
});

test("a symbol whose only change is a removal links into the diff at the removal when no commit is recorded", () => {
  const index = buildCodeIndex({
    changedFiles: ["A.java"],
    extraction: { types: [], functions: [{ name: "ctor", filePath: "A.java", linesStart: 89, linesEnd: 107 }] },
    diffText: "+++ b/A.java\n@@ -96,9 +95,0 @@",
  });
  const r = resolveRef({ file: "A.java", symbol: "ctor" }, { pr: 1, headSha: null, codeIndex: index });
  assert.equal(r.target, "diff");
  assert.match(r.url, /R95$/);
});

test("explicit lines and file-only references resolve", () => {
  assert.match(resolveRef({ file: TX, lines: [340, 345] }, ctx).url, /#L340-L345$/);
  const whole = resolveRef({ file: GRAPH }, ctx);
  assert.equal(whole.target, "file");
  assert.equal(whole.url, `https://github.com/apache/tinkerpop/blob/abc123/${GRAPH}`);
  assert.equal(whole.label, "TinkerStorageGraph.java");
});

test("unknown, ambiguous and symbol-less references come back unresolved with a reason", () => {
  assert.match(resolveRef({ file: "Nope.java" }, ctx).title, /no changed file/);
  assert.match(resolveRef({ file: TX, symbol: "missing" }, ctx).title, /no symbol "missing"/);
  const ambiguous = { pr: 1, codeIndex: { files: { [GRAPH]: { hunks: [], symbols: [] }, [OTHER_GRAPH]: { hunks: [], symbols: [] } } } };
  const r = resolveRef({ file: "TinkerStorageGraph.java" }, ambiguous);
  assert.equal(r.url, null);
  assert.match(r.title, /matches 2 changed files/);
});

test("the guided walk renders questions with chips and reports unresolved refs", () => {
  const warnings = [];
  const html = render({
    meta: { pr: 3639, headSha: "abc123", title: "t", domains: [], timestamp: "now" },
    codeIndex,
    guidedWalk: [
      {
        title: "Commit path", badge: "attention", badgeText: "defect",
        intro: "<p>What a commit does now.</p>",
        questions: [{ text: "Is the index restored?", refs: [{ file: TX, symbol: "doCommit" }, { file: TX, symbol: "ghost" }] }],
        refs: [{ file: GRAPH }],
      },
      { title: "Legacy", badge: "info", body: "<p>Old-style body still renders.</p>" },
    ],
  }, { warnings });
  assert.match(html, /<ul class="walk-questions">/);
  assert.match(html, /class="code-ref code-ref-blob" href="https:\/\/github\.com\/apache\/tinkerpop\/blob\/abc123\/[^"]*TinkerTransaction\.java#L147-L245"/);
  assert.match(html, /class="code-ref code-ref-unresolved"[^>]*>ghost</);
  assert.match(html, /class="code-refs walk-refs"/);
  assert.match(html, /Old-style body still renders/);
  assert.equal(warnings.length, 1);
  assert.match(warnings[0], /guidedWalk\[0\]\.questions\[0\]: unresolved: no symbol "ghost"/);
});
