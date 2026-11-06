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

// Verifies structural fingerprinting: the pure grade logic (gradeMember /
// gradeFile / maxLevel / atLeast) and the end-to-end grading in the tree-sitter
// extractor, which diffs each changed file's base version against head and
// stamps NONE / FORMATTING / BEHAVIORAL / STRUCTURAL.

import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { extract } from "../scripts/extraction/tree-sitter.js";
import { gradeMember, gradeFile, maxLevel, atLeast, CHANGE_LEVEL } from "../scripts/graph/change-levels.js";

// === Pure grade logic ===

test("gradeMember covers all four outcomes", () => {
  const base = { signature: "f(a)", rawHash: "r1", normHash: "n1" };
  assert.equal(gradeMember(null, base), "STRUCTURAL", "no base match = added member");
  assert.equal(gradeMember(base, { signature: "f(a)", rawHash: "r1", normHash: "n1" }), "NONE");
  assert.equal(gradeMember(base, { signature: "f(a)", rawHash: "r2", normHash: "n1" }), "FORMATTING");
  assert.equal(gradeMember(base, { signature: "f(a)", rawHash: "r2", normHash: "n2" }), "BEHAVIORAL");
  assert.equal(gradeMember(base, { signature: "f(a,b)", rawHash: "r2", normHash: "n2" }), "STRUCTURAL");
});

test("maxLevel and atLeast order NONE < FORMATTING < BEHAVIORAL < STRUCTURAL", () => {
  assert.equal(maxLevel("NONE", "FORMATTING", "BEHAVIORAL"), "BEHAVIORAL");
  assert.equal(maxLevel("NONE", "NONE"), "NONE");
  assert.equal(maxLevel(), "NONE");
  assert.ok(atLeast("STRUCTURAL", "BEHAVIORAL"));
  assert.ok(atLeast("BEHAVIORAL", "BEHAVIORAL"));
  assert.ok(!atLeast("FORMATTING", "BEHAVIORAL"));
  assert.ok(!atLeast("NONE", "FORMATTING"));
});

test("gradeFile rolls members up, treats import change as STRUCTURAL, floors trivia at FORMATTING", () => {
  assert.equal(gradeFile({ memberLevels: ["NONE", "BEHAVIORAL"], rawFileChanged: true }), "BEHAVIORAL");
  assert.equal(gradeFile({ memberLevels: ["NONE"], importExportChanged: true, rawFileChanged: true }), "STRUCTURAL");
  assert.equal(gradeFile({ memberLevels: ["NONE"], rawFileChanged: true }), "FORMATTING", "top-level trivia floor");
  assert.equal(gradeFile({ memberLevels: ["NONE"], rawFileChanged: false }), "NONE");
});

// === End-to-end grading through the extractor ===

// Parse one changed file, diffing `base` against `head`, and return the graded
// records. `head` is what lands on disk; `base` is fed as the prior version.
async function gradeFileChange(name, language, base, head) {
  const dir = await mkdtemp(join(tmpdir(), "tinker-fp-"));
  try {
    await writeFile(join(dir, name), head);
    const r = await extract(dir, language, {
      changedFiles: [name],
      baseContents: { [name]: base },
      expandHierarchy: false,
    });
    return r;
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

const JAVA_BASE =
  "package x;\npublic class Calc {\n  public int add(int a, int b) {\n    return a + b;\n  }\n}\n";

test("Java: identical content grades NONE", async () => {
  const r = await gradeFileChange("Calc.java", "java", JAVA_BASE, JAVA_BASE);
  assert.equal(r.functions.find((f) => f.name === "add").changeLevel, "NONE");
  assert.equal(r.files.find((f) => f.path === "Calc.java").changeLevel, "NONE");
});

test("Java: comment/whitespace-only edit grades FORMATTING", async () => {
  const head =
    "package x;\npublic class Calc {\n  public int add(int a, int b) {\n    // sum the two operands\n    return a  +  b;\n  }\n}\n";
  const r = await gradeFileChange("Calc.java", "java", JAVA_BASE, head);
  assert.equal(r.functions.find((f) => f.name === "add").changeLevel, "FORMATTING");
});

test("Java: body logic change grades BEHAVIORAL", async () => {
  const head =
    "package x;\npublic class Calc {\n  public int add(int a, int b) {\n    return a - b;\n  }\n}\n";
  const r = await gradeFileChange("Calc.java", "java", JAVA_BASE, head);
  assert.equal(r.functions.find((f) => f.name === "add").changeLevel, "BEHAVIORAL");
});

test("Java: signature change grades STRUCTURAL", async () => {
  const head =
    "package x;\npublic class Calc {\n  public int add(int a, int b, int c) {\n    return a + b + c;\n  }\n}\n";
  const r = await gradeFileChange("Calc.java", "java", JAVA_BASE, head);
  assert.equal(r.functions.find((f) => f.name === "add").changeLevel, "STRUCTURAL");
});

test("Java: an untouched helper in a changed file grades NONE", async () => {
  const base =
    "package x;\npublic class Calc {\n  public int add(int a, int b) {\n    return a + b;\n  }\n  public int untouched(int a) {\n    return a;\n  }\n}\n";
  const head =
    "package x;\npublic class Calc {\n  public int add(int a, int b) {\n    return a - b;\n  }\n  public int untouched(int a) {\n    return a;\n  }\n}\n";
  const r = await gradeFileChange("Calc.java", "java", base, head);
  assert.equal(r.functions.find((f) => f.name === "add").changeLevel, "BEHAVIORAL");
  assert.equal(r.functions.find((f) => f.name === "untouched").changeLevel, "NONE",
    "the helper the PR did not touch is not in the changed set");
});

// Python exercises the comment-skip on a second grammar (comment node type
// "comment") and significant indentation.
const PY_BASE = "class Calc:\n    def add(self, a, b):\n        return a + b\n";

test("Python: comment-only edit grades FORMATTING", async () => {
  const head = "class Calc:\n    def add(self, a, b):\n        # add them\n        return a + b\n";
  const r = await gradeFileChange("calc.py", "python", PY_BASE, head);
  assert.equal(r.functions.find((f) => f.name === "add").changeLevel, "FORMATTING");
});

test("Python: body change grades BEHAVIORAL", async () => {
  const head = "class Calc:\n    def add(self, a, b):\n        return a * b\n";
  const r = await gradeFileChange("calc.py", "python", PY_BASE, head);
  assert.equal(r.functions.find((f) => f.name === "add").changeLevel, "BEHAVIORAL");
});
