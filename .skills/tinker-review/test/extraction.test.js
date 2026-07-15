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

// Verifies the tree-sitter extractor captures the type hierarchy: supertypes
// (split into extends/implements for Java) and the declares (Type -> method)
// membership relation that override derivation and type-seeded blast radius
// depend on.

import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { extract, extractMulti } from "../scripts/extraction/tree-sitter.js";

async function withJavaSources(files, fn) {
  const dir = await mkdtemp(join(tmpdir(), "tinker-extract-"));
  try {
    const changed = [];
    for (const [name, src] of Object.entries(files)) {
      await writeFile(join(dir, name), src);
      changed.push(name);
    }
    return await fn(await extract(dir, "java", { changedFiles: changed }));
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

test("Java supertypes are split into extends and implements", async () => {
  await withJavaSources(
    {
      "MapStep.java":
        "package x;\npublic class MapStep extends AbstractStep implements Cloneable, Traversal {\n  public Object next() { return null; }\n}\n",
    },
    (r) => {
      const t = r.types.find((t) => t.name === "MapStep");
      assert.ok(t, "MapStep type extracted");
      const supers = t.supertypes.reduce((m, s) => ((m[s.name] = s.relation), m), {});
      assert.equal(supers.AbstractStep, "extends");
      assert.equal(supers.Cloneable, "implements");
      assert.equal(supers.Traversal, "implements");
    },
  );
});

test("an interface extending an interface uses the extends relation", async () => {
  await withJavaSources(
    { "Named.java": "package x;\ninterface Named extends Traversal { String name(); }\n" },
    (r) => {
      const t = r.types.find((t) => t.name === "Named");
      assert.deepEqual(t.supertypes, [{ name: "Traversal", relation: "extends" }]);
    },
  );
});

test("declares maps each method to its enclosing type", async () => {
  await withJavaSources(
    {
      "AbstractStep.java":
        "package x;\nabstract class AbstractStep implements Traversal {\n  public Object next() { return null; }\n  protected void reset() {}\n}\n",
    },
    (r) => {
      const declared = r.declares
        .filter((d) => d.typeName === "AbstractStep")
        .map((d) => d.functionName)
        .sort();
      assert.deepEqual(declared, ["next", "reset"]);
    },
  );
});

test("a changed file with no base version grades its members STRUCTURAL (added file)", async () => {
  await withJavaSources(
    { "Traversal.java": "package x;\npublic interface Traversal { Object next(); }\n" },
    (r) => {
      assert.equal(r.types.find((t) => t.name === "Traversal").changeLevel, "STRUCTURAL");
    },
  );
});

// Hierarchy-neighborhood expansion (n6r): in changed-files mode the extractor
// pulls in the type-hierarchy neighborhood as context so override/hierarchy
// edges don't undercount.
const HIERARCHY = {
  "Traversal.java": "package x;\npublic interface Traversal { Object next(); }\n",
  "AbstractStep.java":
    "package x;\nabstract class AbstractStep implements Traversal {\n  public Object next() { return null; }\n}\n",
  "MapStep.java":
    "package x;\npublic class MapStep extends AbstractStep {\n  public Object next() { return null; }\n}\n",
  "Unrelated.java": "package x;\npublic class Unrelated { void foo() {} }\n",
};

async function withChanged(files, changedFiles, fn) {
  const dir = await mkdtemp(join(tmpdir(), "tinker-hier-"));
  try {
    for (const [name, src] of Object.entries(files)) await writeFile(join(dir, name), src);
    return await fn(await extract(dir, "java", { changedFiles }));
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

test("changing only an interface pulls its implementers in as context (downward)", async () => {
  await withChanged(HIERARCHY, ["Traversal.java"], (r) => {
    const byPath = r.files.reduce((m, f) => ((m[f.path] = f.changeLevel), m), {});
    assert.equal(byPath["Traversal.java"], "STRUCTURAL");
    assert.equal(byPath["AbstractStep.java"], "NONE", "direct implementer pulled in as context");
    assert.equal(byPath["MapStep.java"], "NONE", "transitive subtype pulled in as context");
    assert.equal(byPath["Unrelated.java"], undefined, "unrelated type not pulled in");
  });
});

test("changing only a subclass pulls its ancestors in as context (upward)", async () => {
  await withChanged(HIERARCHY, ["MapStep.java"], (r) => {
    const byPath = r.files.reduce((m, f) => ((m[f.path] = f.changeLevel), m), {});
    assert.equal(byPath["MapStep.java"], "STRUCTURAL");
    assert.equal(byPath["AbstractStep.java"], "NONE", "parent pulled in as context");
    assert.equal(byPath["Traversal.java"], "NONE", "transitive ancestor pulled in as context");
    assert.equal(byPath["Unrelated.java"], undefined, "unrelated type not pulled in");
  });
});

// Multi-language extraction (P1): every language a PR touches lands in one
// merged result, and every record is stamped with its language so population can
// keep by-name edges from crossing languages.
test("extractMulti merges every language and stamps records with language", async () => {
  const dir = await mkdtemp(join(tmpdir(), "tinker-multi-"));
  try {
    await writeFile(join(dir, "Svc.java"), "package x;\npublic class Svc { public void foo() {} }\n");
    await writeFile(join(dir, "svc.py"), "class Svc:\n    def foo(self):\n        pass\n");
    const r = await extractMulti(dir, ["java", "python"], { changedFiles: ["Svc.java", "svc.py"] });

    assert.deepEqual([...r.languages].sort(), ["java", "python"]);
    const langs = new Set(r.functions.map((f) => f.language));
    assert.ok(langs.has("java") && langs.has("python"), "functions from both languages present");
    assert.equal(r.types.find((t) => t.language === "java").name, "Svc");
    assert.equal(r.types.find((t) => t.language === "python").name, "Svc");
    // Every record carries a language (no undefined leaked through the merge).
    assert.ok(r.functions.every((f) => f.language) && r.types.every((t) => t.language));
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("expandHierarchy:false keeps extraction to changed files only", async () => {
  const dir = await mkdtemp(join(tmpdir(), "tinker-hier-"));
  try {
    for (const [name, src] of Object.entries(HIERARCHY)) await writeFile(join(dir, name), src);
    const r = await extract(dir, "java", { changedFiles: ["Traversal.java"], expandHierarchy: false });
    assert.deepEqual(r.files.map((f) => f.path), ["Traversal.java"]);
    assert.equal(r.hierarchyNeighborhood, undefined);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});
