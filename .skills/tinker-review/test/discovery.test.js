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

// Precision guards for discussion discovery — the keyword extraction and the
// proposal matcher that over-linked on ASF license boilerplate (PR #3502).

import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, writeFile, mkdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { extractKeywords, proposalKeywords } from "../scripts/review.js";
import { findMatchingProposals } from "../scripts/discovery/discussions.js";

const ASF_HEADER = `////
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information.
////
`;

test("extractKeywords drops structural filenames and keeps the title topic", () => {
  const kws = extractKeywords(
    ["NOTICE", "gremlin-server/src/main/java/.../Krb5Authenticator.java", "pom.xml"],
    "Remove Kerberos support",
  );
  assert.ok(kws.includes("kerberos"), "title topic retained");
  assert.ok(!kws.includes("notice"), "NOTICE excluded (structural)");
  assert.ok(!kws.includes("remove") && !kws.includes("support"), "generic verbs dropped");
  assert.ok(kws.includes("krb5"), "distinctive identifier token kept");
  assert.equal(kws[0], "kerberos", "title topic leads so it can't be truncated");
});

async function withProposals(files, fn) {
  const dir = await mkdtemp(join(tmpdir(), "tinker-prop-"));
  const pdir = join(dir, "docs/src/dev/future");
  await mkdir(pdir, { recursive: true });
  try {
    for (const [name, body] of Object.entries(files)) await writeFile(join(pdir, name), body);
    return await fn(dir);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

test("a keyword that only appears in the ASF license header does not match (the #3502 bug)", async () => {
  await withProposals(
    { "proposal-equality.asciidoc": `${ASF_HEADER}= Equality Semantics\n\nNothing about auth here.\n` },
    async (dir) => {
      const matched = await findMatchingProposals(dir, ["notice"]);
      assert.equal(matched.length, 0, "NOTICE in the license header must not link the proposal");
    },
  );
});

test("the proposals index (index.asciidoc) is excluded even on a title match", async () => {
  await withProposals(
    { "index.asciidoc": `${ASF_HEADER}= TinkerPop Future\n\n== Gremlin Console\n\nLinks to everything.\n` },
    async (dir) => {
      const matched = await findMatchingProposals(dir, ["console"]);
      assert.equal(matched.length, 0, "the TOC index page must not be treated as a proposal");
    },
  );
});

test("a title keyword matches at INFERRED strength; a lone body mention does not", async () => {
  await withProposals(
    {
      "proposal-asbool.asciidoc": `${ASF_HEADER}= asBool() Step\n\nDefines the asBool step.\n`,
      "proposal-other.asciidoc": `${ASF_HEADER}= Transactions\n\nOne passing mention of asbool somewhere.\n`,
    },
    async (dir) => {
      const matched = await findMatchingProposals(dir, ["asbool"]);
      const titles = matched.map((m) => m.title);
      assert.ok(titles.includes("asBool() Step"), "title match kept");
      assert.equal(matched.find((m) => m.title === "asBool() Step").matchedIn, "title");
      assert.ok(!titles.includes("Transactions"), "single body mention below threshold is dropped");
    },
  );
});

test("extractKeywords drops words that only name where the change is (module and package dirs)", () => {
  const kws = extractKeywords(
    [
      "tinkergraph-gremlin/src/main/java/org/apache/tinkerpop/gremlin/tinkergraph/structure/storage/GraphBinaryStorage.java",
      "tinkergraph-gremlin/src/main/java/org/apache/tinkerpop/gremlin/tinkergraph/structure/TinkerStorageGraph.java",
    ],
    "TINKERPOP-3282 TinkerStorageGraph",
  );
  assert.ok(!kws.includes("tinkergraph"), "module name excluded");
  assert.ok(!kws.includes("storage"), "package name excluded");
  assert.ok(kws.includes("tinkerstoragegraph"), "title topic kept");
  assert.ok(kws.includes("binary"), "a basename token that is not a location is kept");
});

test("extractKeywords keeps a PR-title word even when it also names a changed directory", () => {
  const kws = extractKeywords(
    ["tinkergraph-gremlin/src/main/java/org/apache/tinkerpop/gremlin/tinkergraph/structure/TinkerTransaction.java"],
    "Fix TinkerGraph commit ordering",
  );
  assert.ok(kws.includes("tinkergraph"), "the author's own topic word survives the location filter");
});

// The changed files of PR 3639 that drove its keywords, less TinkerGraph.java so
// that tinkergraph stays a module name rather than a changed class.
const PR_3639_FILES = [
  "THREAT_MODEL.md",
  "docs/src/reference/implementations-tinkergraph.asciidoc",
  "docs/src/upgrade/release-4.x.x.asciidoc",
  "gremlin-server/conf/tinkergraph-credentials.properties",
  "gremlin-server/src/main/java/org/apache/tinkerpop/gremlin/server/auth/SimpleAuthenticator.java",
  "tinkergraph-gremlin/src/main/java/org/apache/tinkerpop/gremlin/tinkergraph/structure/TinkerStorageGraph.java",
  "tinkergraph-gremlin/src/main/java/org/apache/tinkerpop/gremlin/tinkergraph/structure/storage/GraphBinaryStorage.java",
];
const PR_3639_TITLE = "TINKERPOP-3282 TinkerStorageGraph";

test("proposalKeywords grades identifiers strong, plain words weak and module names as location", () => {
  const { strong, weak, location } = proposalKeywords(PR_3639_FILES, PR_3639_TITLE);
  for (const k of ["tinkerstoragegraph", "simpleauthenticator", "graphbinarystorage"]) assert.ok(strong.includes(k), `${k} is strong`);
  for (const k of ["threat", "model", "implementations", "simple"]) assert.ok(weak.includes(k), `${k} is weak`);
  assert.ok(location.includes("tinkergraph"), "the module name is a location keyword");
  for (const k of ["storage", "structure", "auth", "docs", "reference"]) assert.ok(!location.includes(k), `${k} is not a module name`);
  assert.ok(!strong.includes("tinkerpop"), "the JIRA prefix is not a keyword");
});

test("proposalKeywords keeps a module named by a plain title word as a location keyword", () => {
  const { location, weak } = proposalKeywords(
    ["tinkergraph-gremlin/src/main/java/org/apache/tinkerpop/gremlin/tinkergraph/structure/TinkerTransaction.java"],
    "Fix tinkergraph commit ordering",
  );
  assert.ok(location.includes("tinkergraph") && !weak.includes("tinkergraph"));
});

test("proposalKeywords makes a step written as a call in the title strong, and plain title words weak", () => {
  const { strong, weak } = proposalKeywords([], "Clarify local() side-effect state semantics");
  assert.deepEqual(strong, ["local"]);
  assert.ok(weak.includes("semantics") && weak.includes("clarify"));
});

test("PR 3639: dictionary words split from file names link nothing; the module name finds the transaction proposal", async () => {
  await withProposals(
    {
      // Real proposals start at ==, since they are included into the future index.
      "proposal-scoping-5.asciidoc": `${ASF_HEADER}== Lazy vs. Eager Evaluation in TP4 ==\n\nA simple model that implementations may follow.\n`,
      "proposal-type-predicate-8.asciidoc": `${ASF_HEADER}== Type Predicate\n\n=== Motivation\n\nSimple for implementations.\n`,
      "proposal-transaction-4.asciidoc": `${ASF_HEADER}== TinkerGraph Transaction Support\n\nTransactions for the in-memory graph.\n`,
      "proposal-equality-1.asciidoc": `${ASF_HEADER}= Equality Semantics\n\n== Implementations\n\nTinkerGraph storage behaves the same.\n`,
    },
    async (dir) => {
      const matched = await findMatchingProposals(dir, proposalKeywords(PR_3639_FILES, PR_3639_TITLE));
      assert.deepEqual(matched.map((m) => m.file), ["proposal-transaction-4.asciidoc"]);
      assert.equal(matched[0].matchedIn, "location", "a module-name match is graded AMBIGUOUS");
      assert.equal(matched[0].title, "TinkerGraph Transaction Support", "a title at == is still the title");
    },
  );
});

test("a body-only match needs two distinct strong keywords, and weak keywords never make one", async () => {
  await withProposals(
    {
      "proposal-two.asciidoc": `${ASF_HEADER}== Two\n\nMentions FooStep and BarStep.\n`,
      "proposal-one.asciidoc": `${ASF_HEADER}== One\n\nMentions FooStep only.\n`,
      "proposal-weak.asciidoc": `${ASF_HEADER}== Weak\n\n=== Model\n\nA simple model.\n`,
    },
    async (dir) => {
      const matched = await findMatchingProposals(dir, { strong: ["foostep", "barstep"], weak: ["model", "simple"], location: [] });
      assert.deepEqual(matched.map((m) => [m.file, m.matchedIn]), [["proposal-two.asciidoc", "body"]]);
    },
  );
});

test("a module name on a section heading, not the title, does not match", async () => {
  await withProposals(
    { "proposal-x.asciidoc": `${ASF_HEADER}== Removing Closures\n\n=== The Gremlin Language\n\nText.\n` },
    async (dir) => {
      assert.equal((await findMatchingProposals(dir, { strong: [], weak: [], location: ["language"] })).length, 0);
    },
  );
});

test("a proposal starting at == takes that heading as its title and never matches its license header", async () => {
  await withProposals(
    { "proposal-scoping.asciidoc": `${ASF_HEADER}== Lazy vs. Eager Evaluation ==\n\nBody.\n` },
    async (dir) => {
      const [m] = await findMatchingProposals(dir, ["eager"]);
      assert.equal(m.title, "Lazy vs. Eager Evaluation");
      assert.ok(!m.snippet.includes("Licensed"), "the snippet starts at the title");
      assert.equal((await findMatchingProposals(dir, ["notice"])).length, 0);
    },
  );
});
