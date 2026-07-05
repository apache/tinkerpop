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

import { extractKeywords } from "../scripts/review.js";
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
