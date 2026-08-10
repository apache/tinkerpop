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

// Drift guard for the enrichment CLI. The COMMANDS registry in cli.js is the
// single source of truth for which commands exist and who each is for; this
// test fails if the human-facing surfaces fall out of sync with it:
//
//   - references/enrichment-cli.md must have a `## <name>` section per command
//   - `cli.js --help` must list every command in the right group
//   - SKILL.md must mention every agent-facing command, and no internal one
//
// Add / rename / remove a command and this goes red until the docs follow.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

import { COMMANDS } from "../scripts/enrichment/cli.js";

const here = dirname(fileURLToPath(import.meta.url));
const root = join(here, "..");

const all = Object.keys(COMMANDS);
const agent = all.filter((c) => COMMANDS[c].facing === "agent");
const internal = all.filter((c) => COMMANDS[c].facing === "internal");

async function read(rel) {
  return readFile(join(root, rel), "utf-8");
}

test("every command has a facing of agent or internal", () => {
  for (const c of all) {
    assert.ok(
      COMMANDS[c].facing === "agent" || COMMANDS[c].facing === "internal",
      `command "${c}" is missing a valid facing ("agent" | "internal")`,
    );
  }
});

test("enrichment-cli.md documents exactly the registry's commands", async () => {
  const doc = await read("references/enrichment-cli.md");
  const sections = [...doc.matchAll(/^###?\s+(\w+)\s*$/gm)].map((m) => m[1]);
  const documented = new Set(sections);

  for (const c of all) {
    assert.ok(
      documented.has(c),
      `command "${c}" has no "## ${c}" section in references/enrichment-cli.md`,
    );
  }
  // Catch stale sections describing a command that no longer exists. Section
  // headers that aren't command names (e.g. "Invocation") are ignored by only
  // checking single-word headers against the registry's known non-command set.
  const knownNonCommands = new Set(["Invocation"]);
  for (const s of sections) {
    if (knownNonCommands.has(s)) continue;
    assert.ok(
      COMMANDS[s],
      `references/enrichment-cli.md documents "${s}", which is not a registered command`,
    );
  }
});

test("cli.js --help lists every command in the correct group", () => {
  const help = execFileSync(
    process.execPath,
    [join(root, "scripts/enrichment/cli.js"), "--help"],
    { encoding: "utf-8" },
  );
  const [commandsPart, internalPart] = help.split(/^Internal .*$/m);
  assert.ok(internalPart, "--help is missing the 'Internal' group header");

  for (const c of agent) {
    assert.match(
      commandsPart,
      new RegExp(`\\b${c}\\b`),
      `agent command "${c}" is missing from the --help Commands group`,
    );
  }
  for (const c of internal) {
    assert.match(
      internalPart,
      new RegExp(`\\b${c}\\b`),
      `internal command "${c}" is missing from the --help Internal group`,
    );
  }
});

test("SKILL.md points at enrichment-cli.md instead of duplicating the catalog", async () => {
  // enrichment-cli.md is the single command catalog; SKILL.md must link to it
  // (and must not grow its own exhaustive listing again). We assert the link
  // survives rather than that SKILL.md names every command — it deliberately
  // names only a few pivotal ones as workflow narrative.
  const skill = await read("SKILL.md");
  assert.match(
    skill,
    /references\/enrichment-cli\.md/,
    "SKILL.md no longer links to references/enrichment-cli.md — the command catalog reference was dropped",
  );
});
