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

// Structural conformance guard for the playbooks. Every playbook must carry the
// same six sections in the same order, split by data flow:
//
//   Context   — applicability gate
//   Enrich    — graph mutation only; names real enrichment CLI commands
//   Inspect   — reads changed source into findings (no graph representation)
//   Verify    — functional-test battery + the gate deciding whether to run it
//   Interpret — weighs evidence.json checks (+ Inspect/Verify notes) into the report
//   Escape    — stop/escalate gates
//
// It also fails if an Enrich section names no registered command (and doesn't
// explicitly declare that none applies) — the drift that let playbooks gesture
// at "link it as a discussion" without ever naming `linkDiscussion`.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFile, readdir } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

import { COMMANDS } from "../scripts/enrichment/cli.js";

const here = dirname(fileURLToPath(import.meta.url));
const playbooksDir = join(here, "..", "playbooks");

const CANONICAL = ["Context", "Enrich", "Inspect", "Verify", "Interpret", "Escape"];

// An Enrich section that legitimately has no domain-specific graph write must
// say so with this phrase rather than silently naming nothing.
const NO_ENRICHMENT = /\bnone\b.*\bappl/i;

const agentCommands = Object.keys(COMMANDS).filter(
  (c) => COMMANDS[c].facing === "agent",
);

async function playbookFiles() {
  const entries = await readdir(playbooksDir);
  return entries.filter((f) => f.endsWith(".md"));
}

/** The level-2 headers of a markdown doc, in document order. */
function sectionHeaders(md) {
  return [...md.matchAll(/^##\s+(.+?)\s*$/gm)].map((m) => m[1]);
}

/** The body of one `## <name>` section, up to the next `## ` header or EOF. */
function sectionBody(md, name) {
  const headers = [...md.matchAll(/^##\s+(.+?)\s*$/gm)];
  const idx = headers.findIndex((h) => h[1] === name);
  if (idx === -1) return "";
  const start = headers[idx].index + headers[idx][0].length;
  const end = idx + 1 < headers.length ? headers[idx + 1].index : md.length;
  return md.slice(start, end);
}

test("every playbook carries the six canonical sections in order", async () => {
  for (const file of await playbookFiles()) {
    const md = await readFile(join(playbooksDir, file), "utf-8");
    const headers = sectionHeaders(md);
    assert.deepEqual(
      headers,
      CANONICAL,
      `${file} must have exactly these level-2 sections in order: ${CANONICAL.join(", ")} (found: ${headers.join(", ") || "none"})`,
    );
  }
});

test("every Enrich section names a registered command or declares none applies", async () => {
  for (const file of await playbookFiles()) {
    const md = await readFile(join(playbooksDir, file), "utf-8");
    const enrich = sectionBody(md, "Enrich");
    assert.ok(enrich.trim(), `${file} has an empty Enrich section`);

    const named = agentCommands.filter((c) =>
      new RegExp(`\\b${c}\\b`).test(enrich),
    );
    assert.ok(
      named.length > 0 || NO_ENRICHMENT.test(enrich),
      `${file} Enrich names no registered enrichment command and doesn't state that none applies`,
    );
  }
});
