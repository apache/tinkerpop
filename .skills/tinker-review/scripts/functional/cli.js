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

// Thin CLI over functional/setup.js so SKILL.md step 4 can drive the mechanical
// build+start from one command, the way enrichment/cli.js drives the graph.
// Reads session.json from --workDir for the pr/repo, prints the server handle as
// JSON, and persists it to functional.json so `stop` can find it later.
//
//   node scripts/functional/cli.js start --workDir /tmp/pr-review-<pr>
//   node scripts/functional/cli.js stop  --workDir /tmp/pr-review-<pr>

import { readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";

import { buildAndStart, stop } from "./setup.js";

function parseArgs(argv) {
  // A leading `--flag` means no subcommand was given (e.g. `--help`).
  const command = argv[0] && !argv[0].startsWith("--") ? argv[0] : undefined;
  const opts = {};
  for (let i = command ? 1 : 0; i < argv.length; i++) {
    if (argv[i].startsWith("--")) {
      const key = argv[i].slice(2);
      const val = argv[i + 1] && !argv[i + 1].startsWith("--") ? argv[++i] : true;
      opts[key] = val;
    }
  }
  return { command, opts };
}

async function readSession(workDir) {
  return JSON.parse(await readFile(join(workDir, "session.json"), "utf-8"));
}

async function main() {
  const { command, opts } = parseArgs(process.argv.slice(2));
  const workDir = opts.workDir;

  if (!command || opts.help) {
    process.stdout.write(
      "Usage:\n" +
      "  cli.js start --workDir <dir> [--port <n>]   build the PR and start the functional server\n" +
      "  cli.js stop  --workDir <dir>                stop it and remove the build worktree\n",
    );
    return;
  }
  if (!workDir) throw new Error("--workDir is required");

  const session = await readSession(workDir);
  const handlePath = join(workDir, "functional.json");

  if (command === "start") {
    const handle = await buildAndStart(workDir, {
      pr: session.pr,
      repoPath: session.repoPath,
      port: opts.port ? Number(opts.port) : undefined,
    });
    await writeFile(handlePath, JSON.stringify(handle, null, 2));
    process.stdout.write(JSON.stringify(handle, null, 2) + "\n");
    return;
  }

  if (command === "stop") {
    const handle = JSON.parse(await readFile(handlePath, "utf-8").catch(() => "null"));
    if (handle) await stop(handle, { repoPath: session.repoPath });
    process.stdout.write(JSON.stringify({ stopped: Boolean(handle) }) + "\n");
    return;
  }

  throw new Error(`Unknown command: ${command}`);
}

main().catch((err) => {
  process.stderr.write(`${err.message}\n`);
  process.exit(1);
});
