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

import { readFile } from "node:fs/promises";
import { join } from "node:path";
import gremlin from "gremlin";
import {
  listFunctions, listTypes, getCallsFrom, getCanonicalSteps,
  listDeleted, listExternalRefs, addReference,
  mapStep, setEdgeConfidence, linkDiscussion, linkDoc, addGrammarRule, annotate,
  createPrDiscussion,
} from "./api.js";
import { confidenceAudit, listInferred } from "../patterns/confidence-audit.js";
import { classifyExternals } from "../patterns/classify-externals.js";

const COMMANDS = {
  listFunctions: { fn: listFunctions, needsG: true },
  listTypes: { fn: listTypes, needsG: true },
  getCallsFrom: { fn: getCallsFrom, needsG: true },
  getCanonicalSteps: { fn: getCanonicalSteps, needsG: false },
  auditConfidence: { fn: confidenceAudit, needsG: true },
  listInferred: { fn: listInferred, needsG: true },
  listDeleted: { fn: listDeleted, needsG: true },
  listExternalRefs: { fn: listExternalRefs, needsG: true },
  classifyExternals: { fn: classifyExternals, needsG: true },
  addReference: { fn: addReference, needsG: true },
  mapStep: { fn: mapStep, needsG: true },
  setEdgeConfidence: { fn: setEdgeConfidence, needsG: true },
  linkDiscussion: { fn: linkDiscussion, needsG: true },
  linkDoc: { fn: linkDoc, needsG: true },
  addGrammarRule: { fn: addGrammarRule, needsG: true },
  annotate: { fn: annotate, needsG: true },
  createPrDiscussion: { fn: createPrDiscussion, needsG: true },
};

function parseArgs(argv) {
  const args = {};
  for (let i = 0; i < argv.length; i++) {
    if (argv[i].startsWith("--")) {
      const key = argv[i].slice(2);
      const val = argv[i + 1] && !argv[i + 1].startsWith("--") ? argv[i + 1] : "true";
      if (val === "true") args[key] = true;
      else if (val === "false") args[key] = false;
      else if (!isNaN(val)) args[key] = Number(val);
      else args[key] = val;
      if (val !== "true") i++;
    }
  }
  return args;
}

async function loadSession(workDir) {
  const sessionPath = join(workDir, "session.json");
  const raw = await readFile(sessionPath, "utf-8");
  return JSON.parse(raw);
}

async function main() {
  const [command, ...rest] = process.argv.slice(2);

  if (!command || command === "--help") {
    console.log("Usage: node cli.js <command> [--key value ...]");
    console.log("       node cli.js <command> --workDir /tmp/pr-review-3448 [--key value ...]");
    console.log("");
    console.log("Commands:");
    console.log("  listFunctions   [--changed true] [--visibility public]");
    console.log("  listTypes       [--kind class]");
    console.log("  getCallsFrom    --function <name> --file <path>");
    console.log("  getCanonicalSteps");
    console.log("  auditConfidence [--maxAmbiguous 50]");
    console.log("  listInferred    [--relation implements_step|calls|...] [--limit 100]");
    console.log("  listDeleted");
    console.log("  listExternalRefs");
    console.log("  classifyExternals   (tags external stubs origin=library|project|unresolved)");
    console.log("  addReference    --fromPath <path> --toPath <deletedPath> [--symbol <name>] [--location <L42>] [--confidence ...]");
    console.log("  mapStep         --function <name> --file <path> --step <canonicalName> [--confidence INFERRED|AMBIGUOUS|EXTRACTED]");
    console.log("  setEdgeConfidence --relation <label> --fromName <name> [--fromFile <path>] [--toName <name>] --confidence <EXTRACTED|INFERRED|AMBIGUOUS>");
    console.log("  linkDiscussion  --url <url> --source <jira|devlist|proposal> --title <title> [--body <body>] [--confidence ...]");
    console.log("  linkDoc         --entity <label> --name <name> --doc <path> [--section <section>] [--confidence ...]");
    console.log("  addGrammarRule  --name <name> [--production <production>]");
    console.log("  annotate        --label <label> --name <name> --key <key> --value <value>");
    console.log("");
    console.log("Options:");
    console.log("  --workDir       Work directory (default: env PR_REVIEW_WORKDIR or /tmp/pr-review-*)");
    process.exit(0);
  }

  if (!COMMANDS[command]) {
    console.error(`Unknown command: ${command}`);
    console.error(`Available: ${Object.keys(COMMANDS).join(", ")}`);
    process.exit(1);
  }

  const args = parseArgs(rest);
  const workDir = args.workDir || process.env.PR_REVIEW_WORKDIR;

  if (!workDir) {
    console.error("Error: --workDir or PR_REVIEW_WORKDIR env var required");
    process.exit(1);
  }

  const session = await loadSession(workDir);
  const { fn, needsG } = COMMANDS[command];

  let connection;
  let g;
  let result;

  try {
    if (needsG) {
      connection = new gremlin.driver.DriverRemoteConnection(session.url);
      g = gremlin.process.AnonymousTraversalSource.traversal().withRemote(connection);
    }

    switch (command) {
      case "listFunctions":
        result = await fn(g, { changed: args.changed, visibility: args.visibility, filePath: args.file });
        break;
      case "listTypes":
        result = await fn(g, { kind: args.kind, filePath: args.file });
        break;
      case "getCallsFrom":
        result = await fn(g, args.function, args.file);
        break;
      case "getCanonicalSteps":
        result = await fn(session.worktreePath || session.repoPath);
        break;
      case "auditConfidence":
        result = await fn(g, { maxAmbiguous: args.maxAmbiguous });
        break;
      case "listInferred":
        result = await fn(g, { relation: args.relation, limit: args.limit });
        break;
      case "listDeleted":
        result = await fn(g);
        break;
      case "listExternalRefs":
        result = await fn(g);
        break;
      case "classifyExternals":
        result = await fn(g, session.worktreePath || session.repoPath);
        break;
      case "addReference":
        result = await fn(g, {
          fromPath: args.fromPath,
          toPath: args.toPath,
          symbol: args.symbol,
          location: args.location,
          confidence: args.confidence,
        });
        break;
      case "mapStep":
        result = await fn(g, args.function, args.file, args.step, args.confidence);
        break;
      case "setEdgeConfidence":
        result = await fn(g, {
          relation: args.relation,
          fromName: args.fromName,
          fromFile: args.fromFile,
          toName: args.toName,
          fromLabel: args.fromLabel,
          confidence: args.confidence,
        });
        break;
      case "linkDiscussion":
        result = await fn(g, args.url, args.source, args.title, args.body, args.confidence);
        break;
      case "linkDoc":
        result = await fn(g, args.entity, args.name, args.doc, args.section, args.confidence);
        break;
      case "addGrammarRule":
        result = await fn(g, args.name, args.production);
        break;
      case "annotate":
        result = await fn(g, args.label, args.name, args.key, args.value);
        break;
      case "createPrDiscussion":
        result = await fn(g, args.pr, args.title, args.body);
        break;
    }

    console.log(JSON.stringify(result, null, 2));
  } finally {
    if (connection) await connection.close().catch(() => {});
  }
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
