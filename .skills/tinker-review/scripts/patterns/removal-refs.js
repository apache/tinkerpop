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

import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { CONFIDENCE } from "../graph/confidence.js";
import { symbolFromPath, isCodeFile, createReferenceEdge } from "../graph/references.js";

const exec = promisify(execFile);

// Whole-word search for a symbol across the PR-head worktree. Returns one hit
// per file (first line), since deleted files no longer exist in the worktree the
// results are surviving references only. `git grep` exits non-zero on no match.
async function gitGrepWord(symbol, repoPath) {
  try {
    const { stdout } = await exec("git", ["grep", "-nw", symbol], {
      cwd: repoPath,
      maxBuffer: 16 * 1024 * 1024,
    });
    const firstLineByFile = new Map();
    for (const line of stdout.split("\n")) {
      const m = line.match(/^([^:]+):(\d+):/);
      if (m && !firstLineByFile.has(m[1])) firstLineByFile.set(m[1], m[2]);
    }
    return [...firstLineByFile.entries()].map(([file, ln]) => ({ file, line: `L${ln}` }));
  } catch {
    return [];
  }
}

/**
 * Phase-1 removal-impact pass. Mechanical mirror of classifyExternals: for every
 * deleted *code* file, grep the surviving worktree for its symbol and auto-create
 * a `references` edge (INFERRED) for each hit. Finding the references is
 * reproducible; classifying them (blocking vs. benign historical note) is the
 * agent's judgment in the report. Config/non-code symbols are skipped here and
 * left to the addReference escape hatch.
 *
 * @param {object} g - gremlin-js GraphTraversalSource (already connected)
 * @param {string} repoPath - a git worktree (PR-head) to grep
 * @returns {Promise<RemovalRefsResult>}
 */

/**
 * @typedef {Object} RemovalReference
 * @property {string} from      surviving file that still names the removed symbol
 * @property {string} to        the deleted file path
 * @property {string} symbol    the removed symbol matched
 * @property {string} location  first line in `from` (e.g. "L42")
 *
 * @typedef {Object} RemovalRefsResult
 * @property {string[]}          deletedCodeSymbols  symbols of deleted code files that were searched
 * @property {RemovalReference[]} references         surviving references found (also written as edges)
 * @property {string[]}          externalCallers     external-callee stubs whose name matches a deleted
 *                                                    symbol — changed code still calling removed code
 * @property {number}            total               references.length
 */
export async function findRemovalRefs(g, repoPath) {
  const deletedPaths = await g.V().hasLabel("File").has("deleted", true).values("path").toList();
  const result = { deletedCodeSymbols: [], references: [], externalCallers: [], total: 0 };
  if (deletedPaths.length === 0) return result;

  const deletedSymbolSet = new Set(deletedPaths.map(symbolFromPath));

  for (const path of deletedPaths) {
    if (!isCodeFile(path)) continue;
    const symbol = symbolFromPath(path);
    if (!/^\w+$/.test(symbol)) continue;
    result.deletedCodeSymbols.push(symbol);

    for (const { file, line } of await gitGrepWord(symbol, repoPath)) {
      await createReferenceEdge(g, {
        fromPath: file,
        toPath: path,
        symbol,
        location: line,
        confidence: CONFIDENCE.INFERRED,
      });
      result.references.push({ from: file, to: path, symbol, location: line });
    }
  }

  // In-graph dangling references: a changed function still calls a name that a
  // deleted file defined (surfaced from the external-callee stubs, no grep).
  const externalStubs = await g.V().hasLabel("Function").has("external", true).values("name").toList();
  result.externalCallers = externalStubs.filter((n) => deletedSymbolSet.has(n));

  result.total = result.references.length;
  return result;
}
