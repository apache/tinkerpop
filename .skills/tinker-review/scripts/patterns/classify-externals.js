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
import { classifyExternalName } from "../graph/externals.js";

const exec = promisify(execFile);

// A precise, low-false-positive check: does a Java type declaration with this
// exact name exist anywhere in the repo? Uses `git grep` in the worktree (fast,
// respects the tree). Returns the first defining file, or null.
async function definedAsProjectType(name, repoPath) {
  if (!repoPath || !/^\w+$/.test(name)) return null;
  try {
    const { stdout } = await exec(
      "git",
      ["grep", "-lE", `(class|interface|enum|@interface)[[:space:]]+${name}\\b`],
      { cwd: repoPath },
    );
    const files = stdout.split("\n").filter(Boolean);
    return files[0] || null;
  } catch {
    // git grep exits non-zero when there is no match — treat as "not found".
    return null;
  }
}

/**
 * Classify every external Function stub as library / project / unresolved and
 * annotate the vertex with `origin` (and `definedIn` for project types). This
 * lets structural checks drop library noise. Run it after populate and BEFORE
 * centrality so the metric can exclude `origin: library`.
 *
 * @param {object} g - gremlin-js GraphTraversalSource (already connected)
 * @param {string} repoPath - a git worktree to grep for project type definitions
 * @returns {Promise<ExternalsResult>}
 */

/**
 * @typedef {Object} ExternalsResult
 * @property {number}   total       external-callee stubs classified
 * @property {string[]} library     names judged JDK/accessor noise (dropped from centrality out-degree)
 * @property {{name: string, definedIn: string}[]} project  names a repo type declares, with the file
 * @property {string[]} unresolved  neither a known library name nor a project type — unknown
 */
export async function classifyExternals(g, repoPath) {
  const names = await g.V().hasLabel("Function").has("external", true).values("name").toList();

  const classified = await Promise.all(names.map(async (name) => {
    const definedIn = await definedAsProjectType(name, repoPath);
    return { name, origin: classifyExternalName(name, Boolean(definedIn)), definedIn };
  }));

  const summary = { total: names.length, library: [], project: [], unresolved: [] };
  for (const { name, origin, definedIn } of classified) {
    let t = g.V().hasLabel("Function").has("external", true).has("name", name)
      .property("origin", origin);
    if (origin === "project" && definedIn) t = t.property("definedIn", definedIn);
    await t.iterate();

    if (origin === "project") summary.project.push({ name, definedIn });
    else summary[origin].push(name);
  }

  return summary;
}
