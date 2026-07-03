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

import { existsSync } from "node:fs";
import { join } from "node:path";
import gremlin from "gremlin";
import { CONFIDENCE } from "./confidence.js";

const { process: { statics: __ } } = gremlin;

const BATCH_SIZE = 50;

async function submitBatch(batch) {
  const results = await Promise.allSettled(batch.map((t) => t.next()));
  return results.filter((r) => r.status === "fulfilled").length;
}

/**
 * Populate TinkerGraph with extraction data.
 * Creates vertices and edges matching the PR knowledge graph schema.
 *
 * @param {object} g - gremlin-js GraphTraversalSource (already connected)
 * @param {ExtractionResult} extraction - Output from tree-sitter module
 * @param {object} [options]
 * @param {string[]} [options.changedFiles] - Full changed-file list for the PR
 *   (used to mark files the PR touched but the extractor didn't parse)
 * @param {string} [options.worktreePath] - PR-head worktree, to tell a deleted
 *   file (absent on disk) from an unparsed one (present but not a parsed language)
 * @returns {Promise<PopulationSummary>}
 */
export async function populate(g, extraction, options = {}) {
  const { changedFiles = [], worktreePath = "" } = options;
  const counts = {
    vertices: 0,
    edges: 0,
    breakdown: { files: 0, functions: 0, types: 0, tests: 0, calls: 0, defines: 0, testsEdges: 0, externalFunctions: 0, stubFiles: 0 },
  };

  for (const file of extraction.files) {
    await g.addV("File")
      .property("path", file.path)
      .property("language", file.language)
      .property("changed", file.changed)
      .next();
    counts.vertices++;
    counts.breakdown.files++;
  }

  // Mark changed files the extractor didn't parse. The PR's `modifies` edges
  // target every changed file by path, but only parsed files (right language,
  // present on disk) get a File vertex above — so a deleted or non-code file
  // would have no vertex to land on and its `modifies` edge would vanish. Create
  // a stub File as a marker (keyed by unique path, so it's race-free). A file
  // absent from the PR-head worktree was deleted by the PR; one still on disk was
  // simply not parsed (unsupported language). These markers are especially
  // meaningful on removal PRs — they record exactly what the PR took out.
  const extractedPaths = new Set(extraction.files.map((f) => f.path));
  for (const filePath of changedFiles) {
    if (extractedPaths.has(filePath)) continue;
    const onDisk = worktreePath ? existsSync(join(worktreePath, filePath)) : true;
    const ext = filePath.includes(".") ? filePath.split(".").pop() : "";
    await g.addV("File")
      .property("path", filePath)
      .property("language", ext)
      .property("changed", true)
      .property("parsed", false)
      .property("deleted", !onDisk)
      .next();
    counts.vertices++;
    counts.breakdown.stubFiles++;
  }

  for (const fn of extraction.functions) {
    await g.addV("Function")
      .property("name", fn.name)
      .property("signature", fn.signature)
      .property("visibility", fn.visibility)
      .property("filePath", fn.filePath)
      .property("lines_start", fn.linesStart)
      .property("lines_end", fn.linesEnd)
      .property("changed", fn.changed)
      .next();
    counts.vertices++;
    counts.breakdown.functions++;
  }

  for (const type of extraction.types) {
    await g.addV("Type")
      .property("name", type.name)
      .property("kind", type.kind)
      .property("visibility", type.visibility)
      .property("filePath", type.filePath)
      .next();
    counts.vertices++;
    counts.breakdown.types++;
  }

  const tests = extraction.tests || [];
  for (const test of tests) {
    await g.addV("Test")
      .property("name", test.name)
      .property("type", test.type)
      .property("filePath", test.filePath)
      .next();
    counts.vertices++;
    counts.breakdown.tests++;
  }

  let batch = [];

  // Resolve-or-mark callee vertices. Call/test edges target a function by name;
  // when the callee isn't among the extracted functions (a library/JDK call, or
  // a function in a file this PR didn't change) there is no vertex to land on
  // and the edge would silently vanish. Materialize a lightweight "external"
  // stub as a marker so the edge survives and downstream analysis (blast radius,
  // centrality) can see the call. Stubs are keyed by unique name and created
  // up front, so this is idempotent and race-free even under batched inserts.
  // These MUST be flushed before the calls/tests edges below reference them.
  const extractedFunctionNames = new Set(extraction.functions.map((f) => f.name));
  const unresolvedCallees = new Set();
  for (const call of extraction.calls) {
    if (!extractedFunctionNames.has(call.calleeName)) unresolvedCallees.add(call.calleeName);
  }
  for (const test of tests) {
    for (const calledFn of (test.calledFunctions || [])) {
      if (!extractedFunctionNames.has(calledFn)) unresolvedCallees.add(calledFn);
    }
  }

  for (const name of unresolvedCallees) {
    batch.push(
      g.addV("Function")
        .property("name", name)
        .property("external", true)
        .property("resolved", false)
        .property("changed", false)
    );
    counts.vertices++;
    counts.breakdown.externalFunctions++;

    if (batch.length >= BATCH_SIZE) {
      await submitBatch(batch);
      batch = [];
    }
  }
  if (batch.length > 0) {
    await submitBatch(batch);
    batch = [];
  }

  for (const fn of extraction.functions) {
    batch.push(
      g.V().hasLabel("File").has("path", fn.filePath)
        .addE("defines")
        .property("confidence", CONFIDENCE.EXTRACTED)
        .to(__.V().hasLabel("Function").has("name", fn.name).has("filePath", fn.filePath))
    );
    counts.edges++;
    counts.breakdown.defines++;

    if (batch.length >= BATCH_SIZE) {
      await submitBatch(batch);
      batch = [];
    }
  }

  for (const type of extraction.types) {
    batch.push(
      g.V().hasLabel("File").has("path", type.filePath)
        .addE("defines")
        .property("confidence", CONFIDENCE.EXTRACTED)
        .to(__.V().hasLabel("Type").has("name", type.name).has("filePath", type.filePath))
    );
    counts.edges++;
    counts.breakdown.defines++;

    if (batch.length >= BATCH_SIZE) {
      await submitBatch(batch);
      batch = [];
    }
  }

  for (const call of extraction.calls) {
    // INFERRED: the call site is real, but the callee is resolved by name alone
    // (it matches any Function with that name, across files/overloads), so the
    // edge target is a deduction rather than a directly observed fact.
    batch.push(
      g.V().hasLabel("Function")
        .has("name", call.callerName)
        .has("filePath", call.callerFile)
        .addE("calls")
        .property("confidence", CONFIDENCE.INFERRED)
        .to(__.V().hasLabel("Function").has("name", call.calleeName))
    );
    counts.edges++;
    counts.breakdown.calls++;

    if (batch.length >= BATCH_SIZE) {
      await submitBatch(batch);
      batch = [];
    }
  }

  for (const test of tests) {
    for (const calledFn of test.calledFunctions) {
      // INFERRED: a test is linked to a function by name match on the callee.
      batch.push(
        g.V().hasLabel("Test").has("name", test.name).has("filePath", test.filePath)
          .addE("tests")
          .property("confidence", CONFIDENCE.INFERRED)
          .to(__.V().hasLabel("Function").has("name", calledFn))
      );
      counts.edges++;
      counts.breakdown.testsEdges++;

      if (batch.length >= BATCH_SIZE) {
        await submitBatch(batch);
        batch = [];
      }
    }
  }

  // Import resolution (depends_on edges) is intentionally not implemented.
  // File-to-file connectivity is already captured through the calls/defines edges
  // (File A defines Function X which calls Function Y defined in File B). The
  // depends_on edge would only add value for type-only references (imports used
  // for signatures/fields but not method calls) which are less relevant for
  // review purposes. Revisit if cluster analysis produces false disconnections.

  if (batch.length > 0) {
    await submitBatch(batch);
  }

  // Report the true graph size. The per-type breakdown above counts attempted
  // inserts; query the graph itself for the authoritative vertex/edge totals so
  // the summary can't drift from reality (e.g. an edge whose endpoints matched
  // multiple vertices, or a vertex insert that failed).
  const realV = await g.V().count().next();
  const realE = await g.E().count().next();
  counts.vertices = Number(realV.value);
  counts.edges = Number(realE.value);

  return counts;
}
