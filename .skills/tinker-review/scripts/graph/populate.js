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

import gremlin from "gremlin";

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
 * @returns {Promise<PopulationSummary>}
 */
export async function populate(g, extraction) {
  const counts = {
    vertices: 0,
    edges: 0,
    breakdown: { files: 0, functions: 0, types: 0, tests: 0, calls: 0, defines: 0, testsEdges: 0 },
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

  for (const fn of extraction.functions) {
    batch.push(
      g.V().hasLabel("File").has("path", fn.filePath)
        .addE("defines")
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
    batch.push(
      g.V().hasLabel("Function")
        .has("name", call.callerName)
        .has("filePath", call.callerFile)
        .addE("calls")
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
      batch.push(
        g.V().hasLabel("Test").has("name", test.name).has("filePath", test.filePath)
          .addE("tests")
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

  return counts;
}
