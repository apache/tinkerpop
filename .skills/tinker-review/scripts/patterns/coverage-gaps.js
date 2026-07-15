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
import { changedMeaningful } from "../graph/change-levels.js";

/**
 * Find changed functions that have no incoming 'tests' edge.
 *
 * @param {object} g - gremlin-js GraphTraversalSource
 * @param {object} params
 * @param {boolean} [params.changedOnly] - Only check meaningfully-changed functions
 *   — BEHAVIORAL or STRUCTURAL (default: true)
 * @returns {Promise<CoverageGapResult>}
 */

/**
 * @typedef {Object} UncoveredFunction
 * @property {string} name
 * @property {string} signature
 * @property {string} filePath
 * @property {number} linesStart
 * @property {number} linesEnd
 *
 * @typedef {Object} CoverageGapResult
 * @property {UncoveredFunction[]} uncovered     changed functions with no incoming `tests` edge
 * @property {number}              totalChanged  changed functions considered
 * @property {number}              totalCovered  changed functions that do have a test
 */
export async function coverageGaps(g, params = {}) {
  const changedOnly = params.changedOnly !== false;

  let traversal = g.V().hasLabel("Function");
  if (changedOnly) {
    // A formatting-only change does not create a coverage gap; a real body or
    // signature change without a test does.
    traversal = traversal.has("changeLevel", changedMeaningful());
  }

  const functions = await traversal.elementMap().toList();

  const testFilePaths = new Set(
    (await g.V().hasLabel("Test").values("filePath").toList())
  );

  const nonTestFunctions = functions.filter(
    (fnMap) => !testFilePaths.has(fnMap.get("filePath"))
  );

  const totalChanged = nonTestFunctions.length;
  const uncovered = [];

  for (const fnMap of nonTestFunctions) {
    const vertexId = fnMap.get(gremlin.process.t.id);
    const hasTest = await g.V(vertexId).inE("tests").limit(1).toList();

    if (hasTest.length === 0) {
      uncovered.push({
        name: fnMap.get("name"),
        signature: fnMap.get("signature"),
        filePath: fnMap.get("filePath"),
        linesStart: fnMap.get("lines_start"),
        linesEnd: fnMap.get("lines_end"),
      });
    }
  }

  const totalCovered = totalChanged - uncovered.length;

  return { uncovered, totalChanged, totalCovered };
}
