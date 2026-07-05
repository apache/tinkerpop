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

const INHERENTLY_CENTRAL = new Set([
  "equals", "hashCode", "toString", "clone", "close", "compareTo",
  "iterator", "hasNext", "next", "get", "set", "size", "isEmpty",
  "contains", "add", "remove", "clear", "values", "entrySet", "keySet",
  "finalize", "notify", "notifyAll", "wait",
]);

const { statics: __, t: T } = gremlin.process;

/**
 * Calculate blast radius — how far a change ripples. Two seeds:
 *
 *  - Function-seeded: from each changed function, count what's reachable
 *    upstream via `calls` AND `overrides`. The `overrides` hop is what makes
 *    this see impact flowing through interface/abstract hierarchies — change an
 *    interface method and every override counts, which a call-only walk misses.
 *  - Type-seeded (hierarchy bucket): from each changed Type (typically an
 *    interface), count the functions declared by everything that implements or
 *    extends it. Reported separately so call/override impact and pure
 *    type-hierarchy impact stay legible rather than summed into one number.
 *
 * Methods that are inherently central (equals, toString, etc.) are filtered
 * out UNLESS they were modified in this PR.
 *
 * @param {object} g - gremlin-js GraphTraversalSource
 * @param {object} params
 * @param {number} [params.depth] - Max hops to traverse (default: 3)
 * @param {boolean} [params.changedOnly] - Start from changed functions only (default: true)
 * @returns {Promise<BlastRadiusResult>}
 */

/**
 * @typedef {Object} BlastRadiusFn
 * @property {string}  name
 * @property {string}  filePath
 * @property {string}  signature
 * @property {number}  linesStart
 * @property {number}  linesEnd
 * @property {boolean} changed         whether the PR modified this function
 * @property {number}  reachableCount  callers + overriders reachable within `depth` hops upstream;
 *                                      high = the change ripples widely (for driver/server this is expected)
 * @property {number}  depth           hop limit used for this row
 *
 * @typedef {Object} BlastRadiusType
 * @property {string}  name
 * @property {string}  filePath
 * @property {string}  kind            class | interface | struct | enum
 * @property {number}  implementerCount functions declared by types that implement/extend this one
 * @property {number}  depth
 *
 * @typedef {Object} BlastRadiusResult
 * @property {BlastRadiusFn[]}   functions        changed functions and how far each one's change reaches
 * @property {BlastRadiusType[]} types            changed types and how many implementers' functions they reach
 * @property {number}            maxReachable     largest reachableCount across changed functions
 * @property {number}            totalWithCallers changed functions that have any upstream callers/overriders
 * @property {number}            depth            hop limit applied
 */
export async function blastRadius(g, params = {}) {
  const depth = params.depth || 3;
  const changedOnly = params.changedOnly !== false;

  let traversal = g.V().hasLabel("Function").hasNot("external");
  if (changedOnly) {
    traversal = traversal.has("changed", true);
  }

  const functions = await traversal.elementMap().toList();
  const results = [];

  for (const fnMap of functions) {
    const vertexId = fnMap.get(T.id);
    const name = fnMap.get("name");
    const changed = fnMap.get("changed");

    if (INHERENTLY_CENTRAL.has(name) && !changed) continue;

    const reachable = await g.V(vertexId)
      .repeat(__.union(__.in_("calls"), __.in_("overrides")).dedup())
      .times(depth)
      .emit()
      .dedup()
      .count()
      .next();

    const count = reachable.value;
    if (count > 0) {
      results.push({
        name,
        filePath: fnMap.get("filePath"),
        signature: fnMap.get("signature"),
        linesStart: fnMap.get("lines_start"),
        linesEnd: fnMap.get("lines_end"),
        changed,
        reachableCount: count,
        depth,
      });
    }
  }

  results.sort((a, b) => b.reachableCount - a.reachableCount);

  // Type-seeded hierarchy impact: functions declared by everything that
  // implements/extends a changed type, walked in the implementer->supertype
  // direction (`in`) up to `depth` levels of the hierarchy.
  let typeTraversal = g.V().hasLabel("Type").hasNot("external");
  if (changedOnly) {
    typeTraversal = typeTraversal.has("changed", true);
  }
  const changedTypes = await typeTraversal.elementMap().toList();
  const typeResults = [];

  for (const typeMap of changedTypes) {
    const vertexId = typeMap.get(T.id);
    const implementerCount = await g.V(vertexId)
      .repeat(__.in_("implements", "extends").dedup())
      .times(depth)
      .emit()
      .dedup()
      .out("declares")
      .hasNot("external")
      .dedup()
      .count()
      .next();

    const count = Number(implementerCount.value);
    if (count > 0) {
      typeResults.push({
        name: typeMap.get("name"),
        filePath: typeMap.get("filePath"),
        kind: typeMap.get("kind"),
        implementerCount: count,
        depth,
      });
    }
  }

  typeResults.sort((a, b) => b.implementerCount - a.implementerCount);

  return {
    functions: results,
    types: typeResults,
    maxReachable: results.length > 0 ? results[0].reachableCount : 0,
    totalWithCallers: results.length,
    depth,
  };
}
