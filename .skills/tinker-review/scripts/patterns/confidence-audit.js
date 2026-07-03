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
import { CONFIDENCE } from "../graph/confidence.js";

const { process: { statics: __ } } = gremlin;

// Pick a human-readable label for a vertex from whichever identifying property
// it carries. Different labels key on different properties (Function/Type/Step
// use name, File uses path, Discussion uses url/title).
function describeVertex(elementMap) {
  if (!elementMap) return "?";
  const get = (k) => {
    const v = elementMap.get ? elementMap.get(k) : elementMap[k];
    return v == null ? undefined : String(v);
  };
  const labelKey = gremlin.process.t.label;
  const label = (elementMap.get ? elementMap.get(labelKey) : elementMap["label"]) || "";
  const name = get("name") || get("title") || get("path") || get("url") || "?";
  return label ? `${label}(${name})` : name;
}

/**
 * Audit the knowledge graph by edge confidence. Returns the distribution across
 * EXTRACTED / INFERRED / AMBIGUOUS (plus UNTAGGED for any edge missing the
 * property) and the full list of AMBIGUOUS edges — the ones flagged for human
 * review. Since it reads the live graph it reflects enrichment edges too when
 * run after Phase 2.
 *
 * @param {object} g - gremlin-js GraphTraversalSource (already connected)
 * @param {object} [params]
 * @param {number} [params.maxAmbiguous] - Cap the AMBIGUOUS edge list (default 50)
 * @returns {Promise<{distribution: object, total: number, ambiguous: object[]}>}
 */
export async function confidenceAudit(g, params = {}) {
  const maxAmbiguous = params.maxAmbiguous || 50;

  // Distribution over every edge; edges predating this feature (or any we missed)
  // fall into UNTAGGED so the audit stays honest about coverage.
  const grouped = await g.E()
    .groupCount()
    .by(__.coalesce(__.values("confidence"), __.constant("UNTAGGED")))
    .next();

  const distribution = { EXTRACTED: 0, INFERRED: 0, AMBIGUOUS: 0, UNTAGGED: 0 };
  let total = 0;
  const gm = grouped.value;
  if (gm) {
    const entries = gm instanceof Map ? gm.entries() : Object.entries(gm);
    for (const [key, count] of entries) {
      const n = Number(count);
      distribution[key] = (distribution[key] || 0) + n;
      total += n;
    }
  }

  const ambiguousRows = await g.E()
    .has("confidence", CONFIDENCE.AMBIGUOUS)
    .project("relation", "found_in", "found_via", "from", "to")
    .by(__.label())
    .by(__.coalesce(__.values("found_in"), __.constant("")))
    .by(__.coalesce(__.values("found_via"), __.constant("")))
    .by(__.outV().elementMap())
    .by(__.inV().elementMap())
    .limit(maxAmbiguous)
    .toList();

  const ambiguous = ambiguousRows.map((row) => ({
    relation: row.get("relation"),
    from: describeVertex(row.get("from")),
    to: describeVertex(row.get("to")),
    foundIn: row.get("found_in") || undefined,
    foundVia: row.get("found_via") || undefined,
  }));

  return { distribution, total, ambiguous };
}
