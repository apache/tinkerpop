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
import { CONFIDENCE } from "./confidence.js";

const { process: { statics: __ } } = gremlin;

const BATCH_SIZE = 50;

async function submitBatch(batch) {
  await Promise.allSettled(batch.map((t) => t.next()));
}

/**
 * The Discussion url a record's external ref names, matching the urls
 * populateDiscussions() writes. Commit refs (`apache/tinkerpop@sha`) name no
 * Discussion and return null.
 * @param {string|null} ref
 * @returns {string|null}
 */
export function discussionUrlForRef(ref) {
  if (!ref) return null;
  const pr = ref.match(/^apache\/tinkerpop#(\d+)$/);
  if (pr) return `https://github.com/apache/tinkerpop/pull/${pr[1]}`;
  if (/^TINKERPOP-\d+$/.test(ref)) return `https://issues.apache.org/jira/browse/${ref}`;
  if (ref.includes("@")) return null;
  return ref;
}

// bd dependency types become edge labels with underscores: caused-by -> caused_by.
const edgeLabel = (type) => type.replace(/-/g, "_");

/**
 * Load the beads found by discoverBeads() into the graph: a Bead vertex per
 * bead, `child_of` for the parent-child tree, bd's other dependencies carried
 * over as edges between loaded beads, and `records` from a record bead to the
 * Discussion its external ref names. Every edge is EXTRACTED: each one is a
 * fact in the beads database.
 *
 * @param {object} g - gremlin-js GraphTraversalSource (already connected)
 * @param {import("../discovery/beads.js").BeadsDiscovery} beads
 * @returns {Promise<{vertices: number, edges: number, breakdown: object}>}
 */
export async function populateBeads(g, beads) {
  const counts = { vertices: 0, edges: 0, breakdown: { beads: 0, childOf: 0, records: 0, dependencies: 0 } };
  if (!beads || !beads.beads || beads.beads.length === 0) return counts;

  const roleOf = new Map((beads.roots || []).map((r) => [r.id, r.role]));
  const loaded = new Set(beads.beads.map((b) => b.id));

  for (const b of beads.beads) {
    let t = g.addV("Bead")
      .property("beadId", b.id)
      .property("type", b.type)
      .property("status", b.status)
      .property("title", b.title)
      .property("description", (b.description || "").slice(0, 4000))
      .property("labels", (b.labels || []).join(", "))
      .property("root", b.root);
    if (b.rejected !== null) t = t.property("rejected", b.rejected);
    if (b.parent) t = t.property("parent", b.parent);
    if (b.externalRef) t = t.property("externalRef", b.externalRef);
    if (b.closedAt) t = t.property("closedAt", b.closedAt);
    if (roleOf.has(b.id)) t = t.property("rootRole", roleOf.get(b.id));
    await t.next();
    counts.vertices++;
    counts.breakdown.beads++;
  }

  const bead = (id) => __.V().hasLabel("Bead").has("beadId", id);
  let batch = [];
  const push = async (traversal, key) => {
    batch.push(traversal);
    counts.edges++;
    counts.breakdown[key]++;
    if (batch.length >= BATCH_SIZE) { await submitBatch(batch); batch = []; }
  };

  for (const b of beads.beads) {
    if (b.parent && loaded.has(b.parent)) {
      await push(
        g.V().hasLabel("Bead").has("beadId", b.id)
          .addE("child_of").property("confidence", CONFIDENCE.EXTRACTED)
          .to(bead(b.parent)),
        "childOf",
      );
    }
    for (const e of b.edges || []) {
      if (!loaded.has(e.to)) continue;
      await push(
        g.V().hasLabel("Bead").has("beadId", b.id)
          .addE(edgeLabel(e.type)).property("confidence", CONFIDENCE.EXTRACTED)
          .to(bead(e.to)),
        "dependencies",
      );
    }
  }

  // records: only to Discussions discovery already created, so a record for some
  // other PR in a loaded subtree doesn't invent a Discussion vertex.
  for (const b of beads.beads) {
    if (b.type !== "record") continue;
    const url = discussionUrlForRef(b.externalRef);
    if (!url || !(await g.V().hasLabel("Discussion").has("url", url).hasNext())) continue;
    await push(
      g.V().hasLabel("Bead").has("beadId", b.id)
        .addE("records").property("confidence", CONFIDENCE.EXTRACTED)
        .to(__.V().hasLabel("Discussion").has("url", url)),
      "records",
    );
  }

  if (batch.length > 0) await submitBatch(batch);
  return counts;
}
