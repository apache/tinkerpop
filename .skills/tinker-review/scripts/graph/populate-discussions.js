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
import { CONFIDENCE, confidenceForFoundIn } from "./confidence.js";

const { process: { statics: __ } } = gremlin;

const BATCH_SIZE = 50;

async function submitBatch(batch) {
  await Promise.allSettled(batch.map((t) => t.next()));
}

/**
 * Populate TinkerGraph with discussion data from discoverDiscussions().
 * Creates Discussion and Comment vertices plus has_comment, addresses,
 * proposed_in, and modifies edges per the PR knowledge graph schema.
 *
 * @param {object} g - gremlin-js GraphTraversalSource (already connected)
 * @param {object} discussions - Output from discoverDiscussions()
 * @param {object} context
 * @param {number} context.pr - PR number
 * @param {string} context.prTitle - PR title
 * @param {string[]} context.changedFiles - Files modified in the PR
 * @returns {Promise<{vertices: number, edges: number, breakdown: object}>}
 */
export async function populateDiscussions(g, discussions, context) {
  const counts = {
    vertices: 0,
    edges: 0,
    breakdown: { discussions: 0, comments: 0, hasComment: 0, addresses: 0, proposedIn: 0, modifies: 0 },
  };

  const prUrl = `https://github.com/apache/tinkerpop/pull/${context.pr}`;

  // Create the PR itself as a Discussion vertex. Idempotent: review.js may have
  // already created it via createPrDiscussion(). A duplicate PR vertex would make
  // `.V().has("url", prUrl)` match two start vertices and double every PR-sourced
  // edge (modifies, addresses, has_comment).
  const prExists = await g.V().hasLabel("Discussion").has("url", prUrl).hasNext();
  if (!prExists) {
    await g.addV("Discussion")
      .property("url", prUrl)
      .property("source", "pr")
      .property("title", context.prTitle || `PR #${context.pr}`)
      .property("body", "")
      .next();
    counts.vertices++;
    counts.breakdown.discussions++;
  }

  // Create Discussion vertices for JIRAs
  for (const jira of discussions.jiras) {
    await g.addV("Discussion")
      .property("url", jira.url)
      .property("source", "jira")
      .property("title", jira.title)
      .property("body", (jira.body || "").slice(0, 2000))
      .next();
    counts.vertices++;
    counts.breakdown.discussions++;

    // Create Comment vertices for JIRA comments
    for (const comment of (jira.comments || [])) {
      await g.addV("Comment")
        .property("author", comment.author)
        .property("body", comment.body)
        .property("timestamp", comment.timestamp || "")
        .next();
      counts.vertices++;
      counts.breakdown.comments++;
    }
  }

  // Create Discussion vertices for dev list threads
  for (const thread of discussions.devList) {
    await g.addV("Discussion")
      .property("url", thread.url)
      .property("source", "devlist")
      .property("title", thread.title)
      .property("body", (thread.body || "").slice(0, 2000))
      .next();
    counts.vertices++;
    counts.breakdown.discussions++;
  }

  // Create Discussion vertices for secondary (cross-referenced) discussions
  for (const sec of (discussions.secondary || [])) {
    await g.addV("Discussion")
      .property("url", sec.url)
      .property("source", sec.source)
      .property("title", sec.title || "")
      .property("body", (sec.body || "").slice(0, 2000))
      .next();
    counts.vertices++;
    counts.breakdown.discussions++;

    for (const comment of (sec.comments || [])) {
      await g.addV("Comment")
        .property("author", comment.author)
        .property("body", comment.body)
        .property("timestamp", comment.timestamp || "")
        .next();
      counts.vertices++;
      counts.breakdown.comments++;
    }
  }

  // Create Comment vertices for PR issue comments
  for (const comment of (discussions.prComments?.issue || [])) {
    await g.addV("Comment")
      .property("author", comment.author)
      .property("body", comment.body)
      .property("timestamp", comment.timestamp || "")
      .next();
    counts.vertices++;
    counts.breakdown.comments++;
  }

  // Create Comment vertices for PR review comments
  for (const comment of (discussions.prComments?.review || [])) {
    await g.addV("Comment")
      .property("author", comment.author)
      .property("body", comment.body)
      .property("timestamp", comment.timestamp || "")
      .next();
    counts.vertices++;
    counts.breakdown.comments++;
  }

  // Create Discussion vertices for proposals
  for (const proposal of (discussions.proposals || [])) {
    await g.addV("Discussion")
      .property("url", proposal.path || "")
      .property("source", "proposal")
      .property("title", proposal.title)
      .property("body", (proposal.snippet || "").slice(0, 2000))
      .next();
    counts.vertices++;
    counts.breakdown.discussions++;
  }

  // --- Edges (batched) ---
  let batch = [];

  // has_comment: PR Discussion -> PR comments
  for (const comment of (discussions.prComments?.issue || [])) {
    batch.push(
      g.V().hasLabel("Discussion").has("url", prUrl)
        .addE("has_comment")
        .property("confidence", CONFIDENCE.EXTRACTED)
        .to(__.V().hasLabel("Comment").has("author", comment.author).has("timestamp", comment.timestamp || ""))
    );
    counts.edges++;
    counts.breakdown.hasComment++;
    if (batch.length >= BATCH_SIZE) { await submitBatch(batch); batch = []; }
  }

  for (const comment of (discussions.prComments?.review || [])) {
    batch.push(
      g.V().hasLabel("Discussion").has("url", prUrl)
        .addE("has_comment")
        .property("confidence", CONFIDENCE.EXTRACTED)
        .to(__.V().hasLabel("Comment").has("author", comment.author).has("timestamp", comment.timestamp || ""))
    );
    counts.edges++;
    counts.breakdown.hasComment++;
    if (batch.length >= BATCH_SIZE) { await submitBatch(batch); batch = []; }
  }

  // has_comment: JIRA Discussion -> JIRA comments
  for (const jira of discussions.jiras) {
    for (const comment of (jira.comments || [])) {
      batch.push(
        g.V().hasLabel("Discussion").has("url", jira.url)
          .addE("has_comment")
        .property("confidence", CONFIDENCE.EXTRACTED)
          .to(__.V().hasLabel("Comment").has("author", comment.author).has("timestamp", comment.timestamp || ""))
      );
      counts.edges++;
      counts.breakdown.hasComment++;
      if (batch.length >= BATCH_SIZE) { await submitBatch(batch); batch = []; }
    }
  }

  // has_comment: Secondary Discussion -> their comments
  for (const sec of (discussions.secondary || [])) {
    for (const comment of (sec.comments || [])) {
      batch.push(
        g.V().hasLabel("Discussion").has("url", sec.url)
          .addE("has_comment")
        .property("confidence", CONFIDENCE.EXTRACTED)
          .to(__.V().hasLabel("Comment").has("author", comment.author).has("timestamp", comment.timestamp || ""))
      );
      counts.edges++;
      counts.breakdown.hasComment++;
      if (batch.length >= BATCH_SIZE) { await submitBatch(batch); batch = []; }
    }
  }

  // addresses: PR Discussion -> JIRA discussions
  for (const jira of discussions.jiras) {
    batch.push(
      g.V().hasLabel("Discussion").has("url", prUrl)
        .addE("addresses")
        .property("found_in", jira.found_in || "pr")
        .property("confidence", confidenceForFoundIn(jira.found_in || "pr"))
        .to(__.V().hasLabel("Discussion").has("url", jira.url))
    );
    counts.edges++;
    counts.breakdown.addresses++;
    if (batch.length >= BATCH_SIZE) { await submitBatch(batch); batch = []; }
  }

  // addresses: PR Discussion -> dev list threads
  for (const thread of discussions.devList) {
    batch.push(
      g.V().hasLabel("Discussion").has("url", prUrl)
        .addE("addresses")
        .property("found_in", thread.found_in || "pr")
        .property("confidence", confidenceForFoundIn(thread.found_in || "pr"))
        .to(__.V().hasLabel("Discussion").has("url", thread.url))
    );
    counts.edges++;
    counts.breakdown.addresses++;
    if (batch.length >= BATCH_SIZE) { await submitBatch(batch); batch = []; }
  }

  // addresses: source Discussion -> secondary discussions (cross-references)
  for (const sec of (discussions.secondary || [])) {
    const sourceUrl = sec.found_via
      ? (discussions.jiras.find((j) => j.id === sec.found_via)?.url || sec.found_via)
      : prUrl;
    batch.push(
      g.V().hasLabel("Discussion").has("url", sourceUrl)
        .addE("addresses")
        .property("found_in", sec.found_in || "")
        .property("found_via", sec.found_via || "")
        .property("confidence", confidenceForFoundIn(sec.found_in))
        .to(__.V().hasLabel("Discussion").has("url", sec.url))
    );
    counts.edges++;
    counts.breakdown.addresses++;
    if (batch.length >= BATCH_SIZE) { await submitBatch(batch); batch = []; }
  }

  // proposed_in: link proposals to the PR Discussion
  for (const proposal of (discussions.proposals || [])) {
    batch.push(
      g.V().hasLabel("Discussion").has("source", "proposal").has("title", proposal.title)
        .addE("proposed_in")
        .property("confidence", CONFIDENCE.INFERRED)
        .to(__.V().hasLabel("Discussion").has("url", prUrl))
    );
    counts.edges++;
    counts.breakdown.proposedIn++;
    if (batch.length >= BATCH_SIZE) { await submitBatch(batch); batch = []; }
  }

  // modifies: PR Discussion -> changed Files
  for (const filePath of (context.changedFiles || [])) {
    batch.push(
      g.V().hasLabel("Discussion").has("url", prUrl)
        .addE("modifies")
        .property("confidence", CONFIDENCE.EXTRACTED)
        .to(__.V().hasLabel("File").has("path", filePath))
    );
    counts.edges++;
    counts.breakdown.modifies++;
    if (batch.length >= BATCH_SIZE) { await submitBatch(batch); batch = []; }
  }

  if (batch.length > 0) {
    await submitBatch(batch);
  }

  return counts;
}
