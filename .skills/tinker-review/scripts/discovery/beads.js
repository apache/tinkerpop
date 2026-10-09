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

// Beads discovery: find the bead roots recorded for the change under review and
// load their subtrees. The project's beads schema (.beads/PRIME.md section 6)
// puts every external artifact — JIRA, PR, dev@ thread, proposal — on a `record`
// bead that is a parent-child child of its root, and only records carry an
// external ref. A second root that needs the same artifact links to the existing
// record with a `related` edge. So a record whose ref names one of the PR's
// artifacts leads to the owning root and any sharing roots, and everything
// reached that way is EXTRACTED.
//
// The review reads beads and never writes them: every call except the sync
// passes bd's --readonly flag, which makes bd itself refuse a write.

import { execFile } from "node:child_process";
import { promisify } from "node:util";

const exec = promisify(execFile);

export const BEADS_REQUIRED_MESSAGE =
  "tinker-review requires beads (bd). Install it and run `bin/agent-setup.sh --contributor`, then re-run the review.";

/**
 * @typedef {object} Bead
 * A bead from the reviewer's local beads database.
 * @property {string} id - Bead id (e.g. `tp-hv9.1`). Never parse it for structure; `parent` is the truth.
 * @property {string} type - bd issue type: `epic`, `feature`, `task`, `bug`, `decision`, `record`, ...
 * @property {string} status - `open`, `in_progress`, `closed`, `pinned`, ... At PR time the root is
 *   expected to be open (it closes at merge), and decisions are closed as they are written, so
 *   status means most on tasks: `in_progress` is claimed, unfinished work.
 * @property {string} title
 * @property {string} description
 * @property {string} design - For a decision, the reasoning: why it was chosen, or what the rejected
 *   option was, why it lost and what settled it.
 * @property {boolean|null} rejected - On a decision, true marks the road not taken; null on other types.
 * @property {string[]} labels
 * @property {string|null} parent - Parent bead id; null on a root.
 * @property {string|null} externalRef - The artifact a record names (`TINKERPOP-NNNN`,
 *   `apache/tinkerpop#N`, `apache/tinkerpop@sha`, a dev@ URL); null on non-records.
 * @property {string|null} closedAt
 * @property {string} root - Id of the root this bead belongs to.
 * @property {{ to: string, type: string }[]} edges - Outgoing bd dependencies other than
 *   parent-child (`blocks`, `caused-by`, `related`, `supersedes`, `discovered-from`, ...).
 */

/**
 * @typedef {object} BeadRoot
 * A root reached from a matched record.
 * @property {string} id - Root bead id.
 * @property {"primary"|"owner"|"shared"} role - `primary`: the root of this PR's own record
 *   (`apache/tinkerpop#N`), the work the PR delivers. `owner`: the root a matched record belongs
 *   to. `shared`: a root that links a matched record with `related` — connected work elsewhere.
 * @property {string[]} via - Ids of the matched records that reached this root.
 */

/**
 * @typedef {object} BeadsDiscovery
 * What beads discovery found for the PR. Present in `evidence.discussions.beads`.
 * @property {boolean} synced - Whether `bd dolt pull` succeeded. When false the review used the
 *   local database as it was, which may be missing beads pushed since the last sync.
 * @property {string|null} syncError - Why the sync failed, when it did.
 * @property {string[]} seeds - External refs searched: the PR, its JIRAs, explicitly linked dev@
 *   threads and explicitly referenced proposals.
 * @property {{ seed: string, record: string }[]} matches - Record beads whose external ref equals a seed.
 * @property {BeadRoot[]} roots - Roots reached from the matches, primary first. Empty when no
 *   record matched; the review then proceeds without beads, and that is not a finding.
 * @property {Bead[]} beads - Every bead in the reached roots' subtrees, roots included.
 * @property {Object<string, {author: string, text: string, createdAt: string}[]>} comments -
 *   Comments by bead id, for roots and decisions (where facts and later context are recorded).
 */

/**
 * A function that runs bd with the given arguments from the repo checkout and
 * returns its parsed JSON output. Read commands get --readonly and --json added.
 * @callback BdRunner
 * @param {string[]} args
 * @returns {Promise<any>}
 */

/**
 * Build a {@link BdRunner} that runs the real bd from `repoPath`.
 * @param {string} repoPath
 * @returns {BdRunner}
 */
export function bdRunner(repoPath) {
  return async (args) => {
    const { stdout } = await exec("bd", [...args, "--readonly", "--json"], {
      cwd: repoPath,
      maxBuffer: 32 * 1024 * 1024,
    });
    return stdout.trim() ? JSON.parse(stdout) : null;
  };
}

/**
 * Fail early when beads is unavailable: bd must be installed and must open the
 * repo's database. Throws an Error carrying {@link BEADS_REQUIRED_MESSAGE}.
 * @param {string} repoPath
 * @param {BdRunner} [run]
 */
export async function requireBeads(repoPath, run = bdRunner(repoPath)) {
  try {
    await run(["list", "-n", "1"]);
  } catch (err) {
    const detail = err.code === "ENOENT" ? "bd was not found on the PATH" : (err.stderr || err.message || "").trim();
    throw new Error(`${BEADS_REQUIRED_MESSAGE}\n  (${detail})`);
  }
}

/**
 * Pull the shared beads database so the review sees the latest state. A failed
 * pull is not fatal: the review continues on the local state and reports it.
 * @param {string} repoPath
 * @param {(args: string[]) => Promise<unknown>} [pull] - runs `bd <args>`; injectable for tests.
 * @returns {Promise<{ synced: boolean, syncError: string|null }>}
 */
export async function syncBeads(repoPath, pull = (args) => exec("bd", args, { cwd: repoPath, timeout: 120000 })) {
  try {
    await pull(["dolt", "pull"]);
    return { synced: true, syncError: null };
  } catch (err) {
    return { synced: false, syncError: (err.stderr || err.message || "bd dolt pull failed").toString().trim() };
  }
}

// Trailing punctuation a URL picks up from surrounding prose.
const stripTrailing = (s) => s.replace(/[.,)\]>]+$/, "");

/**
 * The external refs a record could carry for this PR. Only artifacts the PR
 * itself names are seeds — its own ref, its JIRAs, explicitly linked dev@
 * threads and explicitly referenced proposals. Keyword-searched dev@ threads and
 * keyword-matched proposals are guesses, and an EXTRACTED match can't rest on one.
 * @param {number} pr
 * @param {object} discussions - Output of discoverDiscussions().
 * @returns {string[]}
 */
export function beadSeeds(pr, discussions = {}) {
  const seeds = [`apache/tinkerpop#${pr}`];
  for (const j of discussions.jiras || []) seeds.push(j.id);
  for (const d of discussions.devList || []) if (d.found_in === "pr") seeds.push(stripTrailing(d.url));
  for (const p of discussions.proposals || []) if (p.matchedIn === "reference") seeds.push(p.path);
  return [...new Set(seeds.filter(Boolean))];
}

function toBead(raw) {
  return {
    id: raw.id,
    type: raw.issue_type,
    status: raw.status,
    title: raw.title || "",
    description: raw.description || "",
    design: raw.design || "",
    rejected: raw.issue_type === "decision" ? raw.metadata?.rejected === true : null,
    labels: raw.labels || [],
    parent: raw.parent || null,
    externalRef: raw.external_ref || null,
    closedAt: raw.closed_at || null,
    root: null,
    edges: (raw.dependencies || [])
      .filter((d) => d.type !== "parent-child")
      .map((d) => ({ to: d.depends_on_id, type: d.type })),
  };
}

/**
 * Find the bead roots recorded for a PR and load their subtrees.
 *
 * 1. For each seed, `bd search <prefix> --external-contains <seed> --type record`,
 *    keeping exact matches (the flag is a substring match). The positional query
 *    is also applied, as a title/ID match, so it is the project's ID prefix — which
 *    every bead matches — rather than the ref, which a record's title need not repeat.
 * 2. Each matched record's top-most ancestor is its owner root; every bead that
 *    links the record with `related` leads to a shared root. A PR record's owner
 *    root is primary. Every root reached is loaded — connected work is context.
 * 3. Load each root's subtree. `bd children --json` returns one level only (its
 *    text output is the tree), so the candidates come from
 *    `bd dep tree <root> --direction up`, which walks every
 *    dependent in one call — including beads outside the subtree that merely
 *    relate to one inside it. One `bd list --id` call then supplies each
 *    candidate's real `parent` and full fields, and only beads whose parent
 *    chain reaches the root are kept. Comments are read for the roots and
 *    decisions that have any.
 *
 * @param {object} params
 * @param {string[]} params.seeds - From {@link beadSeeds}.
 * @param {string} [params.prRef] - This PR's ref, `apache/tinkerpop#N`; marks its root primary.
 * @param {BdRunner} params.run
 * @returns {Promise<Omit<BeadsDiscovery, "synced"|"syncError">>}
 */
export async function discoverBeads({ seeds, prRef, run }) {
  const result = { seeds, matches: [], roots: [], beads: [], comments: {} };

  const [sample] = (await run(["list", "-n", "1", "--all", "--flat"])) || [];
  if (!sample) return result;
  const idPrefix = sample.id.slice(0, sample.id.indexOf("-") + 1);

  for (const seed of seeds) {
    const hits = (await run(["search", idPrefix, "--external-contains", seed, "--type", "record", "--status", "all", "--limit", "100"])) || [];
    for (const h of hits) {
      if (h.external_ref === seed) result.matches.push({ seed, record: h.id });
    }
  }
  if (result.matches.length === 0) return result;

  const fetched = new Map();
  const fetch = async (ids) => {
    const missing = ids.filter((id) => !fetched.has(id));
    if (missing.length === 0) return;
    const rows = (await run(["list", "--id", missing.join(","), "--all", "-n", "0", "--flat"])) || [];
    for (const r of rows) fetched.set(r.id, r);
  };
  const topOf = async (id) => {
    let cur = id;
    const seen = new Set();
    for (;;) {
      await fetch([cur]);
      const parent = fetched.get(cur)?.parent;
      if (!parent || seen.has(parent)) return cur;
      seen.add(cur);
      cur = parent;
    }
  };

  const RANK = { primary: 0, owner: 1, shared: 2 };
  const roots = new Map();
  const reach = (rootId, role, record) => {
    const r = roots.get(rootId) || { id: rootId, role, via: [] };
    if (RANK[role] < RANK[r.role]) r.role = role;
    if (!r.via.includes(record)) r.via.push(record);
    roots.set(rootId, r);
  };

  for (const { seed, record } of result.matches) {
    reach(await topOf(record), seed === prRef ? "primary" : "owner", record);
    const sharers = (await run(["dep", "list", record, "--direction", "up", "-t", "related"])) || [];
    for (const s of sharers) reach(await topOf(s.id), "shared", record);
  }
  result.roots = [...roots.values()].sort((a, b) => RANK[a.role] - RANK[b.role]);

  for (const { id: rootId } of result.roots) {
    if (result.beads.some((b) => b.root === rootId)) continue;
    const tree = (await run(["dep", "tree", rootId, "--direction", "up"])) || [];
    await fetch(tree.map((t) => t.id));
    const inSubtree = new Set([rootId]);
    let grew = true;
    while (grew) {
      grew = false;
      for (const { id } of tree) {
        if (!inSubtree.has(id) && inSubtree.has(fetched.get(id)?.parent)) {
          inSubtree.add(id);
          grew = true;
        }
      }
    }
    for (const id of inSubtree) {
      const raw = fetched.get(id);
      if (raw) result.beads.push({ ...toBead(raw), root: rootId });
    }
  }

  for (const b of result.beads) {
    if (b.id !== b.root && b.type !== "decision") continue;
    if (!fetched.get(b.id)?.comment_count) continue;
    const comments = (await run(["comments", b.id])) || [];
    if (comments.length > 0) {
      result.comments[b.id] = comments.map((c) => ({ author: c.author, text: c.text, createdAt: c.created_at }));
    }
  }

  return result;
}
