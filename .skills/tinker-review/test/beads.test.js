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

// Beads discovery against a fake bd — record matching, root roles, subtree
// loading, the beads gate and the sync fallback — and the Project Memory
// section the renderer builds from it.

import { test } from "node:test";
import assert from "node:assert/strict";

import {
  BEADS_REQUIRED_MESSAGE, beadSeeds, discoverBeads, requireBeads, syncBeads,
} from "../scripts/discovery/beads.js";
import { render } from "../scripts/renderer/render.js";

// A small beads database shaped like PRIME.md section 6. Each entry is the row
// bd list --json returns; `deps` are outgoing dependencies as [target, type].
function bead(id, type, { parent = null, ref = null, rejected, deps = [], comments = 0, title = id } = {}) {
  return {
    id, issue_type: type, status: type === "decision" ? "closed" : "open", title,
    description: "", design: `design of ${id}`, labels: ["gremlin-core"], parent,
    external_ref: ref, closed_at: null, comment_count: comments,
    metadata: rejected === undefined ? null : { rejected },
    dependencies: [
      ...(parent ? [{ depends_on_id: parent, type: "parent-child" }] : []),
      ...deps.map(([to, t]) => ({ depends_on_id: to, type: t })),
    ],
  };
}

const DB = [
  bead("tp-a", "feature", { comments: 2 }),
  bead("tp-a.1", "record", { parent: "tp-a", ref: "TINKERPOP-1234", title: "PR and JIRA" }),
  bead("tp-a.2", "record", { parent: "tp-a", ref: "https://lists.apache.org/thread/abc", title: "dev@ thread: topic" }),
  bead("tp-a.3", "decision", { parent: "tp-a", rejected: false, deps: [["tp-a.4", "related"]], comments: 1 }),
  bead("tp-a.4", "decision", { parent: "tp-a", rejected: true }),
  bead("tp-a.5", "task", { parent: "tp-a", deps: [["tp-a.3", "caused-by"]] }),
  bead("tp-a.5.1", "task", { parent: "tp-a.5" }),
  // A second root that shares tp-a's JIRA record instead of duplicating it.
  bead("tp-b", "epic", { deps: [["tp-a.1", "related"]] }),
  bead("tp-b.1", "record", { parent: "tp-b", ref: "apache/tinkerpop#99" }),
  bead("tp-b.2", "decision", { parent: "tp-b", rejected: false }),
  // Unrelated work that relates to a bead inside tp-a: reachable by bd dep tree
  // from tp-a, but not part of tp-a's subtree.
  bead("tp-c", "task", { deps: [["tp-a.3", "related"]] }),
  // Substring trap for --external-contains.
  bead("tp-d", "feature"),
  bead("tp-d.1", "record", { parent: "tp-d", ref: "TINKERPOP-12345" }),
];

const COMMENTS = {
  "tp-a": [{ author: "a", text: "a fact", created_at: "2026-01-01" }, { author: "b", text: "another", created_at: "2026-01-02" }],
  "tp-a.3": [{ author: "a", text: "later context", created_at: "2026-01-03" }],
};

const byId = new Map(DB.map((b) => [b.id, b]));
const dependentsOf = (id) => DB.filter((b) => b.dependencies.some((d) => d.depends_on_id === id));

// Emulates the bd commands discovery uses, including the quirks it relies on:
// search's positional query is a title/ID match, and --external-contains is a
// case-insensitive substring match.
function fakeBd(calls = []) {
  return async (args) => {
    calls.push(args);
    const opt = (name) => args[args.indexOf(name) + 1];
    switch (args[0]) {
      case "list":
        if (args.includes("--id")) return opt("--id").split(",").map((id) => byId.get(id)).filter(Boolean);
        return DB.slice(0, Number(opt("-n")) || DB.length);
      case "search": {
        const q = args[1].toLowerCase();
        const ext = opt("--external-contains").toLowerCase();
        return DB.filter((b) => b.issue_type === opt("--type")
          && (b.id.toLowerCase().startsWith(q) || b.title.toLowerCase().includes(q))
          && (b.external_ref || "").toLowerCase().includes(ext));
      }
      case "dep":
        if (args[1] === "list") {
          return dependentsOf(args[2]).filter((b) => b.dependencies.some((d) => d.depends_on_id === args[2] && d.type === opt("-t")));
        }
        if (args[1] === "tree") {
          const seen = new Set([args[2]]);
          const out = [byId.get(args[2])];
          for (let i = 0; i < out.length; i++) {
            for (const d of dependentsOf(out[i].id)) if (!seen.has(d.id)) { seen.add(d.id); out.push(d); }
          }
          return out.map(({ id, issue_type, title }) => ({ id, issue_type, title }));
        }
        break;
      case "comments":
        return COMMENTS[args[1]] || [];
    }
    throw new Error(`fake bd: unhandled ${args.join(" ")}`);
  };
}

test("seeds are the PR plus only the artifacts the PR names explicitly", () => {
  const seeds = beadSeeds(42, {
    jiras: [{ id: "TINKERPOP-1234" }],
    devList: [
      { url: "https://lists.apache.org/thread/abc).", found_in: "pr" },
      { url: "https://lists.apache.org/thread/searched", found_in: "search" },
    ],
    proposals: [
      { path: "docs/src/dev/future/proposal-x.asciidoc", matchedIn: "reference" },
      { path: "docs/src/dev/future/proposal-y.asciidoc", matchedIn: "title" },
    ],
  });
  assert.deepEqual(seeds, [
    "apache/tinkerpop#42",
    "TINKERPOP-1234",
    "https://lists.apache.org/thread/abc",
    "docs/src/dev/future/proposal-x.asciidoc",
  ]);
});

test("a record matches only on an exact external ref", async () => {
  const r = await discoverBeads({ seeds: ["TINKERPOP-1234"], prRef: "apache/tinkerpop#1", run: fakeBd() });
  assert.deepEqual(r.matches, [{ seed: "TINKERPOP-1234", record: "tp-a.1" }], "TINKERPOP-12345 is not a match");
});

test("a record whose title does not repeat its ref is still found", async () => {
  const r = await discoverBeads({ seeds: ["https://lists.apache.org/thread/abc"], run: fakeBd() });
  assert.deepEqual(r.matches.map((m) => m.record), ["tp-a.2"]);
});

test("the owning root and every sharing root are loaded; the PR's own root is primary", async () => {
  const r = await discoverBeads({
    seeds: ["apache/tinkerpop#99", "TINKERPOP-1234"], prRef: "apache/tinkerpop#99", run: fakeBd(),
  });
  assert.deepEqual(r.roots, [
    { id: "tp-b", role: "primary", via: ["tp-b.1", "tp-a.1"] },
    { id: "tp-a", role: "owner", via: ["tp-a.1"] },
  ]);
});

test("a shared record leads to the sharing root as shared", async () => {
  const r = await discoverBeads({ seeds: ["TINKERPOP-1234"], prRef: "apache/tinkerpop#7", run: fakeBd() });
  assert.deepEqual(r.roots.map(({ id, role }) => ({ id, role })), [
    { id: "tp-a", role: "owner" },
    { id: "tp-b", role: "shared" },
  ]);
});

test("the subtree includes grandchildren and excludes beads that only relate to it", async () => {
  const r = await discoverBeads({ seeds: ["https://lists.apache.org/thread/abc"], run: fakeBd() });
  const ids = r.beads.map((b) => b.id).sort();
  assert.deepEqual(ids, ["tp-a", "tp-a.1", "tp-a.2", "tp-a.3", "tp-a.4", "tp-a.5", "tp-a.5.1"]);
  assert.ok(r.beads.every((b) => b.root === "tp-a"));
});

test("beads carry the rejected flag, external ref and non-tree edges", async () => {
  const r = await discoverBeads({ seeds: ["https://lists.apache.org/thread/abc"], run: fakeBd() });
  const get = (id) => r.beads.find((b) => b.id === id);
  assert.equal(get("tp-a.3").rejected, false);
  assert.equal(get("tp-a.4").rejected, true);
  assert.equal(get("tp-a.5").rejected, null, "only decisions have a rejected flag");
  assert.equal(get("tp-a.1").externalRef, "TINKERPOP-1234");
  assert.deepEqual(get("tp-a.3").edges, [{ to: "tp-a.4", type: "related" }]);
  assert.deepEqual(get("tp-a.5").edges, [{ to: "tp-a.3", type: "caused-by" }]);
  assert.equal(get("tp-a.5.1").parent, "tp-a.5");
});

test("comments are read only for roots and decisions that have them", async () => {
  const calls = [];
  const r = await discoverBeads({ seeds: ["https://lists.apache.org/thread/abc"], run: fakeBd(calls) });
  assert.deepEqual(Object.keys(r.comments).sort(), ["tp-a", "tp-a.3"]);
  assert.equal(r.comments["tp-a.3"][0].text, "later context");
  assert.deepEqual(calls.filter((c) => c[0] === "comments").map((c) => c[1]).sort(), ["tp-a", "tp-a.3"]);
});

test("no matching record means no roots and no beads", async () => {
  const r = await discoverBeads({ seeds: ["apache/tinkerpop#5", "TINKERPOP-9"], run: fakeBd() });
  assert.deepEqual([r.matches, r.roots, r.beads], [[], [], []]);
});

test("requireBeads fails early with the install message when bd is missing", async () => {
  const missing = async () => { throw Object.assign(new Error("spawn bd ENOENT"), { code: "ENOENT" }); };
  await assert.rejects(requireBeads("/repo", missing), (err) => {
    assert.ok(err.message.startsWith(BEADS_REQUIRED_MESSAGE));
    assert.match(err.message, /not found on the PATH/);
    return true;
  });
});

test("requireBeads passes when bd answers", async () => {
  await requireBeads("/repo", fakeBd());
});

test("a failed pull falls back to local state instead of failing", async () => {
  const offline = async () => { throw Object.assign(new Error("pull failed"), { stderr: "remote unreachable" }); };
  assert.deepEqual(await syncBeads("/repo", offline), { synced: false, syncError: "remote unreachable" });
  assert.deepEqual(await syncBeads("/repo", async () => {}), { synced: true, syncError: null });
});

// ---- Project Memory rendering ----


const meta = { pr: 99, headSha: "abc", title: "t", domains: [], timestamp: "now" };

function shaped(id, type, extra = {}) {
  return {
    id, type, status: "closed", title: `title ${id}`, description: "", design: `why ${id}`,
    rejected: type === "decision" ? false : null, labels: [], parent: null, externalRef: null,
    closedAt: null, root: "tp-a", edges: [], ...extra,
  };
}

const found = {
  synced: true, syncError: null,
  seeds: ["apache/tinkerpop#99"],
  matches: [{ seed: "apache/tinkerpop#99", record: "tp-a.1" }],
  roots: [{ id: "tp-a", role: "primary", via: ["tp-a.1"] }],
  beads: [
    shaped("tp-a", "feature", { status: "open", labels: ["gremlin-core", "3.7.7"] }),
    shaped("tp-a.1", "record", { parent: "tp-a", externalRef: "apache/tinkerpop#99" }),
    shaped("tp-a.2", "decision", { parent: "tp-a", edges: [{ to: "tp-a.3", type: "related" }] }),
    shaped("tp-a.3", "decision", { parent: "tp-a", rejected: true }),
    shaped("tp-a.4", "decision", { parent: "tp-a", edges: [{ to: "tp-a.2", type: "supersedes" }] }),
    shaped("tp-a.5", "task", { parent: "tp-a", status: "in_progress" }),
    shaped("tp-a.6", "task", { parent: "tp-a", status: "open" }),
    shaped("tp-a.7", "task", { parent: "tp-a", status: "open", labels: ["human"] }),
  ],
  comments: { "tp-a": [{ author: "a", text: "a fact on the root", createdAt: "x" }] },
};

test("Project Memory shows the root, its records, plan and decisions with alternatives nested", () => {
  const html = render({ meta, discussions: { beads: found } });
  assert.match(html, /This PR's work/);
  assert.match(html, /id="bead-tp-a"/);
  assert.match(html, /href="https:\/\/github.com\/apache\/tinkerpop\/pull\/99">apache\/tinkerpop#99<\/a> <em>\(matched\)<\/em>/);
  assert.match(html, /Plan:<\/strong> 1 in progress, 2 open, 0 done/);
  assert.match(html, /Unanswered questions:[\s\S]*tp-a\.7/);
  assert.match(html, /Decisions: 2 chosen, 1 not taken/);
  assert.match(html, /Chose<\/strong> title tp-a\.2[\s\S]*superseded by[\s\S]*bead-alternatives[\s\S]*Instead of<\/span> title tp-a\.3/);
  assert.match(html, /a fact on the root/);
  assert.match(html, /<h3>Beads<\/h3>/, "the appendix lists the loaded beads");
  for (const b of found.beads) {
    assert.match(html, new RegExp(`id="bead-${b.id.replace(/\./g, "\\.")}"`), `appendix link target for ${b.id}`);
  }
});

test("Project Memory says the data wasn't available when nothing matched, and when the sync failed", () => {
  const html = render({
    meta,
    discussions: { beads: { synced: false, syncError: "remote unreachable", seeds: ["apache/tinkerpop#99"], matches: [], roots: [], beads: [], comments: {} } },
  });
  assert.match(html, /beads data wasn't available for this change/);
  assert.match(html, /Searched record beads for: <code>apache\/tinkerpop#99<\/code>/);
  assert.match(html, /latest beads data wasn't available \(<code>bd dolt pull<\/code> failed: remote unreachable\)/);
  assert.doesNotMatch(html, /<h3>Beads<\/h3>/);
});

test("bead refs link into Project Memory and an unknown bead is reported", () => {
  const warnings = [];
  const html = render({
    meta,
    discussions: { beads: found },
    findings: [{ title: "f", body: "<p>b</p>", refs: [{ bead: "tp-a.3", label: "rejected option" }, { bead: "tp-zzz" }] }],
  }, { warnings });
  assert.match(html, /<a class="code-ref bead-ref" href="#bead-tp-a\.3"[^>]*>rejected option<\/a>/);
  assert.match(html, /code-ref-unresolved[^>]*>tp-zzz</);
  assert.deepEqual(warnings.length, 1);
  assert.match(warnings[0], /findings\[0\]\.refs: unresolved: no bead "tp-zzz"/);
});
