#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
"""Structural report over the beads graph.

ADVISORY ONLY. This never blocks anything. Run as a gate it would fire constantly
on legitimate work -- a one-line fix has no record bead and no decisions, and that
is correct.

Two scopes:

  bin/beads-report.py --root tp-abc   At merge, before pinning. Reads just that
                                      subtree via `bd children`. Bounded and fast.

  bin/beads-report.py                 At release, before purging. Reads the whole
                                      database via `bd export`. Its most valuable
                                      output is "here is every piece of rationale
                                      about to be deleted" -- review it, pin what
                                      should survive, then purge.

Checks divide into two kinds, and the distinction matters:

  OBJECTIVE   dangling edge targets; rationale on closed unpinned beads.
              Defects and facts. No judgment involved.

  HEURISTIC   decisions with no rejected alternative recorded; roots with
              several tasks and no decisions. These cannot distinguish "no
              decisions were made" from "decisions were not captured", so they
              are questions for a human, never verdicts.

Neither kind can judge whether design text is real reasoning or fluent filler.
Structure is checkable; substance is not.
"""

import argparse
import collections
import json
import subprocess
import sys


def bd(*args):
    """Run a bd command with --json and return the parsed result."""
    proc = subprocess.run(["bd", *args, "--json"], capture_output=True, text=True)
    if proc.returncode != 0:
        sys.exit(f"bd {' '.join(args)} failed: {proc.stderr.strip()}")
    if not proc.stdout.strip():
        return []
    return json.loads(proc.stdout)


def as_list(payload):
    """bd sometimes returns a bare object where a list is expected."""
    if isinstance(payload, dict):
        return payload.get("issues", [payload])
    return payload


def load_export():
    """Whole database, one record per line. Carries design and comments."""
    proc = subprocess.run(["bd", "export"], capture_output=True, text=True)
    if proc.returncode != 0:
        sys.exit(f"bd export failed: {proc.stderr.strip()}")
    beads = {}
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        record = json.loads(line)
        if record.get("_type") == "issue":
            beads[record["id"]] = record
    return beads


def load_subtree(root):
    """One root's subtree, gathered by descending one level at a time.

    `bd children` recurses in its text output but NOT under `--json`, which
    returns direct children only -- so a single call silently truncates the
    subtree to depth 1. Verified against a four-level tree at bd 1.1.2.

    The records returned carry no `design` field, only comment_count, so
    rationale-at-risk detection is weaker in this scope than at release scope."""
    beads = {}
    for record in as_list(bd("show", root)):
        beads.setdefault(record["id"], record)

    frontier = [root]
    seen = {root}
    while frontier:
        node = frontier.pop()
        for child in as_list(bd("children", node)):
            cid = child["id"]
            beads.setdefault(cid, child)
            if cid not in seen:
                seen.add(cid)
                frontier.append(cid)
    return beads


def edges_of(beads):
    return [
        (bid, dep["depends_on_id"], dep["type"])
        for bid, bead in beads.items()
        for dep in bead.get("dependencies", [])
    ]


def report(title, rows):
    print(f"\n{title}")
    print("  (none)" if not rows else "\n".join(f"  {r}" for r in rows))


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--root", help="limit to one root's subtree (merge scope)")
    args = parser.parse_args()

    scoped = bool(args.root)
    beads = load_subtree(args.root) if scoped else load_export()
    edges = edges_of(beads)

    neighbours = collections.defaultdict(set)
    for src, dst, _ in edges:
        neighbours[src].add(dst)
        neighbours[dst].add(src)

    # --- OBJECTIVE ---------------------------------------------------------

    # Dangling targets are only meaningful against the whole graph; in a subtree
    # a "missing" target is usually just outside the scope.
    if not scoped:
        report("Dangling dependency targets", [
            f"{s} -[{t}]-> {d}  (target missing)"
            for s, d, t in edges if d not in beads
        ])

    at_risk = []
    for bead in beads.values():
        if bead.get("status") != "closed":
            continue  # pinned and open beads are not purge-eligible
        carried = []
        if bead.get("comment_count") or bead.get("comments"):
            n = bead.get("comment_count") or len(bead.get("comments") or [])
            carried.append(f"{n} comment(s)")
        if (bead.get("design") or "").strip():
            carried.append("design text")
        if carried:
            at_risk.append(f"{bead['id']:18} {', '.join(carried):22} {bead['title'][:44]}")
    report("Rationale on closed beads (lost at next purge -- pin to keep)", at_risk)
    if scoped:
        print("  note: subtree scope cannot see `design` text; comment counts only")

    # --- HEURISTIC ---------------------------------------------------------

    # A rejected alternative's neighbour is the decision that beat it, which is not
    # itself rejected -- so only the chosen side of a pair is worth checking here.
    lonely = [
        f"{b['id']:18} {b['title'][:50]}"
        for b in beads.values()
        if b.get("issue_type") == "decision"
        and (b.get("metadata") or {}).get("rejected") is not True
        and not any((beads[n].get("metadata") or {}).get("rejected") is True
                    for n in neighbours[b["id"]] if n in beads)
    ]
    report("Decisions with no rejected alternative recorded (suspect)", lonely)

    # `bd children --json` carries a `parent` field; `bd export` does not -- there
    # the hierarchy lives only in parent-child edges, which point child -> parent.
    parent_of = {b: bead["parent"] for b, bead in beads.items() if bead.get("parent")}
    for src, dst, typ in edges:
        if typ == "parent-child":
            parent_of.setdefault(src, dst)

    children = collections.defaultdict(list)
    for bid, parent in parent_of.items():
        children[parent].append(beads[bid])

    roots = [args.root] if scoped else [
        b for b in beads if b not in parent_of and children.get(b)
    ]

    thin = []
    for root in roots:
        if root not in beads:
            continue
        kids = children.get(root, [])
        kinds = collections.Counter(k.get("issue_type") for k in kids)
        linked = [beads[n] for n in neighbours[root] if n in beads]
        flags = []
        if not any(x.get("issue_type") == "record" for x in kids + linked):
            flags.append("no record")
        if kinds.get("task", 0) >= 3 and not kinds.get("decision"):
            flags.append(f"{kinds['task']} tasks, 0 decisions")
        if flags:
            thin.append(f"{root:18} {', '.join(flags):26} {beads[root]['title'][:34]}")
    report("Roots that look thin (a question, not a verdict)", thin)

    scope = f"subtree of {args.root}" if scoped else "whole database"
    print(f"\n{len(beads)} beads, {len(edges)} edges, {len(roots)} root(s)  [{scope}]")


if __name__ == "__main__":
    main()
