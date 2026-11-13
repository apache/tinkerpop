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
"""Resolve the PR gates that hold errands back.

An errand (PRIME.md section 8) is a non-blocking review follow-up. The code it
fixes usually exists only on the reviewed PR's branch, so it waits behind a
`gh:pr` gate on that PR, one gate shared by every errand the review raised.
Nothing resolves those gates automatically. Someone runs this:

    bin/beads-gates.py            # report what each open PR gate would do
    bin/beads-gates.py --apply    # and do it

For each open `gh:pr` gate it asks GitHub for the PR's state:

    MERGED   resolve the gate, so its errands appear in `bd ready --type=errand`
    CLOSED   resolve the gate and close every open errand behind it -- the code
             they fix never landed, so there is nothing left to do
    OPEN     leave it

The PR is looked up in the origin remote's repository, the same rule
bin/beads-commits.py uses. `bd gate check` is not used because it runs
`gh pr view <N>` without naming a repository, so from a clone whose gh default is
a fork it would read the fork's PR #N. A bead blocked by a gate that is not an
errand is reported and never closed.

Nothing here pins anything. Errands and their gates are built to be purged.
"""

import argparse
import json
import re
import subprocess
import sys


def git(*args):
    """Run a git command and return its stripped stdout."""
    proc = subprocess.run(["git", *args], capture_output=True, text=True)
    if proc.returncode != 0:
        sys.exit(f"git {' '.join(args)} failed: {proc.stderr.strip()}")
    return proc.stdout.strip()


def bd(*args):
    """Run a bd command with --json and return the parsed result."""
    proc = subprocess.run(["bd", *args, "--json"], capture_output=True, text=True)
    if proc.returncode != 0:
        sys.exit(f"bd {' '.join(args)} failed: {proc.stderr.strip()}")
    if not proc.stdout.strip():
        return []
    return json.loads(proc.stdout)


def bd_write(*args):
    """Run a writing bd command; not every one of them answers --json."""
    proc = subprocess.run(["bd", *args], capture_output=True, text=True)
    if proc.returncode != 0:
        sys.exit(f"bd {' '.join(args)} failed: {proc.stderr.strip()}")


def as_list(payload):
    """bd sometimes returns a bare object where a list is expected."""
    if isinstance(payload, dict):
        return payload.get("issues", [payload])
    return payload


def repo_slug():
    """owner/repo for the origin remote."""
    url = git("remote", "get-url", "origin")
    match = re.search(r"[:/]([^/:]+/[^/]+?)(?:\.git)?$", url)
    if not match:
        sys.exit(f"cannot read owner/repo out of origin url: {url}")
    return match.group(1)


def pr_state(number, slug):
    """MERGED, CLOSED or OPEN, or None when gh cannot answer."""
    proc = subprocess.run(
        ["gh", "pr", "view", str(number), "-R", slug, "--json", "state"],
        capture_output=True, text=True)
    if proc.returncode != 0:
        print(f"  gh could not read {slug}#{number}: {proc.stderr.strip()}")
        return None
    return json.loads(proc.stdout).get("state")


def waiters(gate):
    """Open beads the gate blocks."""
    return [b for b in as_list(bd("dep", "list", gate, "--direction=up"))
            if b.get("dependency_type") == "blocks" and b.get("status") != "closed"]


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--apply", action="store_true",
                        help="resolve and close; without it nothing is written")
    args = parser.parse_args()

    slug = repo_slug()
    gates = [g for g in as_list(bd("gate", "list"))
             if g.get("await_type") == "gh:pr" and g.get("status") != "closed"]
    if not gates:
        print("no open gh:pr gates")
        return

    verb = "" if args.apply else "would "
    for gate in gates:
        number = gate.get("await_id")
        ref = f"{slug}#{number}"
        state = pr_state(number, slug)
        blocked = waiters(gate["id"])
        errands = [b for b in blocked if b.get("issue_type") == "errand"]
        others = [b for b in blocked if b.get("issue_type") != "errand"]
        print(f"{gate['id']}  {ref}  {state or 'UNKNOWN'}  ({len(errands)} errand(s))")

        if state == "MERGED":
            print(f"  {verb}resolve gate; {verb}release: "
                  + (", ".join(e["id"] for e in errands) or "(none)"))
            if args.apply:
                bd_write("gate", "resolve", gate["id"], "--reason", f"{ref} merged")
        elif state == "CLOSED":
            for e in errands:
                print(f"  {verb}close {e['id']}  {e['title'][:60]}")
            # Resolving the gate would release whatever else it holds, which this script
            # has no business deciding, so a gate holding non-errands stays open.
            if others:
                print("  gate left open: it also blocks beads that are not errands")
            # Errands close before the gate resolves, or resolving it would release them
            # into `bd ready` for as long as the gap lasts.
            if args.apply:
                if errands:
                    bd_write("close", "--force", *[e["id"] for e in errands],
                             "--reason", f"origin PR {ref} closed unmerged; the code this fixes never landed")
                if not others:
                    bd_write("gate", "resolve", gate["id"], "--reason", f"{ref} closed unmerged")

        for b in others:
            print(f"  {b['id']} is a {b.get('issue_type')}, not an errand -- left for a human")

    if not args.apply:
        print("\nnothing written; re-run with --apply")


if __name__ == "__main__":
    main()
