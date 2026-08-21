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
"""Record the commits a root bead's work merged as.

Run at merge, as part of the pin step -- and only then. A commit's sha is not
final until the work has landed on a published branch, because squash and rebase
rewrite whatever the branch held before that. TinkerPop merges forward with real
merge commits, so a sha published on 3.7-dev is byte-identical on 3.8-dev and
master, and the sha recorded here stays correct for good.

Creates one `record` bead per landing, under the root:

    title / external-ref   apache/tinkerpop@<tip>   GitHub's owner/repo@sha form,
                                                    the sibling of the PR record
                                                    apache/tinkerpop#3578
    notes                  every sha, full length

The shas go in `notes` because that is the one free-text field bd can query.
`bd search` indexes title and id only, and `bd query` has no field for comments
or design, so a sha recorded anywhere else is unreachable except by grepping an
export. `bd query 'notes=...'` is a substring match, which is why the full
40-character sha is stored: a short sha pasted from `git log --oneline` is a
prefix of it, so both forms of the query find the bead. Quote the sha inside the
query: most shas begin with a digit, and an unquoted one is lexed as a number and
fails to parse, the same way an unquoted version label does.

    bd query 'notes="e1ca7ea3e4"' --all --json   # -> the record, and its parent root

That is the reverse direction, from a line of `git log` back to the reasoning.
The forward direction is `bd show <record>`.

The operator says when work has merged, in the way they would say a JIRA issue is
ready to resolve. `--suggest` answers with the commits that appear to belong to
the root, for them to confirm or correct:

    bin/beads-commits.py --root tp-abc --branch 3.7-dev --suggest

The root's own record beads are what identify them. A JIRA record gives the
ticket id that TinkerPop commit subjects carry, and a PR record gives a number
GitHub can resolve to the commit a squash merge produced. A root with neither
falls back to the commits on the branch since it was created, which is a list to
choose from rather than an answer. Nothing is written in this mode.

Once the operator confirms, the same command records them:

    bin/beads-commits.py --root tp-abc --branch 3.7-dev --pr 3610
    bin/beads-commits.py --root tp-abc --branch 3.7-dev --commits 374b0c76d0 e1ca7ea3e4
    bin/beads-commits.py --root tp-abc --branch 3.7-dev --range 74fe2a64d0..374b0c76d0

What this script guarantees is the part that is easy to get wrong: every sha is
verified reachable from origin/<branch> before anything is written, and a re-run
finds the existing record rather than creating a second one.
"""

import argparse
import json
import re
import subprocess
import sys
from datetime import datetime


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


def as_list(payload):
    """bd sometimes returns a bare object where a list is expected."""
    if isinstance(payload, dict):
        return payload.get("issues", [payload])
    return payload


def repo_slug():
    """owner/repo for the origin remote, so a fork records its own commits."""
    url = git("remote", "get-url", "origin")
    match = re.search(r"[:/]([^/:]+/[^/]+?)(?:\.git)?$", url)
    if not match:
        sys.exit(f"cannot read owner/repo out of origin url: {url}")
    return match.group(1)


def commits_from_pr(number):
    """The commit GitHub recorded for the merge -- one, for a squash merge."""
    proc = subprocess.run(
        ["gh", "pr", "view", str(number), "--json", "mergeCommit,title,url"],
        capture_output=True, text=True)
    if proc.returncode != 0:
        sys.exit(f"gh pr view {number} failed: {proc.stderr.strip()}")
    payload = json.loads(proc.stdout)
    merge_commit = payload.get("mergeCommit") or {}
    if not merge_commit.get("oid"):
        sys.exit(f"PR {number} has no merge commit -- is it merged?")
    return [merge_commit["oid"]]


def bd_show(bead_id):
    """One bead, or None when the id does not resolve."""
    found = as_list(bd("show", bead_id))
    return found[0] if found else None


def pr_merge_commit(number):
    """The merge commit for a PR, or None when it is unmerged or unreachable."""
    proc = subprocess.run(
        ["gh", "pr", "view", str(number), "--json", "mergeCommit"],
        capture_output=True, text=True)
    if proc.returncode != 0:
        return None
    return ((json.loads(proc.stdout).get("mergeCommit") or {}).get("oid")) or None


def suggest(root, branch):
    """Print the commits that look like this root's, for the operator to confirm.

    The root's record beads are the discovery mechanism, which is what PRIME.md
    section 6 means by calling records the link between beads and code. A JIRA
    record names a ticket that TinkerPop commit subjects carry, and a PR record
    names a pull request GitHub resolves to a commit. Neither involves guessing.
    """
    bead = bd_show(root)
    if not bead:
        sys.exit(f"no bead {root}")
    ref = f"origin/{branch}"
    git("rev-parse", "--verify", ref)

    print(f"{root} · {bead.get('title', '')}\n")

    # why[sha] collects every record that points at the same commit
    why = {}
    for child in as_list(bd("children", root)):
        if child.get("issue_type") != "record":
            continue
        external = child.get("external_ref") or child.get("title") or ""
        if re.fullmatch(r"[A-Z]+-\d+", external):
            found = git("log", "--format=%H", f"--grep={external}", ref).splitlines()
        elif re.search(r"#(\d+)$", external):
            oid = pr_merge_commit(re.search(r"#(\d+)$", external).group(1))
            found = [oid] if oid else []
        else:
            continue
        for sha in found:
            why.setdefault(sha, []).append(f"{child['id']} ({external})")

    # The window is shown even when records identified something. A ticket id only
    # finds the commits that quote it, and a follow-up fix or a docs pass usually
    # does not, so returning here would hide exactly the commits most easily lost.
    # Filter on committer date, in Python, because `git log --since` compares the
    # AUTHOR date. A merged pull request carries the contributor's authoring date,
    # often weeks before it landed, so --since would drop the commits this window
    # exists to catch.
    stamp = bead.get("started_at") or bead.get("created_at") or ""
    since = stamp[:10]
    window = []
    if stamp:
        # A day of slack. PRIME.md has the root created before the code, but a root
        # is sometimes opened once work is already under way, and the operator
        # confirms this list. Showing a commit that does not belong costs a glance;
        # hiding one that does costs the link entirely.
        cutoff = datetime.fromisoformat(stamp.replace("Z", "+00:00")).timestamp() - 86400
        for entry in git("log", "--format=%H %ct", "-200", ref).splitlines():
            sha, _, committed = entry.partition(" ")
            if int(committed) >= cutoff:
                window.append(sha)
            if len(window) >= 25:
                break
    mine = (git("config", "user.email") or "").lower()

    def line(sha):
        who = git("log", "-1", "--format=%ae %ce", sha).lower()
        return f"  {sha[:10]}  {subject(sha)}{'   [you]' if mine and mine in who else ''}"

    if why:
        print(f"identified by this root's records, on {ref}:")
        for sha, reasons in why.items():
            print(line(sha))
            print(f"              {', '.join(reasons)}")
    else:
        print("no JIRA or PR record on this root, so nothing identifies its commits directly.")

    # When records already answered, the window is a prompt rather than a list: it
    # exists so a follow-up commit that quotes no ticket is not silently lost. A
    # long-lived root accumulates unrelated work, so showing all of it would bury
    # the answer. When records answered nothing, the window is all there is.
    rest = [sha for sha in window if sha not in why]
    shown = rest[:5] if why else rest
    if shown:
        print(f"\nalso on {ref} since {since}, identified by nothing:")
        for sha in shown:
            print(line(sha))
        if len(rest) > len(shown):
            print(f"  ... and {len(rest) - len(shown)} older, "
                  f"'git log {ref} --since={since}' for the rest")
    elif not why:
        print(f"and no commits on {ref} since {since or 'the root was created'}.")
        return

    confirmed = " ".join(sha[:10] for sha in why) if why else "<sha>..."
    print(f"\nconfirm the list with the operator, adding any of the above, then:\n"
          f"  bin/beads-commits.py --root {root} --branch {branch} --commits {confirmed}")


def resolve(args):
    """The commit list, oldest first, in whichever way the caller named it."""
    if args.pr:
        return commits_from_pr(args.pr)
    if args.range:
        listed = git("log", "--format=%H", "--reverse", args.range).splitlines()
        if not listed:
            sys.exit(f"{args.range} names no commits")
        return listed
    return args.commits


def verify(shas, branch):
    """Expand to full length and refuse anything not on the published branch."""
    ref = f"origin/{branch}"
    git("rev-parse", "--verify", ref)  # exits if the branch was never fetched

    verified, unreachable = [], []
    for sha in shas:
        full = git("rev-parse", f"{sha}^{{commit}}")
        reachable = subprocess.run(
            ["git", "merge-base", "--is-ancestor", full, ref],
            capture_output=True, text=True).returncode == 0
        (verified if reachable else unreachable).append(full)

    if unreachable:
        print(f"not reachable from {ref}:", file=sys.stderr)
        for full in unreachable:
            print(f"  {full[:10]}  {subject(full)}", file=sys.stderr)
        sys.exit(
            f"\nRefusing to record these. A commit that has not landed on {ref} can still\n"
            "be rewritten by a squash or a rebase, and the sha would then point at nothing.\n"
            "Run this once the work has merged. If it has, fetch first (--fetch).")
    return verified


def subject(sha):
    return git("log", "-1", "--format=%s", sha)


def chronological(shas):
    """Oldest first, so the tip is genuinely the last commit to land.

    The tip names the record, so the caller's argument order must not decide it.
    `--suggest` emits in record-discovery order and a human types them in any
    order at all, both of which would otherwise title the record with the wrong
    commit and date the landing to the wrong day.
    """
    return sorted(shas, key=lambda sha: int(git("log", "-1", "--format=%ct", sha)))


def existing_record(root, tip):
    """The record for this landing, if a previous run already wrote it."""
    # The value has to be quoted inside the query. A sha that starts with a digit
    # is otherwise lexed as a number and the parse fails -- the same trap
    # release.asciidoc documents for version labels, and most shas start with one.
    for bead in as_list(bd("query", f'notes="{tip}"', "--all")):
        if bead.get("parent") == root:
            return bead
    return None


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--root", required=True, help="root bead the work hangs off")
    parser.add_argument("--branch", required=True,
                        help="published branch the work merged to, e.g. 3.7-dev")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--pr", type=int, help="pull request number; asks gh for the merge commit")
    source.add_argument("--commits", nargs="+", metavar="SHA", help="the commits, oldest first")
    source.add_argument("--range", metavar="A..B", help="a git revision range")
    source.add_argument("--suggest", action="store_true",
                        help="print the commits that look like this root's; writes nothing")
    parser.add_argument("--fetch", action="store_true",
                        help="refresh origin/<branch> first; otherwise nothing touches the network")
    parser.add_argument("--dry-run", action="store_true", help="print the record, write nothing")
    args = parser.parse_args()

    if args.fetch:
        git("fetch", "origin", args.branch)

    if args.suggest:
        suggest(args.root, args.branch)
        return

    shas = chronological(verify(resolve(args), args.branch))
    tip = shas[-1]
    slug = repo_slug()
    ref = f"{slug}@{tip[:10]}"

    merged_on = git("log", "-1", "--format=%cs", tip)
    lines = [f"merged {args.branch} {merged_on}"]
    lines += [f"  {sha}  {subject(sha)}" for sha in shas]
    notes = "\n".join(lines)

    found = existing_record(args.root, tip)
    if found:
        print(f"{found['id']} already records this landing under {args.root}; nothing to do")
        return

    print(f"{ref}\n{notes}\n")
    if args.dry_run:
        print(f"--dry-run: would create a record under {args.root} and pin it")
        return

    created = as_list(bd("create", "--type=record", f"--parent={args.root}",
                         f"--title={ref}", f"--external-ref={ref}", f"--notes={notes}"))
    record = created[0]["id"]
    # Records are created pinned (PRIME.md section 6), but `bd create` has no
    # status flag, so pinning is a second call.
    bd("update", record, "-s", "pinned")
    print(f"created {record} under {args.root}, pinned, {len(shas)} commit(s)")


if __name__ == "__main__":
    main()
