#!/usr/bin/env bash
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

# Merge work forward through the release branches toward master, testing each branch, and stop
# before pushing.
#
# Usage:
#   merge-forward.sh <source> [--test default|integration|skip | --test-command <cmd>]
#                    [--onto <branch>] [--remote <name>]
#   merge-forward.sh --continue
#   merge-forward.sh --abort
#
# <source> is one of:
#   #1234 or pr:1234   a pull request, fetched to the local branch pr-1234
#   <release-branch>   commits already on a local release branch (e.g. 3.7-dev)
#   <branch-or-ref>    any other branch or commit, such as a contributor's feature branch
#
# Exit codes:
#   0  every branch merged and tested; push command printed
#   1  refused or failed before changing anything, or bad usage
#   2  merge conflict; resolve, commit, then --continue
#   3  test failure; fix and commit on the branch, then --continue
#
# --test-command runs the given shell command from the repository root on every branch in place of
# the --test presets. A non-zero exit counts as a test failure.
#
# A JDK can be pinned per branch with: git config tinker-merge.<branch>.javaHome <path>
#
# The whole script runs inside main, which bash parses before executing, because the checkouts it
# performs can replace this file on disk while it runs.

set -uo pipefail

# Maintained release branches, oldest first. Keep in step with the Branches section of
# docs/src/dev/developer/for-committers.asciidoc.
RELEASE_BRANCHES=("3.7-dev" "3.8-dev" "master")

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m'

info() { echo -e "${GREEN}==>${NC} $1"; }
warn() { echo -e "${YELLOW}==>${NC} $1"; }
die()  { echo -e "${RED}==> $1${NC}" >&2; exit "${2:-1}"; }

usage() {
    sed -n '/^# Usage:/,/^# A JDK/p' "$0" | sed 's/^# \{0,1\}//' | sed '$d'
    exit 1
}

state_dir()  { echo "$(git rev-parse --git-dir)/tinker-merge"; }
state_file() { echo "$(state_dir)/state"; }

save_state() {
    local f
    f="$(state_file)"
    {
        printf 'SOURCE_DESC=%q\n' "$SOURCE_DESC"
        printf 'SOURCE_REF=%q\n' "$SOURCE_REF"
        printf 'TEST_CMD=%q\n' "$TEST_CMD"
        printf 'REMOTE=%q\n' "$REMOTE"
        printf 'ORIG_BRANCH=%q\n' "$ORIG_BRANCH"
        printf 'CHAIN=(%s)\n' "$(printf '%q ' "${CHAIN[@]}")"
        printf 'START_SHAS=(%s)\n' "$(printf '%q ' "${START_SHAS[@]}")"
        printf 'FINAL_SHAS=(%s)\n' "$(printf '%q ' "${FINAL_SHAS[@]}")"
        printf 'NEXT=%q\n' "$NEXT"
    } > "$f"
}

load_state() {
    [[ -f "$(state_file)" ]] || die "No merge in progress."
    # shellcheck disable=SC1090
    source "$(state_file)"
}

require_clean_tree() {
    [[ -f "$(git rev-parse --git-dir)/MERGE_HEAD" ]] && die "A merge is still in progress. Resolve and commit it first."
    git diff --quiet && git diff --cached --quiet || die "Tracked files have uncommitted changes. Commit or stash them first."
}

is_release_branch() {
    local b
    for b in "${RELEASE_BRANCHES[@]}"; do [[ "$b" == "$1" ]] && return 0; done
    return 1
}

release_index() {
    local i
    for i in "${!RELEASE_BRANCHES[@]}"; do
        [[ "${RELEASE_BRANCHES[$i]}" == "$1" ]] && { echo "$i"; return 0; }
    done
    return 1
}

# The remote whose URL points at apache/tinkerpop, unless --remote named one.
detect_remote() {
    local matches
    matches=$(git remote -v | awk '$2 ~ /apache\/tinkerpop(\.git)?$/ && $3 == "(fetch)" {print $1}' | sort -u)
    [[ -n "$matches" ]] || die "No remote points at apache/tinkerpop. Name one with --remote."
    [[ $(echo "$matches" | wc -l) -eq 1 ]] || die "Several remotes point at apache/tinkerpop ($(echo $matches)). Name one with --remote."
    echo "$matches"
}

test_command() {
    case "$1" in
        default)     echo "mvn clean install" ;;
        integration) echo "mvn clean install -DskipIntegrationTests=false" ;;
        skip)        echo "" ;;
    esac
}

java_home_for() {
    git config --get "tinker-merge.$1.javaHome" || echo "${JAVA_HOME:-}"
}

# Prints the release branch a feature branch was cut from: the one whose merge-base with it is
# newest. A tie means the fork point is on the older branch and was merged forward from there, so
# the oldest tied branch wins.
infer_start_branch() {
    local src="$1" best="" best_mb="" b mb
    for b in "${RELEASE_BRANCHES[@]}"; do
        mb=$(git merge-base "$src" "$REMOTE/$b" 2>/dev/null) || continue
        if [[ -z "$best" ]]; then
            best="$b"; best_mb="$mb"
        elif [[ "$mb" != "$best_mb" ]] && git merge-base --is-ancestor "$best_mb" "$mb"; then
            best="$b"; best_mb="$mb"
        fi
    done
    [[ -n "$best" ]] || return 1
    echo "$best"
}

# Refuses to start while an earlier run is unfinished or its result has not reached the remote.
check_previous_run() {
    [[ -f "$(state_file)" ]] || return 0
    load_state
    [[ "$NEXT" -ge "${#CHAIN[@]}" ]] || die "A merge of $SOURCE_DESC is still in progress. Run --continue or --abort."
    git fetch -q "$REMOTE" || die "git fetch $REMOTE failed."
    local i
    for i in "${!CHAIN[@]}"; do
        git merge-base --is-ancestor "${FINAL_SHAS[$i]}" "$REMOTE/${CHAIN[$i]}" \
            || die "The merge of $SOURCE_DESC has not been pushed. Push it, or run --abort."
    done
    rm -f "$(state_file)"
}

start() {
    check_previous_run
    local source="" onto="" remote_opt=""
    local test_mode="default" test_cmd_opt=""
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --test)   test_mode="${2:-}"; shift 2 ;;
            --test-command) test_cmd_opt="${2:-}"; shift 2 ;;
            --onto)   onto="${2:-}"; shift 2 ;;
            --remote) remote_opt="${2:-}"; shift 2 ;;
            -h|--help) usage ;;
            -*) die "Unknown option $1" ;;
            *) [[ -z "$source" ]] || die "Only one source may be given."; source="$1"; shift ;;
        esac
    done
    [[ -n "$source" ]] || usage
    [[ "$test_mode" =~ ^(default|integration|skip)$ ]] || die "--test must be default, integration or skip."
    if [[ -n "$test_cmd_opt" ]]; then
        [[ "$test_mode" == "default" ]] || die "Give --test or --test-command, not both."
        TEST_CMD="$test_cmd_opt"
    else
        TEST_CMD=$(test_command "$test_mode")
    fi
    [[ -z "$onto" ]] || is_release_branch "$onto" || die "--onto must be one of: ${RELEASE_BRANCHES[*]}"

    require_clean_tree
    if [[ -n "$remote_opt" ]]; then
        REMOTE="$remote_opt"
    else
        REMOTE=$(detect_remote) || exit 1
    fi
    ORIG_BRANCH=$(git symbolic-ref -q --short HEAD || git rev-parse HEAD)

    info "Fetching $REMOTE"
    git fetch -q "$REMOTE" || die "git fetch $REMOTE failed."

    # Resolve the source to a ref to merge into the first branch, and the branch the chain starts at.
    local start_branch
    SOURCE_REF=""
    if [[ "$source" =~ ^(#|pr:)([0-9]+)$ ]]; then
        local pr="${BASH_REMATCH[2]}" pr_info state head title
        command -v gh >/dev/null || die "The gh CLI is needed to merge a pull request."
        pr_info=$(gh pr view "$pr" --repo apache/tinkerpop --json state,baseRefName,headRefOid,title \
            --jq '[.state, .baseRefName, .headRefOid, .title] | @tsv') || die "Could not read pull request #$pr."
        IFS=$'\t' read -r state start_branch head title <<< "$pr_info"
        [[ "$state" == "OPEN" ]] || die "Pull request #$pr is $state."
        is_release_branch "$start_branch" || die "Pull request #$pr targets $start_branch, which is not a maintained release branch."
        [[ -z "$onto" || "$onto" == "$start_branch" ]] || die "Pull request #$pr targets $start_branch, not $onto."
        git fetch -q "$REMOTE" "pull/$pr/head:pr-$pr" || die "Could not fetch pull request #$pr into pr-$pr. Does a local pr-$pr branch already hold other commits?"
        [[ "$(git rev-parse "pr-$pr")" == "$head" ]] || die "pr-$pr does not match the pull request head $head."
        SOURCE_REF="pr-$pr"
        SOURCE_DESC="#$pr ($title)"
    elif is_release_branch "$source"; then
        [[ -z "$onto" || "$onto" == "$source" ]] || die "--onto does not apply when the source is the release branch $source."
        start_branch="$source"
        SOURCE_DESC="$source"
    else
        git rev-parse -q --verify "$source^{commit}" >/dev/null || die "$source is not a branch or commit."
        if [[ -n "$onto" ]]; then
            start_branch="$onto"
        else
            start_branch=$(infer_start_branch "$source") \
                || die "Cannot tell which release branch $source was cut from. Name it with --onto."
        fi
        git merge-base --is-ancestor "$source" "$REMOTE/$start_branch" \
            && die "$source is already on $REMOTE/$start_branch."
        SOURCE_REF="$source"
        SOURCE_DESC="$source"
    fi

    local first i b
    first=$(release_index "$start_branch")
    CHAIN=("${RELEASE_BRANCHES[@]:$first}")

    # Bring every branch in the chain level with the remote. Only the start branch may hold local
    # commits the remote lacks, and only when it is itself the source.
    # Every check runs before any branch moves, so a refusal leaves the clone as it was.
    for i in "${!CHAIN[@]}"; do
        b="${CHAIN[$i]}"
        git rev-parse -q --verify "$REMOTE/$b" >/dev/null || die "$REMOTE/$b does not exist."
        git rev-parse -q --verify "refs/heads/$b" >/dev/null || continue
        if ! git merge-base --is-ancestor "$b" "$REMOTE/$b" && ! git merge-base --is-ancestor "$REMOTE/$b" "$b"; then
            die "$b and $REMOTE/$b have diverged. Reconcile them first."
        fi
        if [[ -n "$(git rev-list "$REMOTE/$b..$b")" ]] && ! [[ "$i" -eq 0 && -z "$SOURCE_REF" ]]; then
            die "$b has local commits that are not on $REMOTE/$b. Push or remove them first."
        fi
    done
    START_SHAS=()
    for b in "${CHAIN[@]}"; do
        if [[ "$b" == "$(git symbolic-ref -q --short HEAD)" ]]; then
            git merge -q --ff-only "$REMOTE/$b" >/dev/null || die "Could not fast-forward $b."
        elif ! git rev-parse -q --verify "refs/heads/$b" >/dev/null || git merge-base --is-ancestor "$b" "$REMOTE/$b"; then
            git branch -q -f "$b" "$REMOTE/$b" || die "Could not update $b."
        fi
        START_SHAS+=("$(git rev-parse "$b")")
    done

    FINAL_SHAS=()
    NEXT=0
    mkdir -p "$(state_dir)"
    save_state

    info "Merging $SOURCE_DESC through: ${CHAIN[*]}"
    if [[ -n "$SOURCE_REF" ]]; then
        info "Commits to merge into ${CHAIN[0]}:"
        git log --oneline --no-decorate "${CHAIN[0]}..$SOURCE_REF"
    else
        info "Local commits on ${CHAIN[0]} to merge forward:"
        git log --oneline --no-decorate "$REMOTE/${CHAIN[0]}..${CHAIN[0]}"
    fi
    if [[ -z "$TEST_CMD" ]]; then
        warn "Testing skipped"
    else
        info "Test command: $TEST_CMD"
        for b in "${CHAIN[@]}"; do
            info "$b tests with JDK: $(java_home_for "$b" | sed 's/^$/(java on PATH)/')"
        done
    fi
    run
}

# Merges and tests from branch NEXT onward. Every step checks whether it has already been done,
# so --continue simply calls this again.
run() {
    local i b from log jh
    for (( i = NEXT; i < ${#CHAIN[@]}; i++ )); do
        b="${CHAIN[$i]}"
        if [[ "$i" -eq 0 ]]; then from="$SOURCE_REF"; else from="${CHAIN[$((i - 1))]}"; fi

        git checkout -q "$b" || die "Could not check out $b."
        if [[ -n "$from" ]] && ! git merge-base --is-ancestor "$from" "$b"; then
            info "Merging $from into $b"
            if ! git merge --no-edit "$from"; then
                warn "Conflicts merging $from into $b:"
                git diff --name-only --diff-filter=U
                die "Resolve the conflicts, commit, then run --continue." 2
            fi
        fi

        if [[ -n "$TEST_CMD" ]]; then
            log="$(state_dir)/$b.log"
            jh=$(java_home_for "$b")
            info "Testing $b: $TEST_CMD (log: $log)"
            if ! ( [[ -n "$jh" ]] && export JAVA_HOME="$jh" PATH="$jh/bin:$PATH"; bash -c "$TEST_CMD" ) > "$log" 2>&1; then
                tail -n 60 "$log"
                die "Tests failed on $b. Fix and commit on $b, then run --continue, or run --abort." 3
            fi
        fi

        NEXT=$((i + 1))
        save_state
    done

    FINAL_SHAS=()
    for b in "${CHAIN[@]}"; do FINAL_SHAS+=("$(git rev-parse "$b")"); done
    save_state

    git checkout -q "$ORIG_BRANCH" 2>/dev/null || true
    info "Done. What each branch now holds beyond $REMOTE:"
    for b in "${CHAIN[@]}"; do
        echo "  $b: $(git rev-list --count "$REMOTE/$b..$b") commit(s)"
    done
    local reversed=()
    for (( i = ${#CHAIN[@]} - 1; i >= 0; i-- )); do reversed+=("${CHAIN[$i]}"); done
    info "Review the result, then push it yourself:"
    echo "  git push --atomic $REMOTE ${reversed[*]}"
    info "To discard it instead: $0 --abort"
}

continue_run() {
    load_state
    require_clean_tree
    [[ "$NEXT" -lt "${#CHAIN[@]}" ]] || die "The merge of $SOURCE_DESC already finished. Push it, or run --abort."
    run
}

abort_run() {
    load_state
    local i
    [[ -f "$(git rev-parse --git-dir)/MERGE_HEAD" ]] && git merge --abort
    git reset -q --hard
    git checkout -q --detach
    for i in "${!CHAIN[@]}"; do
        git branch -q -f "${CHAIN[$i]}" "${START_SHAS[$i]}"
        info "${CHAIN[$i]} restored to ${START_SHAS[$i]:0:10}"
    done
    git checkout -q "$ORIG_BRANCH"
    rm -f "$(state_file)"
}

main() {
    git rev-parse --git-dir >/dev/null 2>&1 || die "Not inside a git repository."
    cd "$(git rev-parse --show-toplevel)" || die "Cannot reach the repository root."
    case "${1:-}" in
        --continue) continue_run ;;
        --abort)    abort_run ;;
        ""|-h|--help) usage ;;
        *)          start "$@" ;;
    esac
}

main "$@"; exit $?
