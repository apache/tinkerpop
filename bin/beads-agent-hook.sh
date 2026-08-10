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
# Agent hook logic for TinkerPop's beads workflow.
#
# All of the behaviour lives here so it can be read, reviewed and tested without
# a running agent. The per-tool files under bin/agent-hooks/ are thin wiring that
# name an event and invoke this script; bin/agent-setup.sh --contributor installs
# them. Every hook is advisory -- nothing here blocks an agent.
#
# Usage:
#   bin/beads-agent-hook.sh <event> [--format=claude|plain]
#
# Events:
#   session-start   emit the beads workflow (.beads/PRIME.md, via bd prime)
#   stop            wrap-up checklist, rate limited (see STOP_INTERVAL)
#   prompt-submit   nudge when the operator's prompt reads as a redirect
#
# Run any event by hand to see exactly what an agent would be shown:
#   bin/beads-agent-hook.sh stop
#
set -uo pipefail

STOP_INTERVAL=${TINKERPOP_BEADS_STOP_INTERVAL:-1800}  # seconds; 0 disables limit

usage() {
    awk '/^# Agent hook logic/,/^[^#]/ { if ($0 ~ /^#/) { sub(/^# ?/, ""); print } }' "$0"
    exit "${1:-0}"
}

event=""
format="plain"
for arg in "$@"; do
    case "$arg" in
        --format=*) format="${arg#--format=}" ;;
        -h|--help)  usage 0 ;;
        -*)         echo "unknown option: $arg" >&2; usage 2 ;;
        *)          event="$arg" ;;
    esac
done
[[ -n "$event" ]] || usage 2

# Beads is for committers. A contributor without bd installed, or a directory
# that is not a beads workspace, gets silence rather than an error.
command -v bd >/dev/null 2>&1 || exit 0
bd where >/dev/null 2>&1 || exit 0

# Read the hook payload when one is piped in. Agents send JSON on stdin; running
# this by hand from a terminal must not block waiting for input.
read_stdin_payload() {
    [[ -t 0 ]] && return 0
    cat 2>/dev/null
}

# Pull a field out of the agent's JSON payload, tolerating non-JSON input.
payload_field() {
    local payload="$1" field="$2"
    printf '%s' "$payload" | python3 -c "
import json, sys
raw = sys.stdin.read()
try:
    print(json.loads(raw).get('$field', '') or '')
except Exception:
    print(raw if '$field' == 'user_prompt' else '')
" 2>/dev/null
}

# Stop fires after every agent turn, so an unconditional checklist would be
# noise. Fire at most once per STOP_INTERVAL per workspace. State lives in the
# temp dir rather than the repo so nothing is left behind to commit.
stop_is_due() {
    [[ "$STOP_INTERVAL" -eq 0 ]] && return 0
    local key stamp now last
    key=$(printf '%s' "$PWD" | cksum | cut -d' ' -f1)
    stamp="${TMPDIR:-/tmp}/tinkerpop-beads-stop-$key"
    now=$(date +%s)
    last=$(cat "$stamp" 2>/dev/null || echo 0)
    (( now - last < STOP_INTERVAL )) && return 1
    echo "$now" > "$stamp" 2>/dev/null
    return 0
}

# High-signal redirect language. The cost of a false positive is one extra
# sentence in context, so this errs toward firing.
REDIRECT_RE='(^|[[:space:]])(no,|nope|instead|rather than|we tried|already tried|'\
'that will not work|that won.t work|that breaks|don.t do|do not do|revert|back out|'\
'undo that|wrong approach|not what i)([[:space:]]|[[:punct:]]|$)'

text=""
case "$event" in
    session-start)
        text=$(bd prime 2>/dev/null)
        ;;

    stop)
        stop_is_due || exit 0
        # bd prints "No issues found." rather than nothing when the list is empty.
        active=$(bd list --status=in_progress 2>/dev/null | grep -v '^No issues found' | head -20)
        text="Beads check — did anything get decided this session?

An alternative that was actually considered and rejected belongs in its own decision bead,
marked --metadata '{\"rejected\":true}'. An approach you tried and abandoned counts. Anything
else worth remembering goes in a comment on the root bead."

        # Editing files while nothing is claimed means the in_progress window -- the only
        # thing a later session can read to resume -- is never being written. These are two
        # independent facts, so never let one suppress the other: parallel sessions share
        # one actor, so a populated in_progress list is no evidence *your* work is claimed.
        dirty=$(git status --porcelain 2>/dev/null | head -1)
        if [[ -n "$active" ]]; then
            text="$text

Still in progress:
$active"
            if [[ -n "$dirty" ]]; then
                text="$text

You have uncommitted changes. If what you are working on is not in that list, claim it —
'bd update <id> --claim'. Beads claimed by another session or contributor are not yours."
            fi
        elif [[ -n "$dirty" ]]; then
            text="$text

You have uncommitted changes and NO bead is in_progress anywhere. Claim the bead you are
working on now — 'bd update <id> --claim'. Without it, the next session cannot tell what
was underway or where it stopped."
        fi
        ;;

    prompt-submit)
        prompt=$(payload_field "$(read_stdin_payload)" user_prompt)
        [[ -n "$prompt" ]] || exit 0
        if printf '%s' "$prompt" | tr '[:upper:]' '[:lower:]' | grep -Eq "$REDIRECT_RE"; then
            text="That prompt reads as a redirect. If an alternative was just rejected, create the
decision bead and its rejected sibling now, while the reasoning is exact — operator
redirects are the highest-signal capture trigger there is."
        fi
        ;;

    *)
        echo "unknown event: $event" >&2
        usage 2
        ;;
esac

[[ -n "${text// /}" ]] || exit 0

case "$format" in
    plain)
        printf '%s\n' "$text"
        ;;
    claude)
        case "$event" in
            session-start)  hook_event="SessionStart" ;;
            stop)           hook_event="Stop" ;;
            prompt-submit)  hook_event="UserPromptSubmit" ;;
        esac
        HOOK_EVENT="$hook_event" HOOK_TEXT="$text" python3 -c "
import json, os
print(json.dumps({'hookSpecificOutput': {
    'hookEventName': os.environ['HOOK_EVENT'],
    'additionalContext': os.environ['HOOK_TEXT'],
}}))
"
        ;;
    *)
        echo "unknown format: $format" >&2
        exit 2
        ;;
esac
