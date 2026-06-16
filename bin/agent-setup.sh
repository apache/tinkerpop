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

# Set up AI coding agent integration with TinkerPop's Agent Skills.
#
# TinkerPop maintains Agent Skills in .skills/:
#   tinker-dev    - Development guidance (coding conventions, build recipes, etc.)
#   tinker-review - Graph-based PR review (knowledge graph analysis, playbooks, etc.)
#
# Different AI coding tools discover skills in different directories. This script
# creates the necessary symlinks or shims so your tool can find the skills.
#
# Usage:
#   bin/agent-setup.sh <agent>
#   bin/agent-setup.sh --list
#   bin/agent-setup.sh --all
#
# Examples:
#   bin/agent-setup.sh claude       # Set up for Claude Code
#   bin/agent-setup.sh kiro         # Set up for Kiro
#   bin/agent-setup.sh --all        # Set up for all supported agents
#
# Supported agents:
#   claude    - Claude Code (.claude/skills/)
#   copilot   - GitHub Copilot (.github/skills/ and .agents/skills/)
#   cursor    - Cursor (.cursor/skills/)
#   codex     - OpenAI Codex (.codex/skills/)
#   junie     - JetBrains Junie (.junie/skills/)
#   kiro      - Kiro (.kiro/skills/)

set -uo pipefail

SKILLS=("tinker-dev" "tinker-review")

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m'

ok()   { echo -e "  ${GREEN}✓${NC} $1"; }
skip() { echo -e "  ${YELLOW}○${NC} $1"; }
bad()  { echo -e "  ${RED}✗${NC} $1"; }

usage() {
    echo "Usage: bin/agent-setup.sh <agent|--list|--all>"
    echo ""
    echo "Agents: claude, copilot, cursor, codex, junie, kiro"
    echo ""
    echo "Options:"
    echo "  --list    List supported agents and their skill discovery paths"
    echo "  --all     Set up shims for all supported agents"
    echo "  --help    Show this message"
}

# Verify we're in the repo root
if [[ ! -d ".skills/tinker-dev" ]]; then
    bad "Cannot find .skills/tinker-dev — run this script from the TinkerPop repository root."
    exit 1
fi

# Remove TinkerPop skill entries (symlinks or copies) from a tool's skill
# directory before the current ones are (re)created. Matching only "tinker*"
# keeps any custom skills the user maintains intact. Entries for skills that no
# longer exist (e.g. a renamed "tinkerpop-dev") are reported as stale removals;
# current skills are cleared silently so the setup step can recreate them
# cleanly — this also fixes symlinks whose relative target path has changed.
purge_tinker_skills() {
    local target_dir="$1"
    [[ -d "$target_dir" ]] || return 0

    local entry name skill is_current
    for entry in "$target_dir"/tinker*; do
        # If the glob matched nothing it stays literal — skip non-existent paths.
        [[ -e "$entry" || -L "$entry" ]] || continue
        name=$(basename "$entry")
        is_current=0
        for skill in "${SKILLS[@]}"; do
            [[ "$name" == "$skill" ]] && is_current=1 && break
        done
        rm -rf "$entry"
        [[ "$is_current" -eq 0 ]] && skip "removed stale skill $target_dir/$name"
    done
}

# Create a symlink from a tool's skill directory to our canonical skill
setup_symlink() {
    local tool_name="$1"
    local target_dir="$2"
    local skill_name="$3"
    local skill_dir=".skills/$skill_name"

    mkdir -p "$target_dir"
    local link_path="$target_dir/$skill_name"

    if [[ -L "$link_path" ]]; then
        skip "$tool_name: symlink already exists at $link_path"
        return 0
    fi

    if [[ -e "$link_path" ]]; then
        bad "$tool_name: $link_path already exists and is not a symlink — skipping"
        return 1
    fi

    # Compute relative path from target_dir to skill_dir
    local rel_path
    rel_path=$(python3 -c "import os.path; print(os.path.relpath('$skill_dir', '$target_dir'))" 2>/dev/null)
    if [[ -z "$rel_path" ]]; then
        rel_path=$(perl -e 'use File::Spec; print File::Spec->abs2rel("'"$skill_dir"'", "'"$target_dir"'")' 2>/dev/null)
    fi
    if [[ -z "$rel_path" ]]; then
        bad "$tool_name: could not compute relative path (need python3 or perl)"
        return 1
    fi

    ln -s "$rel_path" "$link_path"
    ok "$tool_name: created symlink $link_path -> $rel_path"
}

# Kiro doesn't follow symlinks in .kiro/skills/, so we copy the skill directory
# instead. See: https://github.com/kirodotdev/Kiro/issues (symlink support).
setup_kiro() {
    mkdir -p ".kiro/skills"
    purge_tinker_skills ".kiro/skills"
    for skill_name in "${SKILLS[@]}"; do
        local target_dir=".kiro/skills/$skill_name"
        cp -r ".skills/$skill_name" "$target_dir"
        ok "kiro: copied $skill_name to $target_dir"
    done
    echo ""
    echo -e "  ${YELLOW}NOTE:${NC} Kiro uses copies, not symlinks. If you update skills in"
    echo -e "        .skills/, re-run this script to sync the changes."
}

setup_agent() {
    local agent="$1"
    case "$agent" in
        claude)
            purge_tinker_skills ".claude/skills"
            for skill in "${SKILLS[@]}"; do
                setup_symlink "claude" ".claude/skills" "$skill"
            done
            ;;
        copilot)
            purge_tinker_skills ".github/skills"
            purge_tinker_skills ".agents/skills"
            for skill in "${SKILLS[@]}"; do
                setup_symlink "copilot (.github)" ".github/skills" "$skill"
                setup_symlink "copilot (.agents)" ".agents/skills" "$skill"
            done
            ;;
        cursor)
            purge_tinker_skills ".cursor/skills"
            for skill in "${SKILLS[@]}"; do
                setup_symlink "cursor" ".cursor/skills" "$skill"
            done
            ;;
        codex)
            purge_tinker_skills ".codex/skills"
            for skill in "${SKILLS[@]}"; do
                setup_symlink "codex" ".codex/skills" "$skill"
            done
            ;;
        junie)
            purge_tinker_skills ".junie/skills"
            for skill in "${SKILLS[@]}"; do
                setup_symlink "junie" ".junie/skills" "$skill"
            done
            ;;
        kiro)
            setup_kiro
            ;;
        *)
            bad "Unknown agent: $agent"
            echo ""
            usage
            return 1
            ;;
    esac
}

list_agents() {
    echo "Supported agents and their skill discovery paths:"
    echo ""
    echo "  Skills: ${SKILLS[*]}"
    echo ""
    echo "  claude    .claude/skills/<skill>/     -> symlink to .skills/<skill>"
    echo "  copilot   .github/skills/<skill>/     -> symlink to .skills/<skill>"
    echo "            .agents/skills/<skill>/     -> symlink to .skills/<skill>"
    echo "  cursor    .cursor/skills/<skill>/     -> symlink to .skills/<skill>"
    echo "  codex     .codex/skills/<skill>/      -> symlink to .skills/<skill>"
    echo "  junie     .junie/skills/<skill>/      -> symlink to .skills/<skill>"
    echo "  kiro      .kiro/skills/<skill>/       -> copy of .skills/<skill> (re-run to sync)"
}

# --- Main ---

if [[ $# -eq 0 ]]; then
    usage
    exit 1
fi

case "$1" in
    --help|-h)
        usage
        ;;
    --list)
        list_agents
        ;;
    --all)
        echo "Setting up all agent integrations..."
        echo ""
        for agent in claude copilot cursor codex junie kiro; do
            setup_agent "$agent"
        done
        echo ""
        echo "Done. Symlinked directories and generated files are gitignored."
        echo "Add them to .gitignore if they aren't already."
        ;;
    *)
        echo "Setting up $1..."
        echo ""
        setup_agent "$1"
        ;;
esac
