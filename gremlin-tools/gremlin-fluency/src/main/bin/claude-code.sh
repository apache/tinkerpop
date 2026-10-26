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
# A gremlin-fluency driver for Claude Code. It reads the prompt on stdin and prints the traversal on stdout, and
# reports the turns and tokens the agent used and the tools it called on stderr.
#
#   MODEL         the model to use, required
#   --mcp         give the agent gremlin-mcp, pointed at FLUENCY_GREMLIN_ENDPOINT
#   GREMLIN_MCP_PACKAGE  which gremlin-mcp --mcp runs: an npm package spec run with npx, such as
#                 gremlin-mcp@4.0.0-beta.3, or the absolute path to a built gremlin-mcp checkout, run with node. It
#                 defaults to the unpinned gremlin-mcp, whose version can change between runs.
#
# The agent gets no built-in tools, so it cannot search the web or read files, and no MCP servers but the one
# passed here. The runner starts it in an empty directory, so no project CLAUDE.md or memory is found. User-level
# settings and CLAUDE.md still load unless ANTHROPIC_API_KEY is set, in which case --bare skips them. Set
# FLUENCY_CLAUDE_ARGS to pass further arguments.

set -euo pipefail

: "${MODEL:?MODEL must name the model}"

# stream-json shows each tool call, which the final result alone does not, and needs --verbose with -p
args=(-p --model "$MODEL" --output-format stream-json --verbose --tools "" --strict-mcp-config)
[[ -n "${ANTHROPIC_API_KEY:-}" ]] && args+=(--bare)

version="claude-code $(claude --version 2>/dev/null || echo unknown)"

if [[ "${1:-}" == "--mcp" ]]; then
  : "${FLUENCY_GREMLIN_ENDPOINT:?--mcp needs FLUENCY_GREMLIN_ENDPOINT}"
  package="${GREMLIN_MCP_PACKAGE:-gremlin-mcp}"
  # the runner starts this script in an empty directory, so a relative path would point nowhere
  [[ "$package" == .* ]] && { echo "GREMLIN_MCP_PACKAGE must be an absolute path: $package" >&2; exit 1; }
  if [[ "$package" == /* ]]; then
    [[ -f "$package/dist/server.js" ]] || { echo "no built gremlin-mcp at $package" >&2; exit 1; }
    command="node"
    command_args="\"$package/dist/server.js\""
    version+="; gremlin-mcp $(node -p "require('$package/package.json').version") at $package"
  else
    command="npx"
    command_args="\"-y\", \"$package\""
    version+="; gremlin-mcp $package"
  fi

  config=$(mktemp)
  trap 'rm -f "$config"' EXIT
  cat > "$config" <<JSON
{"mcpServers": {"gremlin": {"command": "$command", "args": [$command_args], "env": {"GREMLIN_MCP_ENDPOINT": "$FLUENCY_GREMLIN_ENDPOINT"}}}}
JSON
  args+=(--mcp-config "$config" --allowedTools "mcp__gremlin")
fi

# shellcheck disable=SC2206
[[ -n "${FLUENCY_CLAUDE_ARGS:-}" ]] && args+=(${FLUENCY_CLAUDE_ARGS})

echo "fluency-version: $version" >&2
# input tokens count everything the model read, whether or not it came from the prompt cache
claude "${args[@]}" | node -e '
  const tools = [];
  let r = null;
  for (const line of require("fs").readFileSync(0, "utf8").split("\n")) {
    if (!line.trim()) continue;
    const event = JSON.parse(line);
    if (event.type === "assistant")
      for (const c of (event.message && event.message.content) || []) if (c.type === "tool_use") tools.push(c.name);
    if (event.type === "result") r = event;
  }
  if (!r) { console.error("no result from claude"); process.exit(1); }
  const u = r.usage || {};
  const input = (u.input_tokens || 0) + (u.cache_creation_input_tokens || 0) + (u.cache_read_input_tokens || 0);
  console.error(`fluency-usage: input_tokens=${input} output_tokens=${u.output_tokens || 0} turns=${r.num_turns || 0}`);
  console.error(`fluency-tools: ${tools.join(" ")}`);
  if (r.is_error) { console.error(r.result || r.subtype); process.exit(1); }
  process.stdout.write(r.result || "");'
