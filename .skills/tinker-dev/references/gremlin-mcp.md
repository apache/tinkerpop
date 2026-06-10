<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Gremlin MCP Server

The Gremlin MCP server (`gremlin-mcp`) is an MCP (Model Context Protocol) server that
enables AI assistants to interact with Apache TinkerPop-compatible graph databases. It
lives at `gremlin-js/gremlin-mcp/` and is published as the `gremlin-mcp` npm package.

## When to Use

Use gremlin-mcp when you need to:
- **Translate Gremlin queries** between language variants (JavaScript, Python, Go, .NET,
  Java, Groovy, canonical, anonymized)
- **Format Gremlin queries** using gremlint for consistent style
- **Query a graph database** through natural language via an AI assistant
- **Discover graph schema** including vertex/edge labels, properties, and enum values

## Available Tools

| Tool                         | Requires Server | Purpose                                    |
|------------------------------|-----------------|--------------------------------------------|
| `translate_gremlin_query`    | No              | Translate Gremlin between language variants |
| `format_gremlin_query`       | No              | Format a Gremlin query with gremlint       |
| `get_graph_status`           | Yes             | Check database connectivity                |
| `get_graph_schema`           | Yes             | Get complete graph structure                |
| `run_gremlin_query`          | Yes             | Execute a Gremlin traversal                |
| `refresh_schema_cache`       | Yes             | Force refresh of cached schema             |

## Offline Mode

When `GREMLIN_MCP_ENDPOINT` is not set, graph tools are not registered but
`translate_gremlin_query` and `format_gremlin_query` remain fully available. This is
useful for query translation and formatting without a running database.

## Configuration

### MCP Client Configuration (e.g., Claude Desktop, Cursor, Windsurf, Kiro)

Using the published package:
```json
{
  "mcpServers": {
    "gremlin": {
      "command": "npx",
      "args": ["gremlin-mcp"],
      "env": {
        "GREMLIN_MCP_ENDPOINT": "localhost:8182",
        "GREMLIN_MCP_LOG_LEVEL": "info"
      }
    }
  }
}
```

From source (after building):
```json
{
  "mcpServers": {
    "gremlin": {
      "command": "node",
      "args": ["gremlin-js/gremlin-mcp/dist/server.js"],
      "env": {
        "GREMLIN_MCP_ENDPOINT": "localhost:8182",
        "GREMLIN_MCP_LOG_LEVEL": "info"
      }
    }
  }
}
```

For offline mode (translation and formatting only), omit `GREMLIN_MCP_ENDPOINT`.

### With Authentication
```json
{
  "mcpServers": {
    "gremlin": {
      "command": "npx",
      "args": ["gremlin-mcp"],
      "env": {
        "GREMLIN_MCP_ENDPOINT": "your-server.com:8182/g",
        "GREMLIN_MCP_USERNAME": "your-username",
        "GREMLIN_MCP_PASSWORD": "your-password",
        "GREMLIN_MCP_USE_SSL": "true"
      }
    }
  }
}
```

### Environment Variables

| Variable                              | Required | Default | Purpose                          |
|---------------------------------------|----------|---------|----------------------------------|
| `GREMLIN_MCP_ENDPOINT`               | No       | —       | Server endpoint (host:port[/g])  |
| `GREMLIN_MCP_USE_SSL`                | No       | false   | Enable SSL/TLS                   |
| `GREMLIN_MCP_USERNAME`               | No       | —       | Authentication username          |
| `GREMLIN_MCP_PASSWORD`               | No       | —       | Authentication password          |
| `GREMLIN_MCP_IDLE_TIMEOUT`           | No       | 300     | Connection timeout (seconds)     |
| `GREMLIN_MCP_LOG_LEVEL`              | No       | info    | Logging: error, warn, info, debug|
| `GREMLIN_MCP_ENUM_DISCOVERY_ENABLED` | No       | true    | Smart enum detection             |
| `GREMLIN_MCP_ENUM_CARDINALITY_THRESHOLD` | No   | 10      | Max distinct values for enum     |
| `GREMLIN_MCP_ENUM_PROPERTY_DENYLIST` | No       | —       | Properties to exclude from enums |

## Translation

The `translate_gremlin_query` tool supports these targets:
`canonical`, `javascript`, `python`, `go`, `dotnet`, `java`, `groovy`, `anonymized`

The optional `source` parameter controls normalization:
- Omit or `auto` (default): mechanical normalization + LLM normalization via MCP sampling
- `canonical`: skip normalization (input must be canonical Gremlin ANTLR grammar)

## Testing with MCP Inspector

A fast way to test gremlin-mcp after building:
```bash
npx @modelcontextprotocol/inspector \
  node gremlin-js/gremlin-mcp/dist/server.js \
  -e GREMLIN_MCP_ENDPOINT=localhost:8182/g \
  -e GREMLIN_MCP_LOG_LEVEL=info
```

This starts the MCP server and opens a browser-based tool for interacting with it.

## Building gremlin-mcp

See `references/build-javascript.md` for full build instructions. Quick reference:

```bash
# Build only
mvn clean install -pl :gremlin-mcp -DskipTests

# Build and test
mvn clean install -pl :gremlin-mcp

# npm commands (from gremlin-js/gremlin-mcp/)
npm test                    # Unit tests
npm run test:it             # Integration tests (requires server)
npm run lint                # Linting
npm run validate            # All checks
```
