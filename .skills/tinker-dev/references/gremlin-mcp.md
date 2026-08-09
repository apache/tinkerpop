# Gremlin MCP Server

The Gremlin MCP server (`gremlin-mcp`) is an MCP (Model Context Protocol) server that
enables AI assistants to interact with Apache TinkerPop-compatible graph databases. The
Maven module is `gremlin-mcp/` and the npm project inside it lives at
`gremlin-mcp/src/main/javascript/`. It is published as the `gremlin-mcp` npm package.

## When to Use

Use gremlin-mcp when you need to:
- **Query a graph database** through natural language via an AI assistant
- **Discover graph schema** including vertex/edge labels, properties, and relationship patterns
- **Format Gremlin queries** using gremlint for consistent style

## Available Tools

| Tool                     | Purpose                                                              |
|--------------------------|----------------------------------------------------------------------|
| `get_graph_status`       | Get the connection status of the Gremlin graph database              |
| `get_graph_schema`       | Vertex labels, edge labels, and relationship patterns                |
| `run_gremlin_query`      | Execute a Gremlin traversal query against the graph database         |
| `refresh_schema_cache`   | Force an immediate refresh of the graph schema cache                 |
| `format_gremlin_query`   | Format a Gremlin query using gremlint, returning a structured result |

All five register unconditionally. `GREMLIN_MCP_ENDPOINT` is **required** — the server will
not start without it, so there is no offline mode on this branch and no query-translation
tool. Both exist on `master` only; do not describe them as available here.

Configuration is entirely environment-driven (`GREMLIN_MCP_ENDPOINT`, `GREMLIN_MCP_USE_SSL`,
`GREMLIN_MCP_USERNAME` / `_PASSWORD`, `GREMLIN_MCP_LOG_LEVEL`, `GREMLIN_MCP_IDLE_TIMEOUT`, and
the `GREMLIN_MCP_SCHEMA_*` / `GREMLIN_MCP_ENUM_*` schema-discovery settings). See
`src/main/javascript/src/config.ts` and `.env.example` for the full set.

## Testing with MCP Inspector

A fast way to test gremlin-mcp after building:
```bash
npx @modelcontextprotocol/inspector \
  node gremlin-mcp/src/main/javascript/dist/server.js \
  -e GREMLIN_MCP_ENDPOINT=localhost:8182/g \
  -e GREMLIN_MCP_LOG_LEVEL=info
```

This starts the MCP server and opens a browser-based tool for interacting with it.

## Building gremlin-mcp

Quick reference (validate per the **Definition of Done** table in `SKILL.md`):

```bash
# Build only
mvn clean install -pl :gremlin-mcp -DskipTests

# Build and test
mvn clean install -pl :gremlin-mcp

# npm commands (from gremlin-mcp/src/main/javascript/)
npm test                    # Unit tests (jest)
npm run test:it             # Integration tests (expects a server at localhost:8182/g)
npm run lint                # Linting
npm run type-check          # tsc --noEmit
npm run validate            # format, lint, type-check and unit tests
```
