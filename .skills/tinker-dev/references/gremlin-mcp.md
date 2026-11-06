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

Quick reference (validate per the **Definition of Done** table in `SKILL.md`):

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
