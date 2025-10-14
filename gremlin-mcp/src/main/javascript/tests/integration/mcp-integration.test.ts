/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

/**
 * Integration tests for Gremlin MCP Server with external Gremlin server.
 *
 * These tests validate the server's ability to connect to and interact with
 * a real Gremlin-compatible server instance.
 *
 * Prerequisites:
 * - A running Gremlin-compatible server (e.g., Apache TinkerPop, Gremlin Server, Neptune, etc.)
 * - GREMLIN_ENDPOINT environment variable set
 *
 * Usage:
 *   npm run test:it
 *   GREMLIN_ENDPOINT=localhost:8182/g npm run test:it
 */

import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';
import { TOOL_NAMES } from '../../src/constants';
import { type ContentBlock } from '@modelcontextprotocol/sdk/types.js';

interface TestCallToolResult {
  content: ContentBlock[];
}

describe('MCP Server Integration Tests', () => {
  let client: Client;
  let transport: StdioClientTransport;
  let schemaCache: any = null; // Cache schema to avoid multiple expensive calls

  beforeAll(async () => {
    if (!process.env.GREMLIN_ENDPOINT) {
      console.warn('⚠️  GREMLIN_ENDPOINT not set, skipping MCP integration tests');
      return;
    }

    // Filter out undefined values from process.env for the child process
    const env: Record<string, string> = {};
    for (const key in process.env) {
      const value = process.env[key];
      if (typeof value === 'string') {
        env[key] = value;
      }
    }

    transport = new StdioClientTransport({
      command: 'npx',
      args: ['tsx', 'src/server.ts'],
      env,
    });

    client = new Client({
      name: 'mcp-integration-test-client',
      version: '1.0.0',
    });

    await client.connect(transport);

    // Initialize schema cache once for all tests
    if (process.env.GREMLIN_ENDPOINT) {
      try {
        const result = (await client.callTool({
          name: TOOL_NAMES.GET_GRAPH_SCHEMA,
          arguments: {},
        })) as TestCallToolResult;

        if (result.content[0].type === 'text') {
          schemaCache = JSON.parse(result.content[0].text);
        }
      } catch (error) {
        console.warn('Failed to initialize schema cache:', error);
      }
    }
  }, 600000);

  afterAll(async () => {
    // Close client first
    if (client) {
      try {
        await client.close();
      } catch (error) {
        console.warn('Error closing client:', error);
      }
    }

    // Then explicitly close transport to ensure child process is terminated
    if (transport) {
      try {
        await transport.close();
      } catch (error) {
        console.warn('Error closing transport:', error);
      }
    }

    // Give a small delay to ensure all handles are properly closed
    await new Promise(resolve => setTimeout(resolve, 200));
  }, 10000);

  const itif = (condition: any) => (condition ? it : it.skip);

  itif(process.env.GREMLIN_ENDPOINT)(
    'should list available tools',
    async () => {
      const response = await client.listTools();
      expect(response.tools).toHaveLength(4);
      const toolNames = response.tools.map(t => t.name);
      expect(toolNames).toContain(TOOL_NAMES.GET_GRAPH_STATUS);
      expect(toolNames).toContain(TOOL_NAMES.GET_GRAPH_SCHEMA);
      expect(toolNames).toContain(TOOL_NAMES.RUN_GREMLIN_QUERY);
      expect(toolNames).toContain(TOOL_NAMES.REFRESH_SCHEMA_CACHE);
    },
    30000
  );

  itif(process.env.GREMLIN_ENDPOINT)(
    'should get graph status',
    async () => {
      const result = (await client.callTool({
        name: TOOL_NAMES.GET_GRAPH_STATUS,
        arguments: {},
      })) as TestCallToolResult;

      const content = result.content[0];
      expect(content.type).toBe('text');
      if (content.type === 'text') {
        expect(content.text).toBe('connected');
      }
    },
    30000
  );

  itif(process.env.GREMLIN_ENDPOINT)(
    'should get graph schema and validate structure',
    async () => {
      // Use cached schema if available, otherwise fetch it
      let schema = schemaCache;
      if (!schema) {
        const result = (await client.callTool({
          name: TOOL_NAMES.GET_GRAPH_SCHEMA,
          arguments: {},
        })) as TestCallToolResult;

        const content = result.content[0];
        expect(content.type).toBe('text');
        if (content.type === 'text') {
          schema = JSON.parse(content.text);
        }
      }

      expect(schema).toBeDefined();
      expect(schema.vertices).toBeDefined();
      expect(schema.edges).toBeDefined();
      expect(Array.isArray(schema.vertices)).toBe(true);
      expect(Array.isArray(schema.edges)).toBe(true);

      // Verify edge_patterns is present and adjacency_list is removed
      expect(schema.edge_patterns).toBeDefined();
      expect(Array.isArray(schema.edge_patterns)).toBe(true);
      expect(schema.adjacency_list).toBeUndefined();

      // Verify metadata structure
      if (schema.metadata) {
        expect(schema.metadata.vertex_count).toBeGreaterThanOrEqual(0);
        expect(schema.metadata.edge_count).toBeGreaterThanOrEqual(0);
        expect(schema.metadata.optimization_settings).toBeDefined();
      }
    },
    60000
  );

  itif(process.env.GREMLIN_ENDPOINT)(
    'should handle invalid Gremlin query',
    async () => {
      const query = 'g.invalidSyntax()';
      const result = (await client.callTool({
        name: TOOL_NAMES.RUN_GREMLIN_QUERY,
        arguments: { query },
      })) as TestCallToolResult;

      const content = result.content[0];
      expect(content.type).toBe('text');
      if (content.type === 'text') {
        const queryResult = JSON.parse(content.text);
        expect(queryResult).toBeDefined();
        expect(queryResult.message).toContain('Query failed');
        expect(queryResult.results).toHaveLength(0);
      }
    },
    30000
  );

  itif(process.env.GREMLIN_ENDPOINT)(
    'should perform basic graph operations',
    async () => {
      // Generate a unique ID for this test run to avoid data collisions
      const testRunId = `test-${Date.now()}-${Math.random().toString(36).slice(2, 11)}`;
      const createdVertexIds: string[] = [];

      try {
        // Create a test vertex
        const createQuery = `g.addV('testPerson')
        .property('testRunId', '${testRunId}')
        .property('name', 'Test User')
        .property('age', 30)`;

        const createResult = (await client.callTool({
          name: TOOL_NAMES.RUN_GREMLIN_QUERY,
          arguments: { query: createQuery },
        })) as TestCallToolResult;

        const createContent = JSON.parse(createResult.content[0].text as string);
        expect(createContent.results).toHaveLength(1);
        const vertexId = createContent.results[0].id;
        createdVertexIds.push(vertexId);

        // Query the created vertex
        const queryResult = (await client.callTool({
          name: TOOL_NAMES.RUN_GREMLIN_QUERY,
          arguments: {
            query: `g.V().has('testRunId', '${testRunId}').valueMap()`,
          },
        })) as TestCallToolResult;

        const queryContent = JSON.parse(queryResult.content[0].text as string);
        expect(queryContent.results).toHaveLength(1);
        expect(queryContent.results[0].name).toEqual(['Test User']);
        expect(queryContent.results[0].age).toEqual([30]);
      } finally {
        // Cleanup: Remove test vertices
        for (const vertexId of createdVertexIds) {
          try {
            await client.callTool({
              name: TOOL_NAMES.RUN_GREMLIN_QUERY,
              arguments: { query: `g.V('${vertexId}').drop()` },
            });
          } catch (error) {
            console.warn(`Failed to cleanup vertex ${vertexId}:`, error);
          }
        }
      }
    },
    60000
  );

  itif(process.env.GREMLIN_ENDPOINT)(
    'should refresh schema cache only when needed',
    async () => {
      // This test verifies that schema refresh works but doesn't call it unnecessarily
      const result = (await client.callTool({
        name: TOOL_NAMES.REFRESH_SCHEMA_CACHE,
        arguments: {},
      })) as TestCallToolResult;

      const content = result.content[0];
      expect(content.type).toBe('text');
      if (content.type === 'text') {
        expect(content.text).toBe('Schema cache refreshed successfully.');
      }
    },
    60000
  );
});
