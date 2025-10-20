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
 * @fileoverview MCP tool handlers for Gremlin graph database operations.
 *
 * Registers MCP tools that expose Gremlin functionality including status checks,
 * schema introspection, and query execution. Uses Effect-based dependency injection
 * for service access.
 */

import { Effect, Runtime, pipe } from 'effect';
import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { z } from 'zod';
import { TOOL_NAMES } from '../constants.js';
import { GremlinService } from '../gremlin/service.js';
import { createToolEffect, createStringToolEffect, createQueryEffect } from './tool-patterns.js';

/**
 * Input validation schemas for tool parameters.
 */

// Parameterless tools: strict empty object
const emptyInputSchema = z.object({}).strict();

// Run Gremlin Query input
const runQueryInputSchema = z.object({
  query: z
    .string()
    .min(1, 'The Gremlin query cannot be empty')
    .max(10000, 'The Gremlin query cannot exceed 10,000 characters')
    .refine(q => q.trim().startsWith('g.'), {
      message: 'The Gremlin query must start with "g."',
    })
    .describe('The Gremlin query to execute'),
});

/**
 * Registers all MCP tool handlers with the server.
 *
 * @param server - MCP server instance
 * @param runtime - Effect runtime with Gremlin service
 *
 * Registers tools for:
 * - Graph status monitoring
 * - Schema introspection and caching
 * - Query execution
 */
export function registerEffectToolHandlers(
  server: McpServer,
  runtime: Runtime.Runtime<GremlinService>
): void {
  // Get Graph Status
  server.registerTool(
    TOOL_NAMES.GET_GRAPH_STATUS,
    {
      title: 'Get Graph Status',
      description: 'Get the connection status of the Gremlin graph database',
      inputSchema: emptyInputSchema.shape,
    },
    () =>
      Effect.runPromise(
        pipe(
          createStringToolEffect(
            Effect.andThen(GremlinService, service =>
              Effect.map(service.getStatus, statusObj => statusObj.status)
            ),
            'Connection status check failed'
          ),
          Effect.provide(runtime)
        )
      )
  );

  // Get Graph Schema
  server.registerTool(
    TOOL_NAMES.GET_GRAPH_SCHEMA,
    {
      title: 'Get Graph Schema',
      description:
        'Get the complete schema of the graph including vertex labels, edge labels, and relationship patterns',
      inputSchema: emptyInputSchema.shape,
    },
    () =>
      Effect.runPromise(
        pipe(
          createToolEffect(
            Effect.andThen(GremlinService, service => service.getSchema),
            'Schema retrieval failed'
          ),
          Effect.provide(runtime)
        )
      )
  );

  // Refresh Schema Cache
  server.registerTool(
    TOOL_NAMES.REFRESH_SCHEMA_CACHE,
    {
      title: 'Refresh Schema Cache',
      description: 'Force an immediate refresh of the graph schema cache',
      inputSchema: emptyInputSchema.shape,
    },
    () =>
      Effect.runPromise(
        pipe(
          createStringToolEffect(
            Effect.andThen(GremlinService, service =>
              Effect.map(service.refreshSchemaCache, () => 'Schema cache refreshed successfully.')
            ),
            'Failed to refresh schema'
          ),
          Effect.provide(runtime)
        )
      )
  );

  // Run Gremlin Query
  server.registerTool(
    TOOL_NAMES.RUN_GREMLIN_QUERY,
    {
      title: 'Run Gremlin Query',
      description: 'Execute a Gremlin traversal query against the graph database',
      inputSchema: runQueryInputSchema.shape,
    },
    (args: unknown) => {
      const { query } = runQueryInputSchema.parse(args);
      return Effect.runPromise(pipe(createQueryEffect(query), Effect.provide(runtime)));
    }
  );
}
