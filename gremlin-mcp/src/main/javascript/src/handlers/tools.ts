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
 * schema introspection, query execution, and data import/export operations.
 * Uses Effect-based dependency injection for service access.
 */

import { Effect, Runtime, pipe } from 'effect';
import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { z } from 'zod';
import { TOOL_NAMES } from '../constants.js';
import { GremlinService } from '../gremlin/service.js';
import { exportSubgraph } from '../utils/data-operations.js';
import {
  createToolEffect,
  createStringToolEffect,
  createQueryEffect,
  createValidatedToolEffect,
} from './tool-patterns.js';

/**
 * Input validation schemas for tool parameters.
 */

const exportInputBase = z.object({
  traversal_query: z
    .string()
    .min(1, 'traversal_query must not be empty')
    .max(10000, 'traversal_query is too long')
    .describe(
      'Gremlin traversal query to define the subgraph that will normally use the subgraph() step to gather data'
    ),
  format: z
    .enum(['graphson', 'json', 'csv'])
    .default('graphson')
    .describe('The output format for the exported data'),
  include_properties: z
    .array(z.string().min(1))
    .optional()
    .describe('Properties to include in the export'),
  exclude_properties: z
    .array(z.string().min(1))
    .optional()
    .describe('Properties to exclude from the export'),
});

const exportInputSchema = exportInputBase.refine(
  ({ include_properties, exclude_properties }) => {
    if (!include_properties || !exclude_properties) return true;
    const s = new Set(include_properties);
    return !exclude_properties.some(p => s.has(p));
  },
  { message: 'include_properties and exclude_properties must not overlap' }
);

// Parameterless tools: strict empty object
const emptyInputSchema = z.object({}).strict();

// Run Gremlin Query input
const runQueryInputSchema = z.object({
  query: z.string().min(1).max(10000).describe('The Gremlin query to execute'),
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
 * - Data export operations
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

  // Export Subgraph
  server.registerTool(
    TOOL_NAMES.EXPORT_SUBGRAPH,
    {
      title: 'Export Subgraph',
      description: 'Export a subgraph based on a traversal query to various formats',
      inputSchema: exportInputBase.shape,
    },
    (args: unknown) =>
      Effect.runPromise(
        pipe(
          createValidatedToolEffect(
            exportInputSchema,
            rawInput => {
              const input = exportInputBase.parse(rawInput);
              return Effect.andThen(GremlinService, service => exportSubgraph(service, input));
            },
            'Export Subgraph'
          )(args),
          Effect.provide(runtime)
        )
      )
  );
}
