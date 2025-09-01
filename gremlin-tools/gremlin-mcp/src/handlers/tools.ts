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
import { importGraphData, exportSubgraph } from '../utils/data-operations.js';
import {
  createToolEffect,
  createStringToolEffect,
  createQueryEffect,
  createValidatedToolEffect,
} from './tool-patterns.js';

/**
 * Input validation schemas for tool parameters.
 */
const importInputSchema = z.object({
  format: z.enum(['graphson', 'csv']),
  data: z.string(),
  options: z
    .object({
      batch_size: z.number().optional(),
      clear_graph: z.boolean().optional(),
      validate_schema: z.boolean().optional(),
    })
    .optional(),
});

const exportInputSchema = z.object({
  traversal_query: z.string(),
  format: z.enum(['graphson', 'json', 'csv']),
  max_depth: z.number().optional(),
  include_properties: z.array(z.string()).optional(),
  exclude_properties: z.array(z.string()).optional(),
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
 * - Data import/export operations
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
      inputSchema: {},
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
      inputSchema: {},
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
      inputSchema: {},
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
      inputSchema: {
        query: z.string().describe('The Gremlin query to execute'),
      },
    },
    (args: unknown) => {
      const { query } = z.object({ query: z.string() }).parse(args);
      return Effect.runPromise(pipe(createQueryEffect(query), Effect.provide(runtime)));
    }
  );

  // Import Graph Data
  server.registerTool(
    TOOL_NAMES.IMPORT_GRAPH_DATA,
    {
      title: 'Import Graph Data',
      description: 'Import graph data from various formats including GraphSON and CSV',
      inputSchema: {
        format: z.enum(['graphson', 'csv']).describe('The format of the data to import'),
        data: z.string().describe('The data content to import'),
        options: z
          .object({
            batch_size: z.number().optional().describe('Number of operations per batch'),
            clear_graph: z
              .boolean()
              .optional()
              .describe('Whether to clear the graph before importing'),
            validate_schema: z
              .boolean()
              .optional()
              .describe('Whether to validate against existing schema'),
          })
          .optional()
          .describe('Import options'),
      },
    },
    (args: unknown) =>
      Effect.runPromise(
        pipe(
          createValidatedToolEffect(
            importInputSchema,
            input => Effect.andThen(GremlinService, service => importGraphData(service, input)),
            'Import Graph Data'
          )(args),
          Effect.provide(runtime)
        )
      )
  );

  // Export Subgraph
  server.registerTool(
    TOOL_NAMES.EXPORT_SUBGRAPH,
    {
      title: 'Export Subgraph',
      description: 'Export a subgraph based on a traversal query to various formats',
      inputSchema: {
        traversal_query: z.string().describe('Gremlin traversal query to define the subgraph'),
        format: z
          .enum(['graphson', 'json', 'csv'])
          .describe('The output format for the exported data'),
        max_depth: z.number().optional().describe('Maximum traversal depth for the subgraph'),
        include_properties: z
          .array(z.string())
          .optional()
          .describe('Properties to include in the export'),
        exclude_properties: z
          .array(z.string())
          .optional()
          .describe('Properties to exclude from the export'),
      },
    },
    (args: unknown) =>
      Effect.runPromise(
        pipe(
          createValidatedToolEffect(
            exportInputSchema,
            input => Effect.andThen(GremlinService, service => exportSubgraph(service, input)),
            'Export Subgraph'
          )(args),
          Effect.provide(runtime)
        )
      )
  );
}
