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
 *  Unless required by applicable law or agreed in writing,
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

import { Effect, Runtime, Option, pipe } from 'effect';
import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { z } from 'zod';
import { TOOL_NAMES } from '../constants.js';
import { GremlinService } from '../gremlin/service.js';
import {
  createToolEffect,
  createStringToolEffect,
  createSuccessResponse,
} from './tool-patterns.js';
import { formatQuery } from 'gremlint';
import { GremlinTranslator } from 'gremlin-language/language';
import { normalizeAndTranslate } from '../translator/index.js';
import type { AppConfigType } from '../config.js';
import {
  markGremlinConnected,
  markGremlinDisconnected,
  isGremlinConnectionError,
} from '../connectivity-state.js';

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

// Format Gremlin Query input - allow any string and expose gremlint options as top-level optional fields
const formatQueryInputSchema = z
  .object({
    query: z.string().describe('The Gremlin query (or any string) to format'),
    // Expose gremlint options as optional top-level fields
    indentation: z.number().int().nonnegative().optional(),
    maxLineLength: z.number().int().positive().optional(),
    shouldPlaceDotsAfterLineBreaks: z.boolean().optional(),
  })
  .strict();

// Translate Gremlin Query input
const TRANSLATE_TARGETS = [
  'canonical',
  'javascript',
  'python',
  'go',
  'dotnet',
  'java',
  'groovy',
  'anonymized',
] as const;
type TranslateTarget = (typeof TRANSLATE_TARGETS)[number];

const toUnknownRecord = (value: unknown): Record<string, unknown> | null =>
  typeof value === 'object' && value !== null ? (value as Record<string, unknown>) : null;

const TARGET_TO_TRANSLATOR_KEY: Record<TranslateTarget, string> = {
  canonical: 'CANONICAL',
  javascript: 'JAVASCRIPT',
  python: 'PYTHON',
  go: 'GO',
  dotnet: 'DOTNET',
  java: 'JAVA',
  groovy: 'GROOVY',
  anonymized: 'ANONYMIZED',
};

const TRANSLATE_SOURCES = [
  'canonical',
  'javascript',
  'python',
  'go',
  'dotnet',
  'java',
  'groovy',
  'auto',
] as const;

const translateQueryInputSchema = z
  .object({
    gremlin: z
      .string()
      .min(1, 'The Gremlin query cannot be empty')
      .describe(
        'The Gremlin query string to translate. When source is omitted or "canonical", must use gremlin-language ANTLR grammar format. For any other source value, the query will be normalized automatically.'
      ),
    target: z.enum(TRANSLATE_TARGETS).describe('The target language to translate into'),
    source: z
      .enum(TRANSLATE_SOURCES)
      .optional()
      .describe(
        'The source dialect of the input query. Omit or use "auto" to normalize automatically via LLM before translating (default behavior). Use "canonical" to skip normalization and translate directly (only if your input is already in canonical gremlin-language ANTLR format).'
      ),
    traversalSource: z
      .string()
      .optional()
      .describe('The traversal source variable name (default: "g")'),
  })
  .strict();

/**
 * Registers MCP tool handlers with the server.
 *
 * Graph tools (status, schema, query) are only registered when a Gremlin endpoint is configured. Utility tools (translate, format) are always registered.
 *
 * @param server - MCP server instance
 * @param runtime - Effect runtime with Gremlin service
 * @param config - Application configuration
 */
export function registerEffectToolHandlers(
  server: McpServer,
  runtime: Runtime.Runtime<GremlinService>,
  config: AppConfigType
): void {
  if (Option.isSome(config.gremlin.endpoint)) {
    registerGraphTools(server, runtime);
  }
  registerUtilityTools(server, runtime);
}

/**
 * Registers graph-connectivity tools (status, schema, query).
 * Only called when a Gremlin endpoint is configured.
 */
function registerGraphTools(server: McpServer, runtime: Runtime.Runtime<GremlinService>): void {
  const withOperationConnectivityTracking = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    pipe(
      effect,
      Effect.tap(() => Effect.sync(markGremlinConnected)),
      Effect.tapError(error =>
        Effect.sync(() => {
          if (isGremlinConnectionError(error)) {
            markGremlinDisconnected();
          }
        })
      )
    );

  const getTrackedStatus = pipe(
    GremlinService,
    Effect.andThen(service => service.getStatus),
    Effect.tap(statusObj =>
      Effect.sync(() => {
        if (statusObj.status === 'connected') {
          markGremlinConnected();
        } else {
          markGremlinDisconnected();
        }
      })
    ),
    Effect.map(statusObj => statusObj.status)
  );

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
          createStringToolEffect(getTrackedStatus, 'Connection status check failed'),
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
            withOperationConnectivityTracking(
              Effect.andThen(GremlinService, service => service.getSchema)
            ),
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
            withOperationConnectivityTracking(
              Effect.andThen(GremlinService, service =>
                Effect.map(service.refreshSchemaCache, () => 'Schema cache refreshed successfully.')
              )
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
      return Effect.runPromise(
        pipe(
          GremlinService,
          Effect.andThen(service => withOperationConnectivityTracking(service.executeQuery(query))),
          Effect.map(createSuccessResponse),
          Effect.catchAll(error => {
            const errorResponse = {
              results: [],
              message: `Query failed: ${error}`,
            };
            return Effect.succeed(createSuccessResponse(errorResponse));
          }),
          Effect.provide(runtime)
        )
      );
    }
  );
}

/**
 * Registers utility tools (translate, format) that do not require a Gremlin server.
 * Always registered regardless of endpoint configuration.
 */
function registerUtilityTools(server: McpServer, runtime: Runtime.Runtime<GremlinService>): void {
  // Translate Gremlin Query
  server.registerTool(
    TOOL_NAMES.TRANSLATE_GREMLIN_QUERY,
    {
      title: 'Translate Gremlin Query',
      description:
        'Translate a Gremlin query into another language variant. By default, automatically normalizes the input via LLM before translating. Set source to "canonical" to skip normalization if the input is already in canonical gremlin-language ANTLR format.',
      inputSchema: translateQueryInputSchema.shape,
    },
    (args: unknown) => {
      const { gremlin, target, source, traversalSource } = translateQueryInputSchema.parse(args);
      const translatorKey = TARGET_TO_TRANSLATOR_KEY[target];
      const tsource = traversalSource ?? 'g';

      if (source === 'canonical') {
        // Approach 1: direct translation, canonical input assumed
        try {
          const result = GremlinTranslator.translate(
            gremlin,
            tsource,
            translatorKey as Parameters<typeof GremlinTranslator.translate>[2]
          );
          return Promise.resolve(
            createSuccessResponse({
              success: true,
              original: result.getOriginal(),
              translated: result.getTranslated(),
              target,
            })
          );
        } catch (error) {
          return Promise.resolve(
            createSuccessResponse({
              success: false,
              error: error instanceof Error ? error.message : String(error),
              target,
            })
          );
        }
      }

      // Approach 2+5: mechanical normalization then LLM normalization via MCP sampling
      return Effect.runPromise(
        pipe(
          Effect.tryPromise(() =>
            normalizeAndTranslate(gremlin, translatorKey, tsource, server.server)
          ),
          Effect.map(result =>
            createSuccessResponse({
              success: true,
              original: result.original,
              normalized: result.normalized,
              translated: result.translated,
              target,
              ...(result.llmNormalizationSkipped && {
                warning:
                  'LLM normalization unavailable; result is based on mechanical normalization only and may be less accurate.',
              }),
            })
          ),
          Effect.catchAll(error =>
            Effect.succeed(
              createSuccessResponse({
                success: false,
                error: error instanceof Error ? error.message : String(error),
                target,
              })
            )
          )
        )
      );
    }
  );

  // Format Gremlin Query (uses local gremlint)
  server.registerTool(
    TOOL_NAMES.FORMAT_GREMLIN_QUERY,
    {
      title: 'Format Gremlin Query',
      description: 'Format a Gremlin query using Gremlint and return a structured result',
      inputSchema: formatQueryInputSchema.shape,
    },
    (args: unknown) => {
      const parsed = formatQueryInputSchema.parse(args);
      const { query, indentation, maxLineLength, shouldPlaceDotsAfterLineBreaks } = parsed;

      // Build options only with fields provided (undefineds will be ignored by gremlint defaults)
      const options =
        indentation !== undefined ||
        maxLineLength !== undefined ||
        shouldPlaceDotsAfterLineBreaks !== undefined
          ? { indentation, maxLineLength, shouldPlaceDotsAfterLineBreaks }
          : undefined;

      const effect = Effect.try(() => formatQuery(query, options));

      // Map success to structured JSON and errors to structured error object (still returned as success response)
      const responseEffect = pipe(
        effect,
        Effect.map(formatted =>
          createSuccessResponse({ success: true, formattedQuery: formatted })
        ),
        Effect.catchAll(error =>
          Effect.succeed(
            createSuccessResponse({
              success: false,
              error: {
                message: String(error),
                // include common error fields when present to make it structured
                name:
                  toUnknownRecord(error) && typeof toUnknownRecord(error)?.['name'] === 'string'
                    ? (toUnknownRecord(error)?.['name'] as string)
                    : undefined,
                stack:
                  toUnknownRecord(error) && typeof toUnknownRecord(error)?.['stack'] === 'string'
                    ? (toUnknownRecord(error)?.['stack'] as string)
                    : undefined,
                details: toUnknownRecord(error)?.['details'],
              },
            })
          )
        )
      );

      return Effect.runPromise(pipe(responseEffect, Effect.provide(runtime)));
    }
  );
}
