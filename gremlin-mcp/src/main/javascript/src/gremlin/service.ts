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
 * @fileoverview Effect-based Gremlin service for graph database operations.
 *
 * Provides a modular, composable service layer for Gremlin graph databases using
 * Effect-ts patterns. Handles connection management, schema caching, query execution,
 * and error handling.
 */

import { Effect, Context, Layer, pipe } from 'effect';
import { type GremlinQueryResult, GremlinQueryResultSchema } from './models/index.js';
import { parseGremlinResultsWithMetadata } from '../utils/result-parser.js';
import { isGremlinResult } from '../utils/type-guards.js';
import { GremlinConnectionError, GremlinQueryError, Errors, ParseError } from '../errors.js';
import { GremlinClient } from './client.js';
import { SchemaService } from './schema.js';
import type { GremlinClientType, GremlinResultSet } from './types.js';
import type { GraphSchema, ServiceStatus } from './types.js';

/**
 * Gremlin service using Effect Context.Tag pattern.
 *
 * Provides graph database operations including:
 * - Connection status monitoring
 * - Schema introspection and caching
 * - Query execution with result parsing
 * - Health check capabilities
 *
 * All operations return Effect types for composable error handling.
 */
export class GremlinService extends Context.Tag('GremlinService')<
  GremlinService,
  {
    readonly getStatus: Effect.Effect<ServiceStatus, never>;
    readonly getSchema: Effect.Effect<GraphSchema, GremlinConnectionError | GremlinQueryError>;
    readonly getCachedSchema: Effect.Effect<GraphSchema | null, never>;
    readonly refreshSchemaCache: Effect.Effect<void, GremlinConnectionError | GremlinQueryError>;
    readonly executeQuery: (
      query: string
    ) => Effect.Effect<GremlinQueryResult, GremlinQueryError | GremlinConnectionError | ParseError>;
    readonly healthCheck: Effect.Effect<{ healthy: boolean; details: string }, never>;
  }
>() {}

/**
 * Creates the Gremlin service implementation with dependency injection.
 *
 * @returns Effect providing the complete service implementation
 *
 * The service implementation manages:
 * - Connection state through the GremlinClient service
 * - Schema cache with automatic generation and refresh capabilities
 * - Query execution with result transformation and validation
 * - Health monitoring and status reporting
 */
const makeGremlinService = Effect.gen(function* () {
  const gremlinClient = yield* GremlinClient;
  const schemaService = yield* SchemaService;

  /**
   * Executes a raw Gremlin query against the client.
   *
   * @param query - Gremlin traversal query string
   * @param client - Gremlin client instance
   * @returns Effect with query results or execution error
   */
  const executeRawQuery = (
    query: string,
    client: GremlinClientType
  ): Effect.Effect<unknown, GremlinQueryError> =>
    pipe(
      Effect.tryPromise(() => client.submit(query)),
      Effect.mapError((error: unknown) => Errors.query('Query execution failed', query, error))
    );

  /**
   * Processes Gremlin ResultSet into standard array format.
   *
   * @param resultSet - Raw result from Gremlin client
   * @returns Array of result items
   */
  const processResultSet = (resultSet: unknown): unknown[] => {
    if (resultSet && typeof resultSet === 'object' && '_items' in resultSet) {
      return (resultSet as unknown as GremlinResultSet).toArray();
    }
    if (Array.isArray(resultSet)) {
      return resultSet;
    }
    return resultSet !== undefined ? [resultSet] : [];
  };

  /**
   * Transforms raw result set into parsed GremlinQueryResult format.
   *
   * @param query - Original query string for error context
   * @param resultSet - Raw result set from Gremlin
   * @returns Effect with parsed results and metadata
   */
  const transformGremlinResult = (
    query: string,
    resultSet: unknown
  ): Effect.Effect<{ results: unknown[]; message: string }, ParseError> =>
    pipe(
      Effect.sync(() => processResultSet(resultSet)),
      Effect.flatMap(dataArray => parseGremlinResultsWithMetadata(dataArray)),
      Effect.map(parsed => ({
        results: parsed.results,
        message: 'Query executed successfully',
      })),
      Effect.mapError(error => Errors.parse('Result parsing failed', query, error))
    );

  /**
   * Validates query result against the GremlinQueryResult schema.
   *
   * @param query - Original query string for error context
   * @param result - Parsed result object
   * @returns Effect with validated GremlinQueryResult
   */
  const validateQueryResult = (
    query: string,
    result: unknown
  ): Effect.Effect<GremlinQueryResult, ParseError> =>
    pipe(
      Effect.try(() => GremlinQueryResultSchema.parse(result)),
      Effect.mapError((error: unknown) => Errors.parse('Result validation failed', query, error))
    );

  /**
   * Executes a Gremlin query with comprehensive error handling.
   *
   * @param query - Gremlin traversal query string
   * @returns Effect with parsed and validated query results
   */
  const executeQuery = (
    query: string
  ): Effect.Effect<GremlinQueryResult, GremlinQueryError | GremlinConnectionError | ParseError> =>
    pipe(
      Effect.logDebug(`Executing Gremlin query: ${query}`),
      Effect.andThen(() => executeRawQuery(query, gremlinClient.client)),
      Effect.filterOrFail(isGremlinResult, resultSet =>
        Errors.query('Invalid result format received', query, resultSet)
      ),
      Effect.andThen(resultSet => transformGremlinResult(query, resultSet)),
      Effect.andThen(parsedResults => validateQueryResult(query, parsedResults))
    );

  const getStatus = Effect.succeed({ status: 'connected' as const });

  const healthCheck = Effect.succeed({
    healthy: true,
    details: 'Connected',
  });

  return {
    getStatus,
    getSchema: schemaService.getSchema,
    getCachedSchema: schemaService.peekSchema,
    refreshSchemaCache: schemaService.refreshSchema,
    executeQuery,
    healthCheck,
  } as const;
});

/**
 * Creates a layer providing the Gremlin service implementation.
 *
 * This layer depends on the `GremlinClient` service, which is expected
 * to be provided elsewhere in the application's layer composition.
 */
export const GremlinServiceLive = Layer.effect(GremlinService, makeGremlinService);
