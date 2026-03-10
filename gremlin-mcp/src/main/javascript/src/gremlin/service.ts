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
 * @fileoverview Effect-based Gremlin service for graph database operations.
 *
 * Provides a modular, composable service layer for Gremlin graph databases using
 * Effect-ts patterns. Handles connection management, schema caching, query execution,
 * and error handling. On any GremlinConnectionError the service invalidates the
 * cached connection and retries the operation once to transparently reconnect.
 */

import { Effect, Context, Layer, pipe } from 'effect';
import { type GremlinQueryResult, GremlinQueryResultSchema } from './models/index.js';
import { parseGremlinResultsWithMetadata } from '../utils/result-parser.js';
import { isGremlinResult } from '../utils/type-guards.js';
import { GremlinConnectionError, GremlinQueryError, Errors, ParseError } from '../errors.js';
import { GremlinClient } from './client.js';
import { SchemaService } from './schema.js';
import type { GremlinClientType, GremlinResultSet, ConnectionState } from './types.js';
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
 * The service wraps all graph operations in a reconnect layer: on any
 * GremlinConnectionError the cached connection is invalidated and the operation
 * is retried once with a fresh connection.
 */
const makeGremlinService = Effect.gen(function* () {
  const gremlinClient = yield* GremlinClient;
  const schemaService = yield* SchemaService;

  /**
   * Wraps an Effect so that on GremlinConnectionError the connection is
   * invalidated and the operation retried exactly once.
   */
  const withReconnect = <A, E>(
    operation: Effect.Effect<A, E | GremlinConnectionError>
  ): Effect.Effect<A, E | GremlinConnectionError> =>
    pipe(
      operation,
      Effect.catchTag('GremlinConnectionError', () =>
        pipe(
          Effect.logWarning('Connection error — invalidating and retrying once'),
          Effect.andThen(gremlinClient.invalidate),
          Effect.andThen(operation)
        )
      )
    );

  /**
   * Executes a raw Gremlin query against the client.
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
   * Core query execution — obtains the connection and runs the query.
   */
  const doExecuteQuery = (
    query: string
  ): Effect.Effect<GremlinQueryResult, GremlinQueryError | GremlinConnectionError | ParseError> =>
    pipe(
      gremlinClient.getConnection,
      Effect.flatMap(conn => executeRawQuery(query, conn.client)),
      Effect.filterOrFail(isGremlinResult, resultSet =>
        Errors.query('Invalid result format received', query, resultSet)
      ),
      Effect.andThen(resultSet => transformGremlinResult(query, resultSet)),
      Effect.andThen(parsedResults => validateQueryResult(query, parsedResults))
    );

  /**
   * Executes a Gremlin query with transparent reconnection on failure.
   */
  const executeQuery = (
    query: string
  ): Effect.Effect<GremlinQueryResult, GremlinQueryError | GremlinConnectionError | ParseError> =>
    pipe(
      Effect.logDebug(`Executing Gremlin query: ${query}`),
      Effect.andThen(() => withReconnect(doExecuteQuery(query)))
    );

  /**
   * Performs a lightweight server round-trip to verify the current connection is still alive.
   */
  const verifyConnectionAlive = (
    conn: ConnectionState
  ): Effect.Effect<void, GremlinConnectionError> =>
    pipe(
      Effect.tryPromise(() => conn.g.inject(1).next()),
      Effect.asVoid,
      Effect.mapError(error => Errors.connection('Connection health check failed', error))
    );

  /**
   * Returns current connection status without throwing.
   */
  const getStatus: Effect.Effect<ServiceStatus, never> = pipe(
    withReconnect(
      pipe(
        gremlinClient.getConnection,
        Effect.flatMap(conn => verifyConnectionAlive(conn)),
        Effect.as({ status: 'connected' as const })
      )
    ),
    Effect.catchAll(() => Effect.succeed({ status: 'disconnected' as const }))
  );

  const healthCheck: Effect.Effect<{ healthy: boolean; details: string }, never> = pipe(
    withReconnect(
      pipe(
        gremlinClient.getConnection,
        Effect.flatMap(conn => verifyConnectionAlive(conn)),
        Effect.as({ healthy: true, details: 'Connected' })
      )
    ),
    Effect.catchAll(() => Effect.succeed({ healthy: false, details: 'Connection unavailable' }))
  );

  return {
    getStatus,
    getSchema: withReconnect(schemaService.getSchema),
    getCachedSchema: schemaService.peekSchema,
    refreshSchemaCache: withReconnect(schemaService.refreshSchema),
    executeQuery,
    healthCheck,
  } as const;
});

/**
 * Creates a layer providing the Gremlin service implementation.
 *
 * Depends on `GremlinClient` and `SchemaService`.
 */
export const GremlinServiceLive = Layer.effect(GremlinService, makeGremlinService);
