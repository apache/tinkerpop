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
 * @fileoverview Provides a live implementation of the GremlinClient service.
 *
 * At layer construction time, checks whether GREMLIN_MCP_ENDPOINT is configured:
 *
 * - **Offline mode** (no endpoint): `getConnection` always fails immediately with a
 *   clear error. No gremlin driver objects (Client, DriverRemoteConnection, etc.)
 *   are ever instantiated.
 *
 * - **Online mode** (endpoint configured): connection is created lazily on first use
 *   and cached. On invalidation the connection is closed and the cache cleared, so
 *   the next `getConnection` creates a fresh connection.
 */

import { Effect, Layer, Option, Redacted, Ref, pipe } from 'effect';
import gremlin from 'gremlin';
import { AppConfig } from '../config.js';
import { Errors } from '../errors.js';
import { GremlinClient } from './client.js';
import type { ConnectionState } from './types.js';
import type { GremlinConnectionError } from '../errors.js';

const { PlainTextSaslAuthenticator } = gremlin.driver.auth;
const { Client, DriverRemoteConnection } = gremlin.driver;
const { AnonymousTraversalSource } = gremlin.process;

const NO_ENDPOINT_MSG =
  'No Gremlin Server configured. Set GREMLIN_MCP_ENDPOINT to enable graph operations.';

/**
 * Safely closes a Gremlin connection, logging any errors without propagating them.
 */
const closeConnection = (state: ConnectionState): Effect.Effect<void, never> =>
  Effect.gen(function* () {
    yield* Effect.logInfo('Closing Gremlin connection');
    yield* pipe(
      Effect.tryPromise({
        try: () => state.connection.close(),
        catch: error => Errors.connection('Failed to close Gremlin connection', { error }),
      }),
      Effect.catchAll(error => Effect.logWarning(`Error during connection close: ${error.message}`))
    );
    yield* Effect.logInfo('Gremlin connection closed');
  });

/**
 * Creates a fresh Gremlin connection. Only called when the endpoint IS configured.
 */
const createConnection = (
  host: string,
  port: number,
  traversalSource: string,
  useSSL: boolean,
  authenticatorInput: Option.Option<{ username: string; password: string }>
): Effect.Effect<ConnectionState, GremlinConnectionError> =>
  Effect.gen(function* () {
    const protocol = useSSL ? 'wss' : 'ws';
    const url = `${protocol}://${host}:${port}/gremlin`;

    yield* Effect.logInfo('Creating Gremlin connection', {
      host,
      port,
      ssl: useSSL,
      traversalSource,
    });

    const authenticator = Option.map(
      authenticatorInput,
      ({ username, password }) => new PlainTextSaslAuthenticator(username, password)
    );

    const connection = yield* Effect.try({
      try: () =>
        new DriverRemoteConnection(url, {
          traversalSource,
          authenticator: Option.getOrUndefined(authenticator),
        }),
      catch: error => Errors.connection('Failed to create remote connection', { error }),
    });

    const g = AnonymousTraversalSource.traversal().withRemote(connection);
    const client = new Client(url, {
      traversalSource,
      authenticator: Option.getOrUndefined(authenticator),
    });

    // Verify the server is reachable before caching
    yield* Effect.tryPromise({
      try: () => g.inject(1).next(),
      catch: error => Errors.connection('Connection test failed', { error }),
    });

    yield* Effect.logInfo('✅ Gremlin connection established and verified');

    return { client, connection, g, lastUsed: Date.now() };
  });

/**
 * A layer that provides a live `GremlinClient` service.
 *
 * Branches at construction time on whether GREMLIN_MCP_ENDPOINT is configured:
 * - No endpoint → offline service; `getConnection` always fails, no driver objects created.
 * - Endpoint set → online service; lazy cached connection with invalidate support.
 */
export const GremlinClientLive = Layer.scoped(
  GremlinClient,
  Effect.gen(function* () {
    const config = yield* AppConfig;

    // ── Offline mode ──────────────────────────────────────────────────────────
    if (Option.isNone(config.gremlin.endpoint)) {
      yield* Effect.logInfo(
        '⚠️  No GREMLIN_MCP_ENDPOINT configured — starting in offline mode. ' +
          'Translate and format tools are available; graph tools require an endpoint.'
      );
      return GremlinClient.of({
        getConnection: Effect.fail(Errors.connection(NO_ENDPOINT_MSG)),
        invalidate: Effect.void,
      });
    }

    // ── Online mode ───────────────────────────────────────────────────────────
    const { host, port, traversalSource } = config.gremlin.endpoint.value;

    const authenticatorInput = Option.zipWith(
      config.gremlin.username,
      config.gremlin.password,
      (username, password) => ({ username, password: Redacted.value(password) })
    );

    const ref = yield* Ref.make<Option.Option<ConnectionState>>(Option.none());

    // Release connection when the scope closes
    yield* Effect.addFinalizer(() =>
      Effect.gen(function* () {
        const current = yield* Ref.get(ref);
        if (Option.isSome(current)) {
          yield* closeConnection(current.value);
        }
      })
    );

    const getConnection: Effect.Effect<ConnectionState, GremlinConnectionError> = Effect.gen(
      function* () {
        const current = yield* Ref.get(ref);
        if (Option.isSome(current)) {
          return current.value;
        }
        const state = yield* createConnection(
          host,
          port,
          traversalSource,
          config.gremlin.useSSL,
          authenticatorInput
        );
        yield* Ref.set(ref, Option.some(state));
        return state;
      }
    );

    const invalidate: Effect.Effect<void, never> = Effect.gen(function* () {
      const current = yield* Ref.get(ref);
      yield* Ref.set(ref, Option.none());
      if (Option.isSome(current)) {
        yield* closeConnection(current.value);
      }
    });

    return GremlinClient.of({ getConnection, invalidate });
  })
);
