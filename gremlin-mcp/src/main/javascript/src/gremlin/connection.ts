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
 * This module defines the `GremlinClientLive` layer, which is responsible for
 * creating and managing the lifecycle of a Gremlin database connection.
 * The layer uses `Effect.Layer` to provide the `GremlinClient` service
 * to the application's context, ensuring that the connection is acquired
 * when the layer is built and released when the application shuts down.
 */

import { Effect, Layer, Option, Redacted } from 'effect';
import gremlin from 'gremlin';
import { AppConfig } from '../config.js';
import { Errors } from '../errors.js';
import { GremlinClient } from './client.js';
import type { ConnectionState } from './types.js';

const { PlainTextSaslAuthenticator } = gremlin.driver.auth;
const { Client, DriverRemoteConnection } = gremlin.driver;
const { AnonymousTraversalSource } = gremlin.process;

/**
 * Creates and tests a Gremlin connection.
 *
 * This effect is responsible for establishing a connection to the Gremlin
 * server, creating a client and a graph traversal source (`g`), and then
 * testing the connection to ensure it is functional before it is used.
 *
 * @param config The application configuration containing Gremlin connection details.
 * @returns An `Effect` that resolves to a `ConnectionState` object or fails with a `GremlinConnectionError`.
 */
const makeConnection = Effect.gen(function* () {
  const config = yield* AppConfig;
  const protocol = config.gremlin.useSSL ? 'wss' : 'ws';
  const url = `${protocol}://${config.gremlin.host}:${config.gremlin.port}/gremlin`;
  const traversalSource = config.gremlin.traversalSource;

  yield* Effect.logInfo('Acquiring Gremlin connection', {
    host: config.gremlin.host,
    port: config.gremlin.port,
    ssl: config.gremlin.useSSL,
    traversalSource: config.gremlin.traversalSource,
  });

  const auth = Option.zipWith(
    config.gremlin.username,
    config.gremlin.password,
    (username, password) => ({ username, password: Redacted.value(password) })
  );

  // Build a proper Gremlin authenticator when credentials are provided
  const authenticator = Option.map(
    auth,
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

  // Test the connection
  yield* Effect.tryPromise({
    try: () => g.inject(1).next(),
    catch: error => Errors.connection('Connection test failed', { error }),
  });

  yield* Effect.logInfo('✅ Gremlin connection acquired successfully');

  return {
    client,
    connection,
    g,
    lastUsed: Date.now(),
  };
});

/**
 * Safely closes a Gremlin connection.
 *
 * This effect takes a `ConnectionState` and closes the underlying connection,
 * logging any errors that occur during the process.
 *
 * @param state The `ConnectionState` to be closed.
 * @returns An `Effect` that completes when the connection is closed.
 */
const releaseConnection = (state: ConnectionState) =>
  Effect.gen(function* () {
    yield* Effect.logInfo('Releasing Gremlin connection');
    yield* Effect.tryPromise({
      try: () => state.connection.close(),
      catch: error => Errors.connection('Failed to close Gremlin connection', { error }),
    }).pipe(Effect.catchAll(error => Effect.logWarning(`Error during release: ${error.message}`)));
    yield* Effect.logInfo('Gremlin connection released successfully');
  });

/**
 * A layer that provides a live `GremlinClient` service.
 *
 * This layer is responsible for the lifecycle of the Gremlin connection.
 * It acquires a connection when the layer is initialized and releases it
 * when the application scope is closed.
 *
 * @example
 * ```typescript
 * import { Effect } from 'effect';
 * import { GremlinClientLive } from './connection.js';
 *
 * const myApp = Effect.provide(
 *   // my effects...
 *   GremlinClientLive
 * );
 * ```
 */
export const GremlinClientLive = Layer.scoped(
  GremlinClient,
  Effect.acquireRelease(makeConnection, releaseConnection)
);
