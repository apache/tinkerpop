#!/usr/bin/env node

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
 * @fileoverview MCP server for Apache TinkerPop Gremlin graph databases.
 *
 * Provides MCP (Model Context Protocol) tools and resources for interacting with
 * Gremlin-compatible graph databases including Apache TinkerPop, Amazon Neptune,
 * Azure Cosmos DB, and others.
 *
 * Built with Effect-ts for functional composition and error handling.
 */

import {
  ConfigProvider,
  Effect,
  Layer,
  pipe,
  LogLevel,
  Logger,
  Context,
  Fiber,
  Option,
} from 'effect';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';

import { AppConfig, type AppConfigType } from './config.js';
import { GremlinService, GremlinServiceLive } from './gremlin/service.js';
import { GremlinClient } from './gremlin/client.js';
import { SchemaServiceLive } from './gremlin/schema.js';
import { registerEffectToolHandlers } from './handlers/tools.js';
import { registerEffectResourceHandlers } from './handlers/resources.js';
import { Errors } from './errors.js';

/**
 * Service tag for the MCP server instance using Effect 3.x Context.Tag pattern.
 *
 * Provides server lifecycle management including start/stop operations.
 */
class McpServerService extends Context.Tag('McpServerService')<
  McpServerService,
  {
    readonly server: McpServer;
    readonly start: Effect.Effect<void, Error>;
    readonly stop: Effect.Effect<void, never>;
  }
>() {}

/**
 * Creates the MCP server service implementation.
 *
 * @returns Effect that provides MCP server with registered handlers
 *
 * Side effects:
 * - Creates MCP server instance
 * - Registers tool and resource handlers
 * - Sets up managed runtime for dependency injection
 * - Configures finalizers for cleanup
 */
const makeMcpServerService = Effect.gen(function* () {
  const config = yield* AppConfig;

  // Create MCP server instance
  const server = new McpServer({
    name: config.server.name,
    version: config.server.version,
  });

  yield* Effect.logInfo('✅ MCP Server instance created', {
    service: config.server.name,
    name: config.server.name,
    version: config.server.version,
  });

  // Create runtime for handlers from the current context
  const runtime = yield* Effect.runtime<GremlinService>();

  // Register handlers with dependency injection
  registerEffectToolHandlers(server, runtime, config);
  registerEffectResourceHandlers(server, runtime, config);

  yield* Effect.logInfo('✅ Handlers registered successfully', {
    service: config.server.name,
  });

  return {
    server,
    start: Effect.gen(function* () {
      yield* Effect.logInfo('🔌 Creating STDIO transport...', { service: config.server.name });

      const transport = new StdioServerTransport();

      yield* Effect.logInfo('🔗 Connecting server to transport...', {
        service: config.server.name,
      });

      yield* pipe(
        Effect.tryPromise(() => server.connect(transport)),
        Effect.mapError(error =>
          Errors.connection('Server connection failed', {
            error_type: error instanceof Error ? error.constructor.name : typeof error,
            error_message: error instanceof Error ? error.message : String(error),
          })
        )
      );

      yield* Effect.logInfo('✅ Gremlin MCP Server started successfully', {
        service: config.server.name,
        pid: process.pid,
        ready: true,
      });
    }),
    stop: Effect.gen(function* () {
      yield* Effect.logInfo('🛑 Stopping Gremlin MCP Server...', { service: config.server.name });
      // Server cleanup would go here if needed
    }),
  };
});

const McpServerServiceLive = Layer.effect(McpServerService, makeMcpServerService);

/**
 * Layer composition providing all application dependencies.
 *
 * When GREMLIN_MCP_ENDPOINT is not set we use a gremlin-free offline stub so
 * the gremlin driver is never imported or instantiated.  When it IS set we
 * dynamically import the real GremlinClientLive (which pulls in the driver).
 */
const NO_ENDPOINT_MSG =
  'No Gremlin Server configured. Set GREMLIN_MCP_ENDPOINT to enable graph operations.';

const GremlinClientOfflineLive = Layer.succeed(
  GremlinClient,
  GremlinClient.of({
    getConnection: Effect.fail(Errors.connection(NO_ENDPOINT_MSG)),
    invalidate: Effect.void,
  })
);

const GremlinLayer = Layer.provide(GremlinServiceLive, SchemaServiceLive);

// Choose the client layer based purely on whether the env var is present at startup.
// This is checked before any Effect runs so the gremlin driver is never loaded when
// no endpoint is configured.
const endpointIsConfigured =
  process.env['GREMLIN_MCP_ENDPOINT'] !== undefined &&
  process.env['GREMLIN_MCP_ENDPOINT'].trim() !== '';

// Dynamically import the real client layer only when an endpoint is provided,
// keeping the gremlin driver out of the module graph in offline mode.
const getGremlinClientLayer = async () => {
  if (!endpointIsConfigured) {
    return GremlinClientOfflineLive;
  }
  const { GremlinClientLive } = await import('./gremlin/connection.js');
  return GremlinClientLive;
};

// Resolved once at startup; used below in AppLayer.
const GremlinClientLayerPromise = getGremlinClientLayer();

/**
 * Main application Effect.
 *
 * Orchestrates server startup, configuration loading, and graceful shutdown.
 * Uses Effect.never to keep the server running indefinitely.
 */
const program = Effect.gen(function* () {
  // Get configuration
  const config = yield* AppConfig;

  const endpointInfo = Option.match(config.gremlin.endpoint, {
    onNone: () => ({ configured: false }),
    onSome: ep => ({
      configured: true,
      host: ep.host,
      port: ep.port,
      traversalSource: ep.traversalSource,
    }),
  });

  yield* Effect.logInfo('🚀 Starting Apache TinkerPop Gremlin MCP Server...', {
    service: config.server.name,
    version: config.server.version,
    gremlinEndpoint: Option.match(config.gremlin.endpoint, {
      onNone: () => '(none)',
      onSome: ep => `${ep.host}:${ep.port}`,
    }),
    logLevel: config.logging.level,
    config: {
      server: {
        name: config.server.name,
        version: config.server.version,
      },
      gremlin: {
        ...endpointInfo,
        useSSL: config.gremlin.useSSL,
        idleTimeout: config.gremlin.idleTimeout,
        username: Option.getOrNull(config.gremlin.username),
        hasPassword: Option.isSome(config.gremlin.password),
      },
      schema: {
        enumDiscoveryEnabled: config.schema.enumDiscoveryEnabled,
        enumCardinalityThreshold: config.schema.enumCardinalityThreshold,
        enumPropertyDenyList: config.schema.enumPropertyDenyList,
        includeSampleValues: config.schema.includeSampleValues,
        maxEnumValues: config.schema.maxEnumValues,
        includeCounts: config.schema.includeCounts,
      },
      logging: {
        level: config.logging.level,
        structured: true,
      },
    },
  });

  // Get server service
  const mcpServer = yield* McpServerService;

  // Set up graceful shutdown
  yield* Effect.addFinalizer(() => mcpServer.stop);

  // Start the server
  yield* mcpServer.start;

  // Keep the program running
  yield* Effect.never;
});

/**
 * Safely serializes objects to JSON with fallback error handling.
 *
 * @param obj - Object to serialize
 * @returns JSON string, with error metadata if serialization fails
 */
const safeJsonStringify = (obj: unknown): string =>
  pipe(
    Effect.try(() => JSON.stringify(obj, null, 2)),
    Effect.catchAll(() =>
      Effect.succeed(
        JSON.stringify({
          message: String(obj),
          serialization_error: true,
          timestamp: new Date().toISOString(),
        })
      )
    ),
    Effect.runSync
  );

/**
 * Structured logging Effect that writes to stderr.
 *
 * @param logData - Structured log data object
 * @returns Effect that writes log entry to stderr
 *
 * Note: All logging goes to stderr to avoid interfering with MCP JSON-RPC
 * communication on stdout.
 */
const logToStderr = (logData: Record<string, unknown>): Effect.Effect<void> =>
  Effect.sync(() => {
    const logEntry = {
      ...logData,
      timestamp: new Date().toISOString(),
    };
    process.stderr.write(`${safeJsonStringify(logEntry)}\n`);
  });

/**
 * Enhanced logging configuration based on config
 * CRITICAL: All logging must go to stderr to avoid interfering with MCP JSON responses
 */
const createLoggerLayer = (config: AppConfigType) => {
  const logLevelMap = {
    error: LogLevel.Error,
    warn: LogLevel.Warning,
    info: LogLevel.Info,
    debug: LogLevel.Debug,
  } as const;
  const minimumLogLevel = logLevelMap[config.logging.level] ?? LogLevel.Info;

  return Layer.mergeAll(
    Logger.replace(
      Logger.defaultLogger,
      Logger.make(({ logLevel: level, message, ...rest }) => {
        const logData = {
          level: level._tag.toLowerCase(),
          message,
          ...Object.fromEntries(
            Object.entries(rest).filter(
              ([key]) => !key.startsWith('_') && key !== 'span' && key !== 'fiber'
            )
          ),
        };
        // IMPORTANT: run the effect synchronously, for the love all that is sane, run it synchronously!!!
        Effect.runSync(logToStderr(logData));
      })
    ),
    Logger.minimumLogLevel(minimumLogLevel)
  );
};

/**
 * Effect-based graceful shutdown using Effect's interruption system
 */
const withGracefulShutdown = <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
  Effect.gen(function* () {
    const fiber = yield* Effect.fork(effect);

    // Set up signal handlers using Effect's interruption
    const setupSignalHandler = (signal: NodeJS.Signals) =>
      Effect.sync(() => {
        process.on(signal, () => {
          Effect.runPromise(
            pipe(
              Effect.logInfo(`Received ${signal}. Initiating graceful shutdown...`),
              Effect.andThen(() => Fiber.interrupt(fiber)),
              Effect.catchAll((error: unknown) =>
                logToStderr({
                  level: 'error',
                  message: 'Error during shutdown',
                  error: error instanceof Error ? error.message : String(error),
                })
              )
            )
          ).finally(() => {
            process.exit(0);
          });
        });
      });

    yield* setupSignalHandler('SIGINT');
    yield* setupSignalHandler('SIGTERM');

    return yield* Fiber.join(fiber);
  });

/**
 * Main entry point with full Effect composition.
 *
 * @param appLayer - Fully composed application layer (built after resolving the
 *   gremlin client layer asynchronously so the gremlin driver is never imported
 *   when GREMLIN_MCP_ENDPOINT is not set).
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const main = (appLayer: Layer.Layer<McpServerService, any, never>) =>
  Effect.gen(function* () {
    // Add startup logging before anything else - CRITICAL: Use stderr only
    yield* logToStderr({
      level: 'info',
      message: 'Apache TinkerPop - Gremlin MCP Server executable started',
      process_info: {
        pid: process.pid,
        node_version: process.versions.node,
        platform: process.platform,
        argv: process.argv,
        cwd: process.cwd(),
      },
    });

    // Get configuration early for logging setup
    const config = yield* AppConfig;

    // Log configuration
    yield* logToStderr({
      level: 'info',
      message: 'Configuration loaded',
      config: {
        gremlin: Option.match(config.gremlin.endpoint, {
          onNone: () => ({
            configured: false,
            use_ssl: config.gremlin.useSSL,
            idle_timeout: config.gremlin.idleTimeout,
          }),
          onSome: ep => ({
            configured: true,
            host: ep.host,
            port: ep.port,
            use_ssl: config.gremlin.useSSL,
            traversal_source: ep.traversalSource,
            idle_timeout: config.gremlin.idleTimeout,
          }),
        }),
        logging: {
          level: config.logging.level,
        },
      },
    });

    // Run the main program with all services provided
    yield* pipe(
      withGracefulShutdown(program),
      Effect.provide(appLayer),
      Effect.provide(createLoggerLayer(config))
    );
  });

/**
 * Run the application with improved error handling using Effect patterns.
 *
 * Async so we can await the gremlin client layer resolution before building the
 * full layer graph and starting the Effect runtime.
 */
// Ensure environment variables are used for configuration resolution across the app
const EnvConfigLayer = Layer.setConfigProvider(ConfigProvider.fromEnv());

async function run(): Promise<void> {
  const gremlinClientLayer = await GremlinClientLayerPromise;

  const AppLayer = Layer.provide(
    McpServerServiceLive,
    Layer.provide(GremlinLayer, gremlinClientLayer)
  );

  const runMain = pipe(
    Effect.scoped(main(AppLayer)),
    Effect.provide(EnvConfigLayer),
    Effect.catchAll((error: unknown) =>
      logToStderr({
        level: 'error',
        message: 'Fatal error in main program',
        error: error instanceof Error ? error.message : String(error),
        error_type: error instanceof Error ? error.constructor.name : typeof error,
        stack: error instanceof Error ? error.stack : undefined,
      }).pipe(Effect.andThen(() => Effect.sync(() => process.exit(1))))
    )
  );

  await Effect.runPromise(runMain);
}

run();
