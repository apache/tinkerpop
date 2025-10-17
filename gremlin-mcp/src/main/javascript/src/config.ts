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
 * Gremlin MCP Server
 *
 * Type-safe Application configuration with type-safe environment variable loading.
 *
 * Provides comprehensive configuration management for the Gremlin MCP server using
 * Effect.Config for validation and error handling. All configurations are loaded from
 * environment variables with sensible defaults and detailed validation.
 *
 * @example Environment Variables
 * ```bash
 * GREMLIN_MCP_ENDPOINT=localhost:8182
 * GREMLIN_MCP_USE_SSL=false
 * GREMLIN_MCP_LOG_LEVEL=info
 * GREMLIN_MCP_ENUM_DISCOVERY_ENABLED=true
 * ```
 */

import { Config, ConfigError, Effect, Either, pipe } from 'effect';
import { DEFAULTS, SERVER_INFO } from './constants.js';

/**
 * Parses and validates a Gremlin endpoint string.
 * Format: host:port or host:port/traversal_source
 * Returns Either.right({host, port, traversalSource}) on success, Either.left(ConfigError) on failure.
 * @param endpoint - Raw endpoint string
 */
const parseEndpoint = (
  endpoint: string
): Either.Either<
  { host: string; port: number; traversalSource: string },
  ConfigError.ConfigError
> => {
  const trimmedEndpoint = endpoint.trim();

  if (!trimmedEndpoint) {
    return Either.left(ConfigError.InvalidData([], 'Endpoint cannot be empty'));
  }

  const parts = trimmedEndpoint.split('/');
  const hostPort = parts[0];
  const traversalSource = parts[1] ?? DEFAULTS.TRAVERSAL_SOURCE;

  if (!hostPort) {
    return Either.left(
      ConfigError.InvalidData(
        [],
        'Invalid endpoint format. Expected host:port or host:port/traversal_source'
      )
    );
  }

  const hostPortParts = hostPort.split(':');
  if (hostPortParts.length !== 2) {
    return Either.left(
      ConfigError.InvalidData([], 'Invalid host:port format. Expected exactly one colon separator')
    );
  }

  const [host, portStr] = hostPortParts;

  if (!host?.trim() || !portStr?.trim()) {
    return Either.left(
      ConfigError.InvalidData([], 'Host and port are required and cannot be empty')
    );
  }

  const port = parseInt(portStr.trim(), 10);
  if (isNaN(port) || port <= 0 || port > 65535) {
    return Either.left(
      ConfigError.InvalidData([], 'Port must be a positive integer between 1 and 65535')
    );
  }

  return Either.right({
    host: host.trim(),
    port,
    traversalSource: traversalSource.trim(),
  });
};

/**
 * Parses a comma-separated string into a string array, trimming whitespace and removing empty entries.
 * @param value - Raw comma-separated string
 * @returns string[]
 */
const parseCommaSeparatedList = (value: string): string[] =>
  value
    .split(',')
    .map(s => s.trim())
    .filter(s => s.length > 0);

/**
 * GREMLIN_MCP_ENDPOINT: string, required. Gremlin Server compatible websocket endpoint.
 */
const GremlinMcpEndpointConfig = pipe(
  Config.string('GREMLIN_MCP_ENDPOINT'),
  Config.mapOrFail(parseEndpoint)
);

/**
 * GREMLIN_MCP_USE_SSL: boolean, optional. Enables SSL and defaults to false.
 */
const GremlinMcpUseSslConfig = Config.withDefault(
  Config.boolean('GREMLIN_MCP_USE_SSL'),
  DEFAULTS.USE_SSL
);

/**
 * GREMLIN_MCP_USERNAME: string, optional. Gremlin DB username
 */
const GremlinMcpUsernameConfig = Config.option(Config.string('GREMLIN_MCP_USERNAME'));

/**
 * GREMLIN_MCP_PASSWORD: string, optional, redacted. Gremlin DB password
 */
const GremlinMcpPasswordConfig = Config.option(Config.redacted('GREMLIN_MCP_PASSWORD'));

/**
 * GREMLIN_MCP_LOG_LEVEL: 'error' | 'warn' | 'info' | 'debug', default: info. Logging verbosity
 */
const GremlinMcpLogLevelConfig = pipe(
  Config.literal('error', 'warn', 'info', 'debug')('GREMLIN_MCP_LOG_LEVEL'),
  Config.withDefault(DEFAULTS.LOG_LEVEL)
);

/**
 * GREMLIN_MCP_IDLE_TIMEOUT: number, default: 300. Connection idle timeout (seconds)
 */
const GremlinMcpIdleTimeoutConfig = pipe(
  Config.integer('GREMLIN_MCP_IDLE_TIMEOUT'),
  Config.withDefault(300),
  Config.validate({
    message: 'Idle timeout must be a positive integer',
    validation: n => n > 0,
  })
);

/**
 * GREMLIN_MCP_ENUM_DISCOVERY_ENABLED: boolean, default: true. Enable enum property discovery
 */
const GremlinMcpEnumDiscoveryEnabledConfig = Config.withDefault(
  Config.boolean('GREMLIN_MCP_ENUM_DISCOVERY_ENABLED'),
  true
);

/**
 * GREMLIN_MCP_ENUM_CARDINALITY_THRESHOLD: number, default: 10. Max cardinality for enum detection
 */
const GremlinMcpEnumCardinalityThresholdConfig = pipe(
  Config.integer('GREMLIN_MCP_ENUM_CARDINALITY_THRESHOLD'),
  Config.withDefault(10),
  Config.validate({
    message: 'Enum cardinality threshold must be a positive integer',
    validation: n => n > 0,
  })
);

/**
 * GREMLIN_MCP_ENUM_PROPERTY_DENYLIST: string, default: id,pk,name,description,...
 * Comma-separated list of properties to exclude from enum detection
 */
const GremlinMcpEnumPropertyDenyListConfig = pipe(
  Config.string('GREMLIN_MCP_ENUM_PROPERTY_DENYLIST'),
  Config.withDefault('id,pk,name,description,startDate,endDate,timestamp,createdAt,updatedAt'),
  Config.map(parseCommaSeparatedList)
);

/**
 * GREMLIN_MCP_SCHEMA_INCLUDE_SAMPLE_VALUES: boolean, default: false. Include sample values in schema output
 */
const GremlinMcpSchemaIncludeSampleValuesConfig = Config.withDefault(
  Config.boolean('GREMLIN_MCP_SCHEMA_INCLUDE_SAMPLE_VALUES'),
  false
);

/**
 * GREMLIN_MCP_SCHEMA_MAX_ENUM_VALUES: number, default: 10. Max enum values per property (≤ 100)
 */
const GremlinMcpSchemaMaxEnumValuesConfig = pipe(
  Config.integer('GREMLIN_MCP_SCHEMA_MAX_ENUM_VALUES'),
  Config.withDefault(10),
  Config.validate({
    message: 'Max enum values must be a positive integer (≤ 100)',
    validation: n => n > 0 && n <= 100,
  })
);

/**
 * GREMLIN_MCP_SCHEMA_INCLUDE_COUNTS: boolean, default: false. Include property counts in schema output
 */
const GremlinMcpSchemaIncludeCountsConfig = Config.withDefault(
  Config.boolean('GREMLIN_MCP_SCHEMA_INCLUDE_COUNTS'),
  false
);

/**
 * ConnectionConfig: Aggregates and validates all Gremlin connection-related environment variables.
 * Ensures host, port, traversalSource, useSSL, username, password, and idleTimeout are present and valid.
 * Returns a validated config object or throws ConfigError on failure.
 */
const ConnectionConfig = pipe(
  Config.all({
    endpoint: GremlinMcpEndpointConfig,
    useSSL: GremlinMcpUseSslConfig,
    username: GremlinMcpUsernameConfig,
    password: GremlinMcpPasswordConfig,
    idleTimeout: GremlinMcpIdleTimeoutConfig,
  }),
  Config.map(({ endpoint, ...rest }) => ({
    host: endpoint.host,
    port: endpoint.port,
    traversalSource: endpoint.traversalSource,
    ...rest,
  }))
);

/**
 * SchemaDiscoveryConfig: Aggregates and validates all schema discovery-related environment variables.
 * Ensures enum discovery, cardinality, denylist, sample values, max enum values, and counts are present and valid.
 * Returns a validated config object or throws ConfigError on failure.
 */
const SchemaDiscoveryConfig = Config.all({
  enumDiscoveryEnabled: GremlinMcpEnumDiscoveryEnabledConfig,
  enumCardinalityThreshold: GremlinMcpEnumCardinalityThresholdConfig,
  enumPropertyDenyList: GremlinMcpEnumPropertyDenyListConfig,
  includeSampleValues: GremlinMcpSchemaIncludeSampleValuesConfig,
  maxEnumValues: GremlinMcpSchemaMaxEnumValuesConfig,
  includeCounts: GremlinMcpSchemaIncludeCountsConfig,
});

/**
 * ServerConfig: Immutable server name and version from constants.ts
 */
const ServerConfig = Config.succeed({
  name: SERVER_INFO.NAME,
  version: SERVER_INFO.VERSION,
} as const);

/**
 * LoggingConfig: Aggregates and validates logging configuration.
 * Always uses structured logging output.
 */
const LoggingConfig = pipe(
  Config.all({
    level: GremlinMcpLogLevelConfig,
  }),
  Config.map(config => ({
    level: config.level,
    structured: true as const, // Always use structured logging
  }))
);

/**
 * AppConfig: Complete validated application configuration object.
 * Aggregates gremlin, schema, server, and logging configs.
 * Throws ConfigError on any validation failure.
 */
export const AppConfig = Config.all({
  gremlin: ConnectionConfig,
  schema: SchemaDiscoveryConfig,
  server: ServerConfig,
  logging: LoggingConfig,
});

/**
 * Type alias for the resolved, validated application configuration object.
 */
export type AppConfigType = Effect.Effect.Success<typeof AppConfig>;
