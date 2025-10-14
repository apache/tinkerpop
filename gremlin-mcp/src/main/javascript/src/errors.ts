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
 * @fileoverview Comprehensive error handling with typed error domains.
 *
 * Defines domain-specific error types using Effect's Data.TaggedError pattern
 * for precise error handling and recovery strategies. Each error type includes
 * contextual metadata for debugging and user-friendly error messages.
 */

import { Data } from 'effect';

/**
 * Standardized error message prefixes for consistency
 */
export const ERROR_PREFIXES = {
  CONNECTION: 'Connection error',
  QUERY: 'Query failed',
  SCHEMA: 'Schema error',
  RESOURCE: 'Resource error',
  CONFIG: 'Configuration error',
  TIMEOUT: 'Operation timed out',
  PARSE: 'Parse error',
} as const;

/**
 * Configuration-related errors
 */
export class ConfigError extends Data.TaggedError('ConfigError')<{
  readonly message: string;
  readonly details?: unknown;
}> {}

/**
 * Gremlin connection errors
 */
export class GremlinConnectionError extends Data.TaggedError('GremlinConnectionError')<{
  readonly message: string;
  readonly details?: unknown;
  readonly host?: string;
  readonly port?: number;
}> {}

/**
 * Gremlin query execution errors
 */
export class GremlinQueryError extends Data.TaggedError('GremlinQueryError')<{
  readonly message: string;
  readonly query?: string;
  readonly details?: unknown;
}> {}

/**
 * Schema-related errors
 */
export class SchemaError extends Data.TaggedError('SchemaError')<{
  readonly message: string;
  readonly details?: unknown;
  readonly operation?: string;
}> {}

/**
 * Network timeout errors
 */
export class TimeoutError extends Data.TaggedError('TimeoutError')<{
  readonly message: string;
  readonly timeoutMs?: number;
  readonly operation?: string;
}> {}

/**
 * Resource management errors
 */
export class ResourceError extends Data.TaggedError('ResourceError')<{
  readonly message: string;
  readonly resource?: string;
  readonly details?: unknown;
}> {}

/**
 * Data parsing/validation errors
 */
export class ParseError extends Data.TaggedError('ParseError')<{
  readonly message: string;
  readonly input?: unknown;
  readonly details?: unknown;
}> {}

/**
 * Union type for all possible Gremlin MCP errors
 */
export type GremlinMcpError =
  | ConfigError
  | GremlinConnectionError
  | GremlinQueryError
  | SchemaError
  | TimeoutError
  | ResourceError
  | ParseError;

/**
 * Helper functions for creating common errors with standardized messaging
 */
export const Errors = {
  config: (message: string, details?: unknown) =>
    new ConfigError({
      message: `${ERROR_PREFIXES.CONFIG}: ${message}`,
      details,
    }),

  connection: (message: string, details?: unknown, host?: string, port?: number) =>
    new GremlinConnectionError({
      message: `${ERROR_PREFIXES.CONNECTION}: ${message}`,
      details,
      host,
      port,
    }),

  query: (message: string, query?: string, details?: unknown) =>
    new GremlinQueryError({
      message: `${ERROR_PREFIXES.QUERY}: ${message}`,
      query,
      details,
    }),

  schema: (message: string, operation?: string, details?: unknown) =>
    new SchemaError({
      message: `${ERROR_PREFIXES.SCHEMA}: ${message}`,
      operation,
      details,
    }),

  timeout: (message: string, timeoutMs?: number, operation?: string) =>
    new TimeoutError({
      message: `${ERROR_PREFIXES.TIMEOUT}: ${message}`,
      timeoutMs,
      operation,
    }),

  resource: (message: string, resource?: string, details?: unknown) =>
    new ResourceError({
      message: `${ERROR_PREFIXES.RESOURCE}: ${message}`,
      resource,
      details,
    }),

  parse: (message: string, input?: unknown, details?: unknown) =>
    new ParseError({
      message: `${ERROR_PREFIXES.PARSE}: ${message}`,
      input,
      details,
    }),
} as const;
