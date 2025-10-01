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
 * @fileoverview Reusable patterns for MCP tool handler implementation.
 *
 * Provides standardized Effect-based patterns for creating MCP tool handlers
 * with consistent error handling, response formatting, and runtime execution.
 * Eliminates boilerplate and ensures uniform behavior across all tools.
 */

import { Effect, pipe } from 'effect';
import { z } from 'zod';
import { GremlinService } from '../gremlin/service.js';

/**
 * Standard MCP tool response structure following the protocol specification.
 */
export interface McpToolResponse {
  [x: string]: unknown;
  content: Array<{ type: 'text'; text: string }>;
  isError?: boolean;
}

/**
 * Creates standardized success response with JSON formatting.
 *
 * @param data - Data to include in response
 * @returns MCP tool response with formatted content
 */
export const createSuccessResponse = (data: unknown): McpToolResponse => ({
  content: [{ type: 'text', text: JSON.stringify(data, null, 2) }],
});

/**
 * Creates a success response for simple string values (no JSON encoding)
 */
export const createStringResponse = (text: string): McpToolResponse => ({
  content: [{ type: 'text', text }],
});

/**
 * Creates a standardized error response
 */
export const createErrorResponse = (message: string): McpToolResponse => ({
  content: [{ type: 'text', text: message }],
  isError: true,
});

/**
 * Simple tool handler that executes an Effect and returns proper MCP response
 */
export const createToolEffect = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
  operationName: string
): Effect.Effect<McpToolResponse, never, R> =>
  pipe(
    effect,
    Effect.map(createSuccessResponse),
    Effect.catchAll(error => Effect.succeed(createErrorResponse(`${operationName}: ${error}`)))
  );

/**
 * Simple string response tool handler
 */
export const createStringToolEffect = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
  operationName: string
): Effect.Effect<McpToolResponse, never, R> =>
  pipe(
    effect,
    Effect.map(result => createStringResponse(String(result))),
    Effect.catchAll(error => Effect.succeed(createErrorResponse(`${operationName}: ${error}`)))
  );

/**
 * Query result handler with structured error responses
 */
export const createQueryEffect = (
  query: string
): Effect.Effect<McpToolResponse, never, GremlinService> =>
  pipe(
    GremlinService,
    Effect.andThen(service => service.executeQuery(query)),
    Effect.map(createSuccessResponse),
    Effect.catchAll(error => {
      // For query errors, return structured JSON with empty results and error message
      const errorResponse = {
        results: [],
        message: `Query failed: ${error}`,
      };
      return Effect.succeed(createSuccessResponse(errorResponse));
    })
  );

/**
 * Validated input tool handler
 */
export const createValidatedToolEffect =
  <T, A, E, R>(
    schema: z.ZodSchema<T>,
    handler: (input: T) => Effect.Effect<A, E, R>,
    operationName: string
  ) =>
  (args: unknown): Effect.Effect<McpToolResponse, never, R> =>
    pipe(
      Effect.try(() => schema.parse(args)),
      Effect.andThen(handler),
      Effect.map(createSuccessResponse),
      Effect.catchAll(error => Effect.succeed(createErrorResponse(`${operationName}: ${error}`)))
    );
