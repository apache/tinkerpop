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
 * @fileoverview MCP resource handlers for graph database information.
 *
 * Provides MCP resources that expose real-time graph database status and schema
 * information. Resources are automatically updated and can be subscribed to by
 * MCP clients for live monitoring.
 */

import { Effect, pipe, Runtime } from 'effect';
import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { RESOURCE_URIS, MIME_TYPES } from '../constants.js';
import { ERROR_PREFIXES } from '../errors.js';
import { GremlinService } from '../gremlin/service.js';

/**
 * Registers MCP resource handlers with the server.
 *
 * @param server - MCP server instance
 * @param runtime - Effect runtime with Gremlin service
 *
 * Registers resources for:
 * - Graph connection status monitoring
 * - Live schema information access
 */
export function registerEffectResourceHandlers(
  server: McpServer,
  runtime: Runtime.Runtime<GremlinService>
): void {
  // Register status resource using the recommended registerResource method
  server.registerResource(
    'status',
    RESOURCE_URIS.STATUS,
    {
      title: 'Gremlin Graph Status',
      description: 'Real-time connection status of the Gremlin graph database',
      mimeType: MIME_TYPES.TEXT_PLAIN,
    },
    () =>
      Effect.runPromise(
        pipe(
          GremlinService,
          Effect.andThen(service => service.getStatus),
          Effect.map(statusObj => statusObj.status),
          Effect.catchAll(error => Effect.succeed(`${ERROR_PREFIXES.CONNECTION}: ${error}`)),
          Effect.provide(runtime)
        )
      ).then(result => ({
        contents: [
          {
            uri: RESOURCE_URIS.STATUS,
            mimeType: MIME_TYPES.TEXT_PLAIN,
            text: result,
          },
        ],
      }))
  );

  // Register schema resource using the recommended registerResource method
  server.registerResource(
    'schema',
    RESOURCE_URIS.SCHEMA,
    {
      title: 'Gremlin Graph Schema',
      description:
        'Complete schema of the graph including vertex labels, edge labels, and relationship patterns',
      mimeType: MIME_TYPES.APPLICATION_JSON,
    },
    () =>
      Effect.runPromise(
        pipe(
          GremlinService,
          Effect.andThen(service => service.getSchema),
          Effect.catchAll(error => Effect.succeed({ error: String(error) })),
          Effect.provide(runtime)
        )
      ).then(result => ({
        contents: [
          {
            uri: RESOURCE_URIS.SCHEMA,
            mimeType: MIME_TYPES.APPLICATION_JSON,
            text: JSON.stringify(result, null, 2),
          },
        ],
      }))
  );
}
