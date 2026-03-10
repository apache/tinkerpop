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
 * @fileoverview MCP resource handlers for graph database information.
 *
 * Provides MCP resources that expose real-time graph database status and schema
 * information. Resources are always registered; when no endpoint is configured
 * they return a "disconnected - configure an endpoint" message.
 */

import { Effect, pipe, Runtime, Option } from 'effect';
import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { RESOURCE_URIS, MIME_TYPES } from '../constants.js';
import { GremlinService } from '../gremlin/service.js';
import type { AppConfigType } from '../config.js';

const OFFLINE_STATUS = 'disconnected - configure an endpoint';
const OFFLINE_SCHEMA = JSON.stringify(
  {
    status: 'disconnected',
    message: 'Configure `GREMLIN_MCP_ENDPOINT` to enable graph operations.',
  },
  null,
  2
);

/**
 * Registers MCP resource handlers with the server.
 *
 * Both resources are always registered. When no endpoint is configured the
 * responses contain a static "disconnected" message; otherwise the live
 * Gremlin service is queried.
 *
 * @param server - MCP server instance
 * @param runtime - Effect runtime with Gremlin service
 * @param config - Application configuration
 */
export function registerEffectResourceHandlers(
  server: McpServer,
  runtime: Runtime.Runtime<GremlinService>,
  config: AppConfigType
): void {
  const endpointFromEnv = process.env['GREMLIN_MCP_ENDPOINT'] ?? '';
  const endpointIsConfiguredInEnv = endpointFromEnv.trim() !== '';
  const hasEndpoint = endpointIsConfiguredInEnv && Option.isSome(config.gremlin.endpoint);

  // Register status resource
  server.registerResource(
    'status',
    RESOURCE_URIS.STATUS,
    {
      title: 'Gremlin Graph Status',
      description: 'Real-time connection status of the Gremlin graph database',
      mimeType: MIME_TYPES.TEXT_PLAIN,
    },
    (_uri, _extra) => {
      if (!hasEndpoint) {
        return Promise.resolve({
          contents: [
            {
              uri: RESOURCE_URIS.STATUS,
              mimeType: MIME_TYPES.TEXT_PLAIN,
              text: OFFLINE_STATUS,
            },
          ],
        });
      }

      return Effect.runPromise(
        pipe(
          GremlinService,
          Effect.andThen(service => service.getStatus),
          Effect.map(statusObj => statusObj.status),
          Effect.catchAll(error => Effect.succeed(String(error))),
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
      }));
    }
  );

  // Register schema resource
  server.registerResource(
    'schema',
    RESOURCE_URIS.SCHEMA,
    {
      title: 'Gremlin Graph Schema',
      description:
        'Complete schema of the graph including vertex labels, edge labels, and relationship patterns',
      mimeType: MIME_TYPES.APPLICATION_JSON,
    },
    (_uri, _extra) => {
      if (!hasEndpoint) {
        return Promise.resolve({
          contents: [
            {
              uri: RESOURCE_URIS.SCHEMA,
              mimeType: MIME_TYPES.APPLICATION_JSON,
              text: OFFLINE_SCHEMA,
            },
          ],
        });
      }

      return Effect.runPromise(
        pipe(
          GremlinService,
          Effect.andThen(service => service.getSchema),
          Effect.catchAll(error =>
            Effect.succeed({ status: 'disconnected', message: String(error) })
          ),
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
      }));
    }
  );
}
