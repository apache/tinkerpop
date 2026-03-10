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

import { Option, Runtime } from 'effect';
import { describe, it, expect, beforeEach, afterEach } from '@jest/globals';
import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { registerEffectResourceHandlers } from '../src/handlers/resources';
import { RESOURCE_URIS, MIME_TYPES } from '../src/constants';
import type { GremlinService } from '../src/gremlin/service';
import type { AppConfigType } from '../src/config';

type ResourceHandler = (
  uri: URL,
  extra: unknown
) => Promise<{
  contents: Array<{ uri: string; mimeType: string; text: string }>;
}>;

const createMockServer = () => {
  const handlers = new Map<string, ResourceHandler>();

  const server = {
    registerResource: (
      name: string,
      _uri: string,
      _meta: unknown,
      handler: ResourceHandler
    ): void => {
      handlers.set(name, handler);
    },
  } as unknown as McpServer;

  return { server, handlers };
};

describe('resource handler offline behavior', () => {
  const originalEndpoint = process.env.GREMLIN_MCP_ENDPOINT;

  beforeEach(() => {
    delete process.env.GREMLIN_MCP_ENDPOINT;
  });

  afterEach(() => {
    if (typeof originalEndpoint === 'string') {
      process.env.GREMLIN_MCP_ENDPOINT = originalEndpoint;
    } else {
      delete process.env.GREMLIN_MCP_ENDPOINT;
    }
  });

  it('returns offline status and schema when GREMLIN_MCP_ENDPOINT is missing', async () => {
    const { server, handlers } = createMockServer();

    const config = {
      gremlin: {
        // Simulate stale pre-parsed config; env var absence must still force offline resource output.
        endpoint: Option.some({ host: 'localhost', port: 8182, traversalSource: 'g' }),
        useSSL: false,
        username: Option.none(),
        password: Option.none(),
        idleTimeout: 300,
      },
      schema: {
        enumDiscoveryEnabled: true,
        enumCardinalityThreshold: 10,
        enumPropertyDenyList: [],
        includeSampleValues: false,
        maxEnumValues: 10,
        includeCounts: false,
      },
      server: {
        name: 'gremlin-mcp',
        version: 'test',
      },
      logging: {
        level: 'info',
        structured: true,
      },
    } as unknown as AppConfigType;

    registerEffectResourceHandlers(
      server,
      Runtime.defaultRuntime as Runtime.Runtime<GremlinService>,
      config
    );

    const status = await handlers.get('status')!(new URL(RESOURCE_URIS.STATUS), {});
    const schema = await handlers.get('schema')!(new URL(RESOURCE_URIS.SCHEMA), {});

    expect(status).toEqual({
      contents: [
        {
          uri: RESOURCE_URIS.STATUS,
          mimeType: MIME_TYPES.TEXT_PLAIN,
          text: 'disconnected - configure an endpoint',
        },
      ],
    });

    expect(schema).toEqual({
      contents: [
        {
          uri: RESOURCE_URIS.SCHEMA,
          mimeType: MIME_TYPES.APPLICATION_JSON,
          text: JSON.stringify(
            {
              status: 'disconnected',
              message: 'Configure `GREMLIN_MCP_ENDPOINT` to enable graph operations.',
            },
            null,
            2
          ),
        },
      ],
    });
  });
});
