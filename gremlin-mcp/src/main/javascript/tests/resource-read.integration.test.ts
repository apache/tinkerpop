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
import { Effect, Layer, Option } from 'effect';
import { afterEach, beforeEach, describe, expect, it } from '@jest/globals';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { registerEffectResourceHandlers } from '../src/handlers/resources';
import { RESOURCE_URIS } from '../src/constants';
import type { AppConfigType } from '../src/config';
import { GremlinService } from '../src/gremlin/service';
import type { GraphSchema, GremlinQueryResult } from '../src/gremlin/models';
const makeConfig = (): AppConfigType =>
  ({
    gremlin: {
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
    server: { name: 'gremlin-mcp', version: 'test' },
    logging: { level: 'info', structured: true },
  }) as unknown as AppConfigType;
const makeFakeRuntime = (statusResult: 'connected' | 'disconnected' = 'connected') => {
  const schema: GraphSchema = {
    vertices: [],
    edges: [],
    edge_patterns: [],
    metadata: {
      generated_at: new Date().toISOString(),
      generated_by: 'test',
      sample_size_per_label: 0,
      optimization_settings: {
        include_counts: false,
        include_sample_values: false,
        max_enum_values: 10,
      },
    },
  };

  const queryResult: GremlinQueryResult = {
    results: [],
    message: 'ok',
  };

  const fakeService = GremlinService.of({
    getStatus: Effect.succeed({ status: statusResult }),
    getSchema: Effect.succeed(schema),
    getCachedSchema: Effect.succeed(null),
    refreshSchemaCache: Effect.void,
    executeQuery: (query: string) => {
      void query;
      return Effect.succeed(queryResult);
    },
    healthCheck: Effect.succeed({
      healthy: statusResult === 'connected',
      details: statusResult === 'connected' ? 'Connected' : 'Connection unavailable',
    }),
  });
  return Effect.runSync(
    Effect.provide(Effect.runtime<GremlinService>(), Layer.succeed(GremlinService, fakeService))
  );
};
describe('resource read integration', () => {
  const originalEndpoint = process.env.GREMLIN_MCP_ENDPOINT;
  beforeEach(() => {
    process.env.GREMLIN_MCP_ENDPOINT = 'localhost:8182/g';
  });
  afterEach(() => {
    if (typeof originalEndpoint === 'string') {
      process.env.GREMLIN_MCP_ENDPOINT = originalEndpoint;
    } else {
      delete process.env.GREMLIN_MCP_ENDPOINT;
    }
  });
  it('returns correct URI and content for status and schema resource reads', async () => {
    const server = new McpServer({ name: 'resource-read-test-server', version: '1.0.0' });
    registerEffectResourceHandlers(server, makeFakeRuntime('connected'), makeConfig());
    const client = new Client({ name: 'resource-read-test-client', version: '1.0.0' });
    const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
    await Promise.all([server.connect(serverTransport), client.connect(clientTransport)]);
    try {
      const status = await client.readResource({ uri: RESOURCE_URIS.STATUS });
      const schema = await client.readResource({ uri: RESOURCE_URIS.SCHEMA });
      expect(status.contents[0]?.uri).toBe(RESOURCE_URIS.STATUS);
      const statusText =
        status.contents[0] && 'text' in status.contents[0] ? status.contents[0].text : '';
      expect(statusText).toBe('connected');
      expect(schema.contents[0]?.uri).toBe(RESOURCE_URIS.SCHEMA);
      const schemaText =
        schema.contents[0] && 'text' in schema.contents[0] ? schema.contents[0].text : '';
      expect(schemaText).toContain('vertices');
      expect(schemaText).toContain('edges');
    } finally {
      await client.close();
      await server.close();
    }
  });
  it('returns offline content when GREMLIN_MCP_ENDPOINT is not set', async () => {
    delete process.env.GREMLIN_MCP_ENDPOINT;
    const server = new McpServer({ name: 'resource-offline-test-server', version: '1.0.0' });
    registerEffectResourceHandlers(server, makeFakeRuntime('connected'), makeConfig());
    const client = new Client({ name: 'resource-offline-test-client', version: '1.0.0' });
    const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
    await Promise.all([server.connect(serverTransport), client.connect(clientTransport)]);
    try {
      const status = await client.readResource({ uri: RESOURCE_URIS.STATUS });
      const schema = await client.readResource({ uri: RESOURCE_URIS.SCHEMA });
      const statusText =
        status.contents[0] && 'text' in status.contents[0] ? status.contents[0].text : '';
      expect(statusText).toBe('disconnected - configure an endpoint');
      const schemaText =
        schema.contents[0] && 'text' in schema.contents[0] ? schema.contents[0].text : '';
      expect(schemaText).toContain('Configure `GREMLIN_MCP_ENDPOINT`');
    } finally {
      await client.close();
      await server.close();
    }
  });
});
