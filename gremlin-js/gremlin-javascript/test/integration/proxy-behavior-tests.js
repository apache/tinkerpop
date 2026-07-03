/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

import assert from 'assert';

import Client from '../../lib/driver/client.js';

import { GREMLIN_SINGLE_VERTEX } from './socket-server-constants.js';

const url = process.env.GREMLIN_SOCKET_SERVER_URL || 'http://localhost:45943/gremlin';
const proxyUrl = process.env.GREMLIN_SOCKET_SERVER_PROXY_URL || 'http://localhost:45944';

describe('proxy behavior', function () {
  this.timeout(30000);

  it('routes socket server traffic through the configured proxy', async function () {
    await fetch(proxyUrl + '/__reset', { method: 'POST' });
    const client = new Client(url, { traversalSource: 'g', proxy: proxyUrl });
    await client.open?.();
    const result = await client.submit(GREMLIN_SINGLE_VERTEX);
    assert.strictEqual(result.length, 1);
    const recorded = await (await fetch(proxyUrl + '/__recorded')).json();
    assert.ok(recorded.some((t) => t.endsWith(':45943')), JSON.stringify(recorded));
    await client.close();
  });

  it('does not record when no proxy is configured', async function () {
    await fetch(proxyUrl + '/__reset', { method: 'POST' });
    const client = new Client(url, { traversalSource: 'g' });
    const result = await client.submit(GREMLIN_SINGLE_VERTEX);
    assert.strictEqual(result.length, 1);
    const recorded = await (await fetch(proxyUrl + '/__recorded')).json();
    assert.ok(!recorded.some((t) => t.endsWith(':45943')));
    await client.close();
  });
});
