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

import {
  GREMLIN_SINGLE_VERTEX,
  GREMLIN_CLOSE_CONNECTION,
  GREMLIN_VERTEX_THEN_CLOSE,
  GREMLIN_FAIL_AFTER_DELAY,
  GREMLIN_PARTIAL_CONTENT_CLOSE,
  GREMLIN_SLOW_RESPONSE,
  GREMLIN_MALFORMED_RESPONSE,
  GREMLIN_NO_RESPONSE,
  GREMLIN_EMPTY_BODY,
} from './socket-server-constants.js';

const url = process.env.GREMLIN_SOCKET_SERVER_URL || 'http://localhost:45943/gremlin';

function createClient(options) {
  return new Client(url, { traversalSource: 'g', ...options });
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

describe('Client Behavior', function () {
  this.timeout(30000);
  let client;

  before(async function () {
    client = createClient();
    try {
      await client.submit(GREMLIN_SINGLE_VERTEX);
    } catch (e) {
      client.close();
      this.skip();
    }
  });

  after(function () {
    return client.close();
  });

  it('should return a single vertex', async function () {
    const result = await client.submit(GREMLIN_SINGLE_VERTEX);
    assert.strictEqual(result.length, 1);
  });

  it('should handle connection close before response and recover', async function () {
    await assert.rejects(client.submit(GREMLIN_CLOSE_CONNECTION), /fetch failed/);
    const result = await client.submit(GREMLIN_SINGLE_VERTEX);
    assert.strictEqual(result.length, 1);
  });

  it('should handle connection close after response and recover', async function () {
    const result = await client.submit(GREMLIN_VERTEX_THEN_CLOSE);
    assert.strictEqual(result.length, 1);
    await delay(3000);
    const recovery = await client.submit(GREMLIN_SINGLE_VERTEX);
    assert.strictEqual(recovery.length, 1);
  });

  it('should handle server error after delay and recover', async function () {
    try {
      await client.submit(GREMLIN_FAIL_AFTER_DELAY);
      assert.fail('Expected an error');
    } catch (err) {
      assert.strictEqual(err.statusCode, 500);
      assert.match(err.message, /HTTP 500/);
    }
    const result = await client.submit(GREMLIN_SINGLE_VERTEX);
    assert.strictEqual(result.length, 1);
  });

  it('should handle partial content close and recover', async function () {
    await assert.rejects(client.submit(GREMLIN_PARTIAL_CONTENT_CLOSE), /terminated/);
    const result = await client.submit(GREMLIN_SINGLE_VERTEX);
    assert.strictEqual(result.length, 1);
  });

  it('should handle malformed response and recover', async function () {
    await assert.rejects(client.submit(GREMLIN_MALFORMED_RESPONSE), /Unsupported version/);
    const result = await client.submit(GREMLIN_SINGLE_VERTEX);
    assert.strictEqual(result.length, 1);
  });

  it('should handle empty response body and recover', async function () {
    this.timeout(10000);
    await assert.rejects(client.submit(GREMLIN_EMPTY_BODY), /Buffer is empty/);
    const result = await client.submit(GREMLIN_SINGLE_VERTEX);
    assert.strictEqual(result.length, 1);
  });

  it('should handle slow response', async function () {
    this.timeout(30000);
    const result = await client.submit(GREMLIN_SLOW_RESPONSE);
    assert.ok(result.length > 0);
  });

  it.skip('should timeout when server never responds - JS driver lacks client-side idle timeout', async function () {
    const shortTimeoutClient = createClient({ requestTimeout: 1000 });
    try {
      await assert.rejects(shortTimeoutClient.submit(GREMLIN_NO_RESPONSE));
      const result = await shortTimeoutClient.submit(GREMLIN_SINGLE_VERTEX);
      assert.strictEqual(result.length, 1);
    } finally {
      shortTimeoutClient.close();
    }
  });

  it('should handle async requests during connection close', async function () {
    const p1 = client.submit(GREMLIN_CLOSE_CONNECTION);
    const p2 = client.submit(GREMLIN_CLOSE_CONNECTION);
    await assert.rejects(p1);
    await assert.rejects(p2);
    const result = await client.submit(GREMLIN_SINGLE_VERTEX);
    assert.strictEqual(result.length, 1);
  });

  it('should handle concurrent mixed requests', async function () {
    const good = Array.from({ length: 5 }, () => client.submit(GREMLIN_SINGLE_VERTEX));
    const bad = Array.from({ length: 5 }, () => client.submit(GREMLIN_CLOSE_CONNECTION));
    const results = await Promise.allSettled([...good, ...bad]);
    const fulfilled = results.filter((r) => r.status === 'fulfilled');
    const rejected = results.filter((r) => r.status === 'rejected');
    assert.ok(fulfilled.length > 0, 'Expected at least some fulfilled results');
    assert.ok(rejected.length > 0, 'Expected at least some rejected results');
  });
});
