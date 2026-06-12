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
import Connection from '../../lib/driver/connection.js';
import { RequestMessage } from '../../lib/driver/request-message.js';

// Connection-level unit tests that verify interceptor wiring and auto-serialization
// by mocking the global fetch and capturing what the Connection sends.
describe('Connection (request pipeline)', function () {
  let originalFetch;
  let captured;

  beforeEach(function () {
    captured = null;
    originalFetch = global.fetch;
    // Return an error response so we don't need a valid GraphBinary body; the test only
    // cares about what was sent to fetch. The resulting ResponseError is swallowed per-test.
    global.fetch = (url, init) => {
      captured = { url, init };
      return Promise.resolve({
        ok: false,
        status: 500,
        statusText: 'Internal Server Error',
        headers: { get: () => 'text/plain' },
        arrayBuffer: () => Promise.resolve(Buffer.from('error')),
      });
    };
  });

  afterEach(function () {
    global.fetch = originalFetch;
  });

  function makeConnection(options = {}) {
    const conn = new Connection('http://localhost:8182/gremlin',
      { enableUserAgentOnConnect: false, ...options });
    conn.isOpen = true;
    return conn;
  }

  async function submitAndIgnoreError(conn, msg) {
    try {
      await conn.submit(msg);
    } catch (e) {
      // Expected: the mock returns a 500 response.
    }
  }

  it('auto-serializes the body to JSON when no interceptor does', async function () {
    const conn = makeConnection();
    await submitAndIgnoreError(conn, RequestMessage.build('g.V()').addG('g').create());

    assert.ok(captured, 'fetch should have been called');
    assert.strictEqual(captured.init.headers['Content-Type'], 'application/json');
    const parsed = JSON.parse(Buffer.from(captured.init.body).toString('utf-8'));
    assert.strictEqual(parsed.gremlin, 'g.V()');
    assert.strictEqual(parsed.g, 'g');
  });

  it('keeps the GraphBinary Accept header for responses', async function () {
    const conn = makeConnection();
    await submitAndIgnoreError(conn, RequestMessage.build('g.V()').addG('g').create());

    assert.strictEqual(captured.init.headers['Accept'], 'application/vnd.graphbinary-v4.0');
  });

  it('runs interceptors in registration order', async function () {
    const order = [];
    const conn = makeConnection({
      interceptors: [
        (req) => { order.push(1); },
        (req) => { order.push(2); },
        (req) => { order.push(3); },
      ],
    });
    await submitAndIgnoreError(conn, RequestMessage.build('g.V()').addG('g').create());

    assert.deepStrictEqual(order, [1, 2, 3]);
  });

  it('reflects interceptor body mutation in the serialized payload', async function () {
    const conn = makeConnection({
      interceptors: [
        (req) => {
          req.body = RequestMessage.build('g.inject(99)').addG('gmodern').create();
        },
      ],
    });
    await submitAndIgnoreError(conn, RequestMessage.build('g.V()').addG('g').create());

    const parsed = JSON.parse(Buffer.from(captured.init.body).toString('utf-8'));
    assert.strictEqual(parsed.gremlin, 'g.inject(99)');
    assert.strictEqual(parsed.g, 'gmodern');
  });

  it('lets an interceptor add headers that reach the request', async function () {
    const conn = makeConnection({
      interceptors: [
        (req) => { req.headers['X-Custom'] = 'value'; },
      ],
    });
    await submitAndIgnoreError(conn, RequestMessage.build('g.V()').addG('g').create());

    assert.strictEqual(captured.init.headers['X-Custom'], 'value');
  });

  it('propagates interceptor errors to the caller', async function () {
    const conn = makeConnection({
      interceptors: [
        () => { throw new Error('interceptor broke'); },
      ],
    });

    await assert.rejects(
      () => conn.submit(RequestMessage.build('g.V()').addG('g').create()),
      /interceptor broke/);
    assert.strictEqual(captured, null, 'fetch should not be called when an interceptor throws');
  });

  it('auth interceptor always runs last regardless of option order', async function () {
    const order = [];
    const conn = makeConnection({
      auth: (req) => { order.push(3); },
      interceptors: [
        (req) => { order.push(1); },
        (req) => { order.push(2); },
      ],
    });
    await submitAndIgnoreError(conn, RequestMessage.build('g.V()').addG('g').create());

    assert.deepStrictEqual(order, [1, 2, 3], 'auth interceptor should always run last');
  });
});
