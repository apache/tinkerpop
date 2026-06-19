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

  it('sends Accept-Encoding: deflate by default (compression on)', async function () {
    const conn = makeConnection();
    await submitAndIgnoreError(conn, RequestMessage.build('g.V()').addG('g').create());

    assert.strictEqual(captured.init.headers['Accept-Encoding'], 'deflate');
  });

  it('does not send an Accept-Encoding header when compression is explicitly off', async function () {
    const conn = makeConnection({ compression: 'none' });
    await submitAndIgnoreError(conn, RequestMessage.build('g.V()').addG('g').create());

    assert.strictEqual(captured.init.headers['Accept-Encoding'], undefined);
  });

  it('throws for an invalid compression value', function () {
    assert.throws(
      () => makeConnection({ compression: 'gzip' }),
      /compression must be/);
  });

  it('passes the default dispatcher to fetch', async function () {
    const conn = makeConnection();
    await submitAndIgnoreError(conn, RequestMessage.build('g.V()').addG('g').create());

    assert.ok(captured.init.dispatcher, 'fetch should receive a dispatcher');
  });

  it('closes its built dispatcher on close()', async function () {
    const conn = makeConnection();
    await submitAndIgnoreError(conn, RequestMessage.build('g.V()').addG('g').create());

    const built = captured.init.dispatcher;
    assert.ok(built, 'a dispatcher should have been built and passed to fetch');

    await conn.close();
    // A closed undici Agent reports itself as destroyed; closing it again is a no-op.
    assert.strictEqual(built.closed === true || built.destroyed === true, true,
      'the connection-built dispatcher should be closed on close()');
  });

  it('exposes the default batch size of 64', function () {
    const conn = makeConnection();
    assert.strictEqual(conn.defaultBatchSize, 64);
  });

  it('honors a custom defaultBatchSize', function () {
    const conn = makeConnection({ defaultBatchSize: 250 });
    assert.strictEqual(conn.defaultBatchSize, 250);
  });

  it('invokes a logger callback during the request lifecycle', async function () {
    const lines = [];
    const conn = makeConnection({ logger: (level, message) => lines.push([level, message]) });
    await submitAndIgnoreError(conn, RequestMessage.build('g.V()').addG('g').create());

    assert.ok(lines.some(([, m]) => /Sending POST request/.test(m)), 'should log the outgoing request');
  });

  it('applies the deprecated headers option to the outgoing request via a synthesized interceptor', async function () {
    const conn = makeConnection({ headers: { 'X-Custom': 'value', 'X-Another': 'two' } });
    await submitAndIgnoreError(conn, RequestMessage.build('g.V()').addG('g').create());

    assert.strictEqual(captured.init.headers['X-Custom'], 'value');
    assert.strictEqual(captured.init.headers['X-Another'], 'two');
  });

  it('emits a one-time deprecation warning for the headers option', function () {
    const lines = [];
    makeConnection({ headers: { 'X-Custom': 'value' }, logger: (level, message) => lines.push([level, message]) });

    const warnings = lines.filter(([level, m]) => level === 'warn' && /headers.*deprecated/i.test(m));
    assert.strictEqual(warnings.length, 1, 'should warn exactly once about the deprecated headers option');
  });

  it('composes the deprecated headers option with explicit interceptors and auth', async function () {
    const order = [];
    const conn = makeConnection({
      headers: { 'X-Headers-Option': 'h' },
      interceptors: [
        (req) => { order.push('interceptor'); req.headers['X-Interceptor'] = 'i'; },
      ],
      auth: (req) => {
        order.push('auth');
        // Auth runs last, so the headers-option header is already present and visible to signing.
        req.headers['X-Auth-Saw-Headers-Option'] = String(req.headers['X-Headers-Option'] === 'h');
      },
    });
    await submitAndIgnoreError(conn, RequestMessage.build('g.V()').addG('g').create());

    // Explicit interceptor runs first, then the synthesized headers interceptor, then auth last.
    assert.deepStrictEqual(order, ['interceptor', 'auth']);
    assert.strictEqual(captured.init.headers['X-Interceptor'], 'i');
    assert.strictEqual(captured.init.headers['X-Headers-Option'], 'h');
    assert.strictEqual(captured.init.headers['X-Auth-Saw-Headers-Option'], 'true',
      'auth should run after the headers option is applied');
  });
});
