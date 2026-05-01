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

/**
 * @author Kyle Boyer
 */
'use strict';

import assert from "assert";
import http from "http";
import url from "url";
import Client from '../../lib/driver/client.js';
import ResponseError from '../../lib/driver/response-error.js';

const testServerPort = 45944;
const testServer401ResponseBody = 'Invalid credentials provided';

describe('Connection', function () {
  const server = http.createServer(function (req, res) {
    const parsedUrl = url.parse(req.url, true);
    const pathname = parsedUrl.pathname;
    if (pathname === '/401') {
      res.statusCode = 401;
      return res.end(testServer401ResponseBody);
    }
    if (pathname === '/401/json') {
      res.statusCode = 401;
      res.setHeader('Content-Type', 'application/json');
      return res.end(JSON.stringify({ message: 'Authentication required' }));
    }
    if (pathname === '/500/json-error-field') {
      res.statusCode = 500;
      res.setHeader('Content-Type', 'application/json');
      return res.end(JSON.stringify({ error: 'Internal failure' }));
    }
    if (pathname === '/502/json-malformed') {
      res.statusCode = 502;
      res.setHeader('Content-Type', 'application/json');
      return res.end('not valid json{{{');
    }
    res.statusCode = 404;
    res.end();
  });
  before(function () {
    return new Promise((resolve) => server.listen(testServerPort, resolve));
  });
  after(function () {
    return server.close();
  });

  describe('#submit()', function () {
    it('should handle unexpected response errors with body', async function () {
      const client = new Client(`http://localhost:${testServerPort}/401`, {});
      try {
        await client.submit('g.V()');
        assert.fail('invalid status codes should throw');
      } catch (err) {
        assert.ok(err);
        assert.ok(err.message.indexOf('401') > 0);
      }
    });
    it('should handle unexpected response errors with no body', async function () {
      const client = new Client(`http://localhost:${testServerPort}/404`, {});
      try {
        await client.submit('g.V()');
        assert.fail('invalid status codes should throw');
      } catch (err) {
        assert.ok(err);
        assert.ok(err.message.indexOf('404') > 0);
      }
    });
    it('should extract message from JSON error response', async function () {
      const client = new Client(`http://localhost:${testServerPort}/401/json`, {});
      try {
        await client.submit('g.V()');
        assert.fail('should have thrown');
      } catch (err) {
        assert.ok(err instanceof ResponseError);
        assert.strictEqual(err.statusCode, 401);
        assert.strictEqual(err.statusMessage, 'Authentication required');
      }
    });
    it('should extract error field from JSON error response', async function () {
      const client = new Client(`http://localhost:${testServerPort}/500/json-error-field`, {});
      try {
        await client.submit('g.V()');
        assert.fail('should have thrown');
      } catch (err) {
        assert.ok(err instanceof ResponseError);
        assert.strictEqual(err.statusCode, 500);
        assert.strictEqual(err.statusMessage, 'Internal failure');
      }
    });
    it('should fall back to generic error for malformed JSON response', async function () {
      const client = new Client(`http://localhost:${testServerPort}/502/json-malformed`, {});
      try {
        await client.submit('g.V()');
        assert.fail('should have thrown');
      } catch (err) {
        assert.ok(err instanceof ResponseError);
        assert.strictEqual(err.statusCode, 502);
        assert.ok(err.message.indexOf('502') > 0);
      }
    });
  });

  describe('#stream()', function () {
    /** Helper to drain an async generator and collect thrown errors */
    async function drainStream(gen) {
      const items = [];
      for await (const item of gen) {
        items.push(item);
      }
      return items;
    }

    it('should handle unexpected response errors with body', async function () {
      const client = new Client(`http://localhost:${testServerPort}/401`, {});
      try {
        await drainStream(client.stream('g.V()', null));
        assert.fail('invalid status codes should throw');
      } catch (err) {
        assert.ok(err);
        assert.ok(err.message.indexOf('401') > 0);
      }
    });
    it('should handle unexpected response errors with no body', async function () {
      const client = new Client(`http://localhost:${testServerPort}/404`, {});
      try {
        await drainStream(client.stream('g.V()', null));
        assert.fail('invalid status codes should throw');
      } catch (err) {
        assert.ok(err);
        assert.ok(err.message.indexOf('404') > 0);
      }
    });
    it('should extract message from JSON error response', async function () {
      const client = new Client(`http://localhost:${testServerPort}/401/json`, {});
      try {
        await drainStream(client.stream('g.V()', null));
        assert.fail('should have thrown');
      } catch (err) {
        assert.ok(err instanceof ResponseError);
        assert.strictEqual(err.statusCode, 401);
        assert.strictEqual(err.statusMessage, 'Authentication required');
      }
    });
    it('should extract error field from JSON error response', async function () {
      const client = new Client(`http://localhost:${testServerPort}/500/json-error-field`, {});
      try {
        await drainStream(client.stream('g.V()', null));
        assert.fail('should have thrown');
      } catch (err) {
        assert.ok(err instanceof ResponseError);
        assert.strictEqual(err.statusCode, 500);
        assert.strictEqual(err.statusMessage, 'Internal failure');
      }
    });
    it('should fall back to generic error for malformed JSON response', async function () {
      const client = new Client(`http://localhost:${testServerPort}/502/json-malformed`, {});
      try {
        await drainStream(client.stream('g.V()', null));
        assert.fail('should have thrown');
      } catch (err) {
        assert.ok(err instanceof ResponseError);
        assert.strictEqual(err.statusCode, 502);
        assert.ok(err.message.indexOf('502') > 0);
      }
    });
  });
});
