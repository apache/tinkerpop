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
import anon from '../../lib/process/anonymous-traversal.js';
import { basic } from '../../lib/driver/auth.js';
import { getAuthenticatedClient, getAuthenticatedConnection } from '../helper.js';

/**
 * Integration tests for basic authentication.
 *
 * These tests require a Gremlin Server running with authentication enabled
 * on the auth URL (default: https://localhost:45941/gremlin).
 *
 * To run:
 *   NODE_TLS_REJECT_UNAUTHORIZED=0 RUN_BASIC_AUTH_INTEGRATION_TESTS=true npm run integration-test
 */
describe('Basic Auth Integration', function () {
  const runAuthTests = process.env.RUN_BASIC_AUTH_INTEGRATION_TESTS === 'true';

  before(function () {
    if (!runAuthTests) {
      this.skip();
    }
  });

  describe('Client with basic auth', function () {
    let client;

    before(function () {
      client = getAuthenticatedClient('gmodern', basic('stephen', 'password'));
      return client.open();
    });

    after(function () {
      return client.close();
    });

    it('should authenticate and submit a traversal', async function () {
      const result = await client.submit('g.V().count()');
      assert.ok(result);
      assert.strictEqual(result.first(), 6);
    });
  });

  describe('DriverRemoteConnection with basic auth', function () {
    let connection;

    before(function () {
      connection = getAuthenticatedConnection('gmodern', basic('stephen', 'password'));
      return connection.open();
    });

    after(function () {
      return connection.close();
    });

    it('should authenticate and execute traversal', async function () {
      const g = anon.traversal().with_(connection);
      const count = await g.V().count().toList();
      assert.ok(count);
      assert.strictEqual(count[0], 6);
    });
  });
});
