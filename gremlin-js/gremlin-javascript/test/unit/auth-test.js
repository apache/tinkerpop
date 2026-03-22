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
import { Buffer } from 'buffer';
import { basic, sigv4 } from '../../lib/driver/auth.js';

describe('auth', function () {
  describe('basic', function () {
    function createMockRequest() {
      return {
        url: 'https://localhost:8182/gremlin',
        method: 'POST',
        headers: {
          'accept': 'application/vnd.graphbinary-v4.0',
        },
        body: new Uint8Array(0),
      };
    }

    it('should add authorization header', function () {
      const request = createMockRequest();
      assert.strictEqual(request.headers['authorization'], undefined);

      const interceptor = basic('username', 'password');
      const result = interceptor(request);

      assert.ok(result.headers['authorization'].startsWith('Basic '));
    });

    it('should encode credentials correctly', function () {
      const request = createMockRequest();
      const interceptor = basic('username', 'password');
      const result = interceptor(request);

      const encoded = result.headers['authorization'].substring('Basic '.length);
      const decoded = Buffer.from(encoded, 'base64').toString();
      assert.strictEqual(decoded, 'username:password');
    });

    it('should return the same request object', function () {
      const request = createMockRequest();
      const interceptor = basic('username', 'password');
      const result = interceptor(request);

      assert.strictEqual(result, request);
    });
  });

  describe('sigv4', function () {
    function createMockRequest() {
      return {
        url: 'https://localhost:8182/gremlin',
        method: 'POST',
        headers: {
          'accept': 'application/vnd.graphbinary-v4.0',
        },
        body: new Uint8Array(Buffer.from('{"gremlin":"g.V()"}')),
      };
    }

    const mockProvider = () => ({
      accessKeyId: 'MOCK_ACCESS_KEY',
      secretAccessKey: 'MOCK_SECRET_KEY',
    });

    it('should add signed headers', async function () {
      const request = createMockRequest();
      assert.strictEqual(request.headers['authorization'], undefined);

      const interceptor = sigv4('xx-dummy-1', 'test-service', mockProvider);
      const result = await interceptor(request);

      assert.ok(result.headers['x-amz-date']);
      const authHeader = result.headers['authorization'];
      assert.ok(authHeader.startsWith('AWS4-HMAC-SHA256 Credential=MOCK_ACCESS_KEY'));
      assert.ok(authHeader.includes('xx-dummy-1/test-service/aws4_request'));
      assert.ok(authHeader.includes('Signature='));
    });

    it('should add session token when provided', async function () {
      const request = createMockRequest();
      const providerWithToken = () => ({
        accessKeyId: 'MOCK_ACCESS_KEY',
        secretAccessKey: 'MOCK_SECRET_KEY',
        sessionToken: 'MOCK_SESSION_TOKEN',
      });

      const interceptor = sigv4('xx-dummy-1', 'test-service', providerWithToken);
      const result = await interceptor(request);

      assert.strictEqual(result.headers['x-amz-security-token'], 'MOCK_SESSION_TOKEN');
      const authHeader = result.headers['authorization'];
      assert.ok(authHeader.startsWith('AWS4-HMAC-SHA256 Credential='));
      assert.ok(authHeader.includes('Signature='));
    });

    it('should return the same request object', async function () {
      const request = createMockRequest();
      const interceptor = sigv4('xx-dummy-1', 'test-service', mockProvider);
      const result = await interceptor(request);

      assert.strictEqual(result, request);
    });
  });
});
