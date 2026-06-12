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
import { HttpRequest } from '../../lib/driver/http-request.js';

describe('auth', function () {
  describe('basic', function () {
    function createMockRequest() {
      return new HttpRequest('POST', 'https://localhost:8182/gremlin', {
        'accept': 'application/vnd.graphbinary-v4.0',
      }, Buffer.from(''));
    }

    it('should add authorization header', function () {
      const request = createMockRequest();
      assert.strictEqual(request.headers['authorization'], undefined);

      const interceptor = basic('username', 'password');
      interceptor(request);

      assert.ok(request.headers['authorization'].startsWith('Basic '));
    });

    it('should encode credentials correctly', function () {
      const request = createMockRequest();
      const interceptor = basic('username', 'password');
      interceptor(request);

      const encoded = request.headers['authorization'].substring('Basic '.length);
      const decoded = Buffer.from(encoded, 'base64').toString();
      assert.strictEqual(decoded, 'username:password');
    });
  });

  describe('sigv4', function () {
    function createMockRequest() {
      return new HttpRequest('POST', 'https://localhost:8182/gremlin', {
        'accept': 'application/vnd.graphbinary-v4.0',
        'content-type': 'application/json',
      }, Buffer.from('{"gremlin":"g.V()"}'));
    }

    const mockProvider = () => ({
      accessKeyId: 'MOCK_ACCESS_KEY',
      secretAccessKey: 'MOCK_SECRET_KEY',
    });

    it('should add signed headers', async function () {
      const request = createMockRequest();
      assert.strictEqual(request.headers['authorization'], undefined);

      const interceptor = sigv4('xx-dummy-1', 'test-service', mockProvider);
      await interceptor(request);

      assert.ok(request.headers['x-amz-date']);
      const authHeader = request.headers['authorization'];
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
      await interceptor(request);

      assert.strictEqual(request.headers['x-amz-security-token'], 'MOCK_SESSION_TOKEN');
      const authHeader = request.headers['authorization'];
      assert.ok(authHeader.startsWith('AWS4-HMAC-SHA256 Credential='));
      assert.ok(authHeader.includes('Signature='));
    });

    it('should preserve pre-existing headers after signing', async function () {
      const request = createMockRequest();
      // Capture the headers present before signing (accept + content-type)
      const preSignKeys = Object.keys(request.headers);

      const interceptor = sigv4('xx-dummy-1', 'test-service', mockProvider);
      await interceptor(request);

      // The original headers must still be present (not dropped by wholesale replacement)
      for (const key of preSignKeys) {
        assert.ok(
          key in request.headers,
          `expected header '${key}' to be preserved after signing`);
      }
      assert.strictEqual(request.headers['accept'], 'application/vnd.graphbinary-v4.0');
      assert.strictEqual(request.headers['content-type'], 'application/json');

      // Signing adds at least authorization and x-amz-date on top of the originals
      assert.ok(Object.keys(request.headers).length >= preSignKeys.length + 2);
    });
  });
});
