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
import { basic, sigv4 } from '../../lib/driver/auth.browser.js';
import { HttpRequest } from '../../lib/driver/http-request.js';

describe('auth (browser)', function () {
  describe('basic', function () {
    it('should encode credentials the same way as the Node build', function () {
      const request = new HttpRequest('POST', 'https://localhost:8182/gremlin', {}, Buffer.from(''));
      const interceptor = basic('username', 'password');
      interceptor(request);

      const encoded = request.headers['authorization'].substring('Basic '.length);
      assert.strictEqual(Buffer.from(encoded, 'base64').toString(), 'username:password');
    });
  });

  describe('sigv4', function () {
    function createMockRequest() {
      return new HttpRequest('POST', 'https://example.neptune.amazonaws.com:8182/gremlin', {
        'accept': 'application/vnd.graphbinary-v4.0',
      }, Buffer.from(''));
    }

    const mockProvider = () => ({
      accessKeyId: 'MOCK_ACCESS_KEY',
      secretAccessKey: 'MOCK_SECRET_KEY',
    });

    it('requires a credentialsProvider, unlike the Node build', function () {
      assert.throws(() => sigv4('us-east-1', 'neptune-db'), (err) => {
        assert.ok(err instanceof Error);
        assert.ok(/requires a credentialsProvider/i.test(err.message));
        return true;
      });
    });

    it('should add signed headers', async function () {
      const request = createMockRequest();
      assert.strictEqual(request.headers['authorization'], undefined);

      const interceptor = sigv4('us-east-1', 'test-service', mockProvider);
      await interceptor(request);

      assert.ok(request.headers['x-amz-date']);
      const authHeader = request.headers['authorization'];
      assert.ok(authHeader.startsWith('AWS4-HMAC-SHA256 Credential=MOCK_ACCESS_KEY'));
      assert.ok(authHeader.includes('us-east-1/test-service/aws4_request'));
      assert.ok(authHeader.includes('Signature='));
    });

    it('should add session token when provided', async function () {
      const request = createMockRequest();
      const providerWithToken = () => ({
        accessKeyId: 'MOCK_ACCESS_KEY',
        secretAccessKey: 'MOCK_SECRET_KEY',
        sessionToken: 'MOCK_SESSION_TOKEN',
      });

      const interceptor = sigv4('us-east-1', 'test-service', providerWithToken);
      await interceptor(request);

      assert.strictEqual(request.headers['x-amz-security-token'], 'MOCK_SESSION_TOKEN');
    });

    it('produces the same signature as the Node build for an identical request', async function () {
      const RealDate = Date;
      class FixedDate extends RealDate {
        constructor(...args) {
          if (args.length === 0) {
            super('2024-01-01T00:00:00Z');
          } else {
            super(...args);
          }
        }
        static now() {
          return new RealDate('2024-01-01T00:00:00Z').getTime();
        }
      }
      const OriginalDate = globalThis.Date;
      globalThis.Date = FixedDate;

      try {
        const request = new HttpRequest(
          'POST',
          'https://example.neptune.amazonaws.com/gremlin',
          {},
          Buffer.from('{"gremlin":"g.V()"}'),
        );
        const interceptor = sigv4('us-east-1', 'neptune-db', mockProvider);
        await interceptor(request);

        // Computed independently via lib/driver/auth.ts (the Node build, @smithy/hash-node) for the
        // same fixed date/credentials/body - a byte-for-byte match confirms the browser hash
        // (@aws-crypto/sha256-browser) agrees with the Node hash, not just that both run without error.
        assert.strictEqual(
          request.headers['authorization'],
          'AWS4-HMAC-SHA256 Credential=MOCK_ACCESS_KEY/20240101/us-east-1/neptune-db/aws4_request, ' +
          'SignedHeaders=host;x-amz-content-sha256;x-amz-date, ' +
          'Signature=08b4bbdacdbc6603946dbdcc8465e1a87caa517f38fe21f14cb7cfaaf64dd824',
        );
      } finally {
        globalThis.Date = OriginalDate;
      }
    });
  });
});
