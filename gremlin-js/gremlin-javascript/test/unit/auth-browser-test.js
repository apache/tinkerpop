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
    it('throws immediately instead of silently failing on first request', function () {
      assert.throws(() => sigv4('us-east-1', 'neptune-db'), (err) => {
        assert.ok(err instanceof Error);
        assert.ok(/not supported in the browser/i.test(err.message));
        return true;
      });
    });
  });
});
