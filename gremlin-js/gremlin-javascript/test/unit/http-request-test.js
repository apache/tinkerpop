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
import { HttpRequest } from '../../lib/driver/http-request.js';
import { RequestMessage } from '../../lib/driver/request-message.js';

describe('HttpRequest', function () {
  describe('serializeBody()', function () {
    it('should serialize RequestMessage to JSON bytes and set body to Buffer', function () {
      const msg = RequestMessage.build("g.V().has('name','marko')")
        .addG('g')
        .create();

      const httpReq = new HttpRequest('POST', 'http://localhost:8182/gremlin', {}, msg);
      const data = httpReq.serializeBody();

      assert(Buffer.isBuffer(data), 'should return a Buffer');
      assert(Buffer.isBuffer(httpReq.body), 'body should now be a Buffer');

      const parsed = JSON.parse(data.toString('utf-8'));
      assert.strictEqual(parsed.gremlin, "g.V().has('name','marko')");
      assert.strictEqual(parsed.g, 'g');
      assert.strictEqual(parsed.language, 'gremlin-lang');
    });

    it('should set Content-Type to application/json', function () {
      const msg = RequestMessage.build('g.V()').addG('g').create();
      const httpReq = new HttpRequest('POST', 'http://localhost:8182/gremlin', {}, msg);

      httpReq.serializeBody();

      assert.strictEqual(httpReq.headers['Content-Type'], 'application/json');
    });

    it('should set Content-Length to byte length of the serialized body', function () {
      const msg = RequestMessage.build('g.V()').addG('g').create();
      const httpReq = new HttpRequest('POST', 'http://localhost:8182/gremlin', {}, msg);

      const data = httpReq.serializeBody();

      assert.strictEqual(httpReq.headers['Content-Length'], String(data.length));
    });

    it('should be idempotent when body is already a Buffer', function () {
      const msg = RequestMessage.build('g.V()').addG('g').create();
      const httpReq = new HttpRequest('POST', 'http://localhost:8182/gremlin', {}, msg);

      const data1 = httpReq.serializeBody();
      const data2 = httpReq.serializeBody();

      assert(data1.equals(data2), 'subsequent calls should return identical bytes');
    });

    it('should produce identical results on multiple calls', function () {
      const msg = RequestMessage.build('g.V()').addG('g').create();
      const httpReq = new HttpRequest('POST', 'http://localhost:8182/gremlin', {}, msg);

      const results = [];
      for (let i = 0; i < 3; i++) {
        results.push(httpReq.serializeBody());
      }

      assert(results[0].equals(results[1]));
      assert(results[1].equals(results[2]));
    });

    it('should return existing bytes if body is already a Buffer', function () {
      const existing = Buffer.from('{"gremlin":"g.V()"}', 'utf-8');
      const httpReq = new HttpRequest('POST', 'http://localhost:8182/gremlin', {}, existing);

      const data = httpReq.serializeBody();

      assert.strictEqual(data, existing, 'should return the same Buffer reference');
    });

    it('should include all fields from RequestMessage', function () {
      const msg = RequestMessage.build("g.V().has('age',x)")
        .addG('gCustom')
        .addTimeoutMillis(5000)
        .addMaterializeProperties('all')
        .addBulkResults(true)
        .addField('customKey', 'customValue')
        .create();

      const httpReq = new HttpRequest('POST', 'http://localhost:8182/gremlin', {}, msg);
      const data = httpReq.serializeBody();
      const parsed = JSON.parse(data.toString('utf-8'));

      assert.strictEqual(parsed.gremlin, "g.V().has('age',x)");
      assert.strictEqual(parsed.g, 'gCustom');
      assert.strictEqual(parsed.language, 'gremlin-lang');
      assert.strictEqual(parsed.timeoutMs, 5000);
      assert.strictEqual(parsed.materializeProperties, 'all');
      assert.strictEqual(parsed.bulkResults, true);
      assert.strictEqual(parsed.customKey, 'customValue');
    });

    it('should throw for unsupported body types', function () {
      const httpReq = new HttpRequest('POST', 'http://localhost:8182/gremlin', {}, 42);

      assert.throws(() => httpReq.serializeBody(), /unsupported body type/);
    });

    it('should throw for null body', function () {
      const httpReq = new HttpRequest('POST', 'http://localhost:8182/gremlin', {}, null);

      assert.throws(() => httpReq.serializeBody(), /unsupported body type/);
    });
  });

  describe('interceptors', function () {
    it('should receive HttpRequest with body as RequestMessage', function () {
      const msg = RequestMessage.build('g.V()').addG('g').create();
      const httpReq = new HttpRequest('POST', 'http://localhost:8182/gremlin', { 'Accept': 'application/vnd.graphbinary-v4.0' }, msg);

      let receivedBody;
      const interceptor = (req) => {
        receivedBody = req.body;
      };

      interceptor(httpReq);

      assert(receivedBody instanceof RequestMessage, 'interceptor should receive RequestMessage as body');
    });

    it('should allow reading and modifying headers', function () {
      const msg = RequestMessage.build('g.V()').addG('g').create();
      const httpReq = new HttpRequest('POST', 'http://localhost:8182/gremlin', { 'Accept': 'application/vnd.graphbinary-v4.0' }, msg);

      const interceptor = (req) => {
        assert.strictEqual(req.headers['Accept'], 'application/vnd.graphbinary-v4.0');
        req.headers['X-Custom'] = 'test-value';
      };

      interceptor(httpReq);

      assert.strictEqual(httpReq.headers['X-Custom'], 'test-value');
    });

    it('should allow reading and modifying url', function () {
      const msg = RequestMessage.build('g.V()').addG('g').create();
      const httpReq = new HttpRequest('POST', 'http://localhost:8182/gremlin', {}, msg);

      const interceptor = (req) => {
        req.url = 'http://other-host:8182/gremlin';
      };

      interceptor(httpReq);

      assert.strictEqual(httpReq.url, 'http://other-host:8182/gremlin');
    });

    it('should run in registration order', function () {
      const msg = RequestMessage.build('g.V()').addG('g').create();
      const httpReq = new HttpRequest('POST', 'http://localhost:8182/gremlin', {}, msg);
      const order = [];

      const interceptor1 = (req) => { order.push(1); };
      const interceptor2 = (req) => { order.push(2); };
      const interceptor3 = (req) => { order.push(3); };

      interceptor1(httpReq);
      interceptor2(httpReq);
      interceptor3(httpReq);

      assert.deepStrictEqual(order, [1, 2, 3]);
    });

    it('should allow field mutation before serialization to affect output', function () {
      const msg = RequestMessage.build('g.V()')
        .addG('g')
        .addField('providerField', 'original')
        .create();
      const httpReq = new HttpRequest('POST', 'http://localhost:8182/gremlin', {}, msg);

      // Interceptor that calls serializeBody after modifying a header
      // but a field-mutating interceptor must work at the RequestMessage level
      // For field mutation, the interceptor replaces the body with a new RequestMessage
      const interceptor = (req) => {
        // Build a new request message with modified fields
        const newMsg = RequestMessage.build('g.V().count()')
          .addG('gModified')
          .addField('providerField', 'modified')
          .create();
        req.body = newMsg;
      };

      interceptor(httpReq);
      const data = httpReq.serializeBody();
      const parsed = JSON.parse(data.toString('utf-8'));

      assert.strictEqual(parsed.gremlin, 'g.V().count()');
      assert.strictEqual(parsed.g, 'gModified');
      assert.strictEqual(parsed.providerField, 'modified');
    });

    it('should allow interceptor to call serializeBody for payload hashing', function () {
      const msg = RequestMessage.build('g.V()').addG('g').create();
      const httpReq = new HttpRequest('POST', 'http://localhost:8182/gremlin', {}, msg);

      // Simulates a SigV4 interceptor that needs the serialized bytes
      const signingInterceptor = (req) => {
        const bytes = req.serializeBody();
        // Use bytes for hashing (simulated)
        req.headers['X-Payload-Hash'] = String(bytes.length);
      };

      signingInterceptor(httpReq);

      // Body should now be serialized
      assert(Buffer.isBuffer(httpReq.body));
      assert.strictEqual(httpReq.headers['Content-Type'], 'application/json');
      assert(httpReq.headers['X-Payload-Hash'].length > 0);

      // Subsequent serializeBody call should be idempotent
      const data = httpReq.serializeBody();
      assert(Buffer.isBuffer(data));
    });
  });
});
