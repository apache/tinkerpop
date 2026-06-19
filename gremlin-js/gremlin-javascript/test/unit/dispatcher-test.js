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
import { Agent, ProxyAgent } from 'undici';
import {
  buildDispatcher,
  buildAgentOptions,
  buildKeepAliveConnector,
  resolveKeepAliveTime,
  DEFAULT_MAX_CONNECTIONS,
  DEFAULT_KEEP_ALIVE_TIME,
} from '../../lib/driver/dispatcher.js';

describe('dispatcher', function () {
  it('builds a plain Agent by default', async function () {
    const d = buildDispatcher();
    assert.ok(d instanceof Agent, 'should be an undici Agent');
    assert.ok(!(d instanceof ProxyAgent), 'should not be a ProxyAgent without a proxy');
    await d.close();
  });

  it('defaults the connection cap to 128', function () {
    assert.strictEqual(DEFAULT_MAX_CONNECTIONS, 128);
  });

  it('defaults the keep-alive idle time to 30000ms', function () {
    assert.strictEqual(DEFAULT_KEEP_ALIVE_TIME, 30000);
  });

  it('builds a plain Agent when only timeouts and header size are set', async function () {
    const d = buildDispatcher({ readTimeout: 1000, maxResponseHeaderBytes: 16384, maxConnections: 8 });
    assert.ok(d instanceof Agent);
    assert.ok(!(d instanceof ProxyAgent));
    await d.close();
  });

  it('builds a ProxyAgent when a proxy is configured', async function () {
    const d = buildDispatcher({ proxy: 'http://localhost:3128' });
    assert.ok(d instanceof ProxyAgent, 'should be an undici ProxyAgent');
    await d.close();
  });

  it('builds a single dispatcher with keep-alive wired through a custom connector', async function () {
    const d = buildDispatcher({ keepAliveTime: 30000 });
    assert.ok(d instanceof Agent);
    await d.close();
  });

  it('builds a ProxyAgent with keep-alive and timeouts together', async function () {
    const d = buildDispatcher({
      proxy: 'http://localhost:3128',
      keepAliveTime: 5000,
      readTimeout: 2000,
      maxResponseHeaderBytes: 8192,
      maxConnections: 64,
    });
    assert.ok(d instanceof ProxyAgent);
    await d.close();
  });

  describe('buildAgentOptions (undici option mapping)', function () {
    it('maps readTimeout to the undici Agent bodyTimeout', function () {
      const opts = buildAgentOptions({ readTimeout: 1234 });
      assert.strictEqual(opts.bodyTimeout, 1234);
    });

    it('maps maxResponseHeaderBytes to the undici Agent maxHeaderSize', function () {
      const opts = buildAgentOptions({ maxResponseHeaderBytes: 16384 });
      assert.strictEqual(opts.maxHeaderSize, 16384);
    });

    it('maps maxConnections to the undici Agent connections', function () {
      const opts = buildAgentOptions({ maxConnections: 8 });
      assert.strictEqual(opts.connections, 8);
    });

    it('defaults connections to DEFAULT_MAX_CONNECTIONS when unset', function () {
      const opts = buildAgentOptions();
      assert.strictEqual(opts.connections, DEFAULT_MAX_CONNECTIONS);
    });

    it('omits bodyTimeout and maxHeaderSize when their options are unset', function () {
      const opts = buildAgentOptions();
      assert.strictEqual(opts.bodyTimeout, undefined);
      assert.strictEqual(opts.maxHeaderSize, undefined);
    });

    it('maps both readTimeout and maxResponseHeaderBytes together', function () {
      const opts = buildAgentOptions({ readTimeout: 2000, maxResponseHeaderBytes: 8192 });
      assert.strictEqual(opts.bodyTimeout, 2000);
      assert.strictEqual(opts.maxHeaderSize, 8192);
    });

    it('wires a custom connect connector for keep-alive by default', function () {
      const opts = buildAgentOptions();
      assert.strictEqual(typeof opts.connect, 'function', 'keep-alive should install a connect connector');
    });

    it('omits the connect connector when keep-alive is disabled (keepAliveTime 0)', function () {
      const opts = buildAgentOptions({ keepAliveTime: 0 });
      assert.strictEqual(opts.connect, undefined);
    });
  });

  describe('resolveKeepAliveTime', function () {
    it('falls back to the 30000ms default when unset', function () {
      assert.strictEqual(resolveKeepAliveTime(undefined), DEFAULT_KEEP_ALIVE_TIME);
      assert.strictEqual(resolveKeepAliveTime(), 30000);
    });

    it('honors a user-supplied positive override', function () {
      assert.strictEqual(resolveKeepAliveTime(5000), 5000);
    });

    it('disables keep-alive (null) when set to 0 or a negative value', function () {
      assert.strictEqual(resolveKeepAliveTime(0), null);
      assert.strictEqual(resolveKeepAliveTime(-1), null);
    });
  });

  describe('buildKeepAliveConnector', function () {
    // Drives the connector with a fake base connector and socket so the applied
    // keep-alive delay can be asserted without opening a real socket.
    function fakeBaseConnector(socket, err) {
      return (_connectOptions, callback) => callback(err ?? null, err ? null : socket);
    }

    function fakeSocket() {
      const calls = [];
      return {
        calls,
        setKeepAlive(enable, delay) {
          calls.push({ enable, delay });
        },
      };
    }

    it('applies the default 30000ms delay to a freshly established socket', function (done) {
      const socket = fakeSocket();
      const delay = resolveKeepAliveTime(undefined);
      const connector = buildKeepAliveConnector(delay, fakeBaseConnector(socket));
      connector({}, (err, returned) => {
        assert.ifError(err);
        assert.strictEqual(returned, socket);
        assert.deepStrictEqual(socket.calls, [{ enable: true, delay: 30000 }]);
        done();
      });
    });

    it('applies a user-supplied override delay', function (done) {
      const socket = fakeSocket();
      const connector = buildKeepAliveConnector(resolveKeepAliveTime(7500), fakeBaseConnector(socket));
      connector({}, (err) => {
        assert.ifError(err);
        assert.deepStrictEqual(socket.calls, [{ enable: true, delay: 7500 }]);
        done();
      });
    });

    it('propagates connector errors without touching the socket', function (done) {
      const boom = new Error('connect failed');
      const connector = buildKeepAliveConnector(30000, fakeBaseConnector(null, boom));
      connector({}, (err, returned) => {
        assert.strictEqual(err, boom);
        assert.strictEqual(returned, null);
        done();
      });
    });
  });
});
