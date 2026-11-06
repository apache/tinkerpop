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
import { buildDispatcher } from '../../lib/driver/dispatcher.browser.js';

describe('dispatcher (browser)', function () {
  it('returns undefined when no transport options are set', function () {
    assert.strictEqual(buildDispatcher(), undefined);
    assert.strictEqual(buildDispatcher({}), undefined);
  });

  it('returns undefined when only readTimeoutMillis is set (handled by the connection, not the dispatcher)', function () {
    assert.strictEqual(buildDispatcher({ readTimeoutMillis: 5000 }), undefined);
  });

  for (const [key, value] of [
    ['maxConnections', 10],
    ['keepAliveTimeMillis', 15000],
    ['maxResponseHeaderBytes', 8192],
    ['proxy', 'http://proxy.local:8080'],
  ]) {
    it(`throws when ${key} is set explicitly`, function () {
      assert.throws(() => buildDispatcher({ [key]: value }), (err) => {
        assert.ok(err instanceof Error);
        assert.ok(err.message.includes(key), `message should name ${key}`);
        return true;
      });
    });
  }

  it('lists every unsupported option that was set', function () {
    assert.throws(
      () => buildDispatcher({ maxConnections: 10, proxy: 'http://p:1', readTimeoutMillis: 1000 }),
      (err) => {
        assert.ok(err.message.includes('maxConnections'));
        assert.ok(err.message.includes('proxy'));
        assert.ok(!err.message.includes('readTimeoutMillis'), 'readTimeoutMillis is supported and must not be listed');
        return true;
      },
    );
  });
});
