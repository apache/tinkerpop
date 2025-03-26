/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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

const assert = require('assert');
const http = require('http');
const url = require('url');
const helper = require('../helper');

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
    res.statusCode = 404;
    res.end();
  });
  before(function () {
    return new Promise((resolve) => server.listen(testServerPort, resolve));
  });
  after(function () {
    return server.close();
  });

  describe('#open()', function () {
    it('should use the ws WebSocket when ws specific options are provided', function () {
      let globalWebsocketCalls = 0;
      globalThis.WebSocket = function () {
        globalWebsocketCalls++;
      };
      const wsSpecificOptions = [
        'headers',
        'ca',
        'cert',
        'pfx',
        'rejectUnauthorized',
        'agent',
        'perMessageDeflate',
      ];
      const allOptionTests = wsSpecificOptions.map((wsOption) => {
        const connection = helper.getDriverRemoteConnection(`ws://localhost:${testServerPort}/401`, {
          [wsOption]: 'this option is set',
        });
        return connection.open();
      });
      return Promise.allSettled(allOptionTests).then(function () {
        assert.equal(globalWebsocketCalls, 0, 'global WebSocket should be used when no ws specific options are provided');
      });
    });
    it('should use the global WebSocket when options are not provided', function () {
      let globalWebsocketCalls = 0;
      globalThis.WebSocket = function () {
        globalWebsocketCalls++;
      };
      const connection = helper.getDriverRemoteConnection(`ws://localhost:${testServerPort}/401`);
      return connection
        .open()
        .catch(() => {})
        .finally(function () {
          assert.equal(globalWebsocketCalls, 1, 'global WebSocket should be used when no ws specific options are provided');
        });
    });
    it('should handle unexpected response errors with body', function () {
      globalThis.WebSocket = http.WebSocket;
      const connection = helper.getDriverRemoteConnection(`ws://localhost:${testServerPort}/401`);
      return connection
        .open()
        .then(function () {
          assert.fail('invalid status codes should throw');
        })
        .catch(function (err) {
          assert.ok(err);
          assert.ok(err.message.indexOf(401) > 0);
          assert.ok(err.message.indexOf(testServer401ResponseBody) > 0);
        });
    });
    it('should handle unexpected response errors with no body', function () {
      globalThis.WebSocket = undefined;
      const connection = helper.getDriverRemoteConnection(`ws://localhost:${testServerPort}/404`);
      return connection
        .open()
        .then(function () {
          assert.fail('invalid status codes should throw');
        })
        .catch(function (err) {
          assert.ok(err);
          assert.ok(err.message.indexOf(404) > 0);
          assert.ok(err.message.indexOf('body') < 0);
        });
    });
  });
});
