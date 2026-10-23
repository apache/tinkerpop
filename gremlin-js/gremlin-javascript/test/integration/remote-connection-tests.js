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
 * @author Jorge Bay Gondra
 */

import assert from 'assert';
import { Vertex } from '../../lib/structure/graph.js';
import anon from '../../lib/process/anonymous-traversal.js';
import { getConnection, getClient, getDriverRemoteConnection, serverUrl } from '../helper.js';
import {P} from "../../lib/process/traversal.js";

let connection;
let client;

describe('DriverRemoteConnection', function () {
  before(function () {
    connection = getConnection('gmodern');
    client = getClient('gmodern');
    return connection.open();
  });
  after(function () {
    client.close();
    return connection.close();
  });

  describe('#submit()', function () {
    it('should send the request and parse the response', function () {
      const g = anon.traversal().with_(connection);
      return g.V().tail().toList()
        .then(function (list) {
          assert.ok(list);
          assert.strictEqual(list.length, 1);
          assert.ok(list[0] instanceof Vertex);
        });
    });
    it('should send the request with syntax error and parse the response error', function () {
      return client.submit('SYNTAX_ERROR')
        .then(function() {
          assert.fail("syntax error should throw");
        })
        .catch(function (err) {
          assert.ok(err);
          assert.ok(err.statusCode === 400);
          assert.ok(err.statusException);
        });
    });
    describe('language', function () {
      const notAvailable = err => {
        assert.strictEqual(err.statusCode, 400);
        assert.strictEqual(err.statusException, 'InvalidRequestException');
        assert.match(err.statusMessage, /Script engine \[no-such-engine\] is not available/);
        return true;
      };

      async function withLanguageConnection(language, fn) {
        const conn = getDriverRemoteConnection(serverUrl, { traversalSource: 'gmodern', language });
        try {
          return await fn(anon.traversal().with_(conn));
        } finally {
          await conn.close();
        }
      }

      it('should use the connection-level language', async function () {
        const list = await withLanguageConnection('gremlin-lang', g => g.inject(1).toList());
        assert.deepStrictEqual(list, [1]);
      });
      it('should send the connection-level language to the server', async function () {
        await assert.rejects(withLanguageConnection('no-such-engine', g => g.inject(1).toList()), notAvailable);
      });
      it('should send the with_() language to the server', async function () {
        await assert.rejects(
          withLanguageConnection(undefined, g => g.with_('language', 'no-such-engine').inject(1).toList()),
          notAvailable);
      });
      it('should let with_() override the connection-level language', async function () {
        const list = await withLanguageConnection('no-such-engine',
          g => g.with_('language', 'gremlin-lang').inject(1).toList());
        assert.deepStrictEqual(list, [1]);
      });
    });
    it('should iterate ordered results', async function () {
      const g = anon.traversal().with_(connection);
      const traversal = g.V().has('name', P.within('marko', 'peter')).values('name').order();
      const first = await traversal.next();
      assert.strictEqual(first.value, 'marko');
      assert.strictEqual(await traversal.hasNext(), true);
      const second = await traversal.next();
      assert.strictEqual(second.value, 'peter');
      assert.strictEqual(await traversal.hasNext(), false);
    });
  });
});
