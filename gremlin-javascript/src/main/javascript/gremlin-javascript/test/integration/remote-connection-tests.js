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
import { getConnection, getClient } from '../helper.js';

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
          assert.ok(err.statusAttributes);
          assert.ok(err.statusAttributes.has('exceptions'));
          assert.ok(err.statusAttributes.has('stackTrace'));
        });
    });
  });
});
