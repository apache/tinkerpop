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
 * @author Jorge Bay Gondra
 */
'use strict';

var assert = require('assert');
var Bytecode = require('../../lib/process/bytecode');
var DriverRemoteConnection = require('../../lib/driver/driver-remote-connection');
var graphModule = require('../../lib/structure/graph');

var connection;

describe('DriverRemoteConnection', function () {
  before(function () {
    connection = new DriverRemoteConnection('ws://localhost:45950/gremlin');
    return connection.open();
  });
  after(function () {
    return connection.close();
  });
  describe('#submit()', function () {
    it('should send the request and parse the response', function () {
      return connection.submit(new Bytecode().addStep('addV', [ 'person' ]).addStep('property', [ 'name', 'marko']))
        .then(function (response) {
          assert.ok(response);
          assert.ok(response.traversers);
          assert.strictEqual(response.traversers.length, 1);
          assert.ok(response.traversers[0].object instanceof graphModule.Vertex);
        });
    });
    it('should send the request with syntax error and parse the response error', function () {
      return connection.submit(new Bytecode().addStep('SYNTAX_ERROR'))
        .catch(function (err) {
          assert.ok(err);
          assert.ok(err.message.indexOf('599') > 0);
        });
    });
  });
});

function delay(ms) {
  return new Promise(function (resolve) {
    setTimeout(resolve, ms);
  });
}