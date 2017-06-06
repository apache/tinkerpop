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
var graphModule = require('../../lib/structure/graph');
var Graph = graphModule.Graph;
var Vertex = graphModule.Vertex;
var utils = require('../../lib/utils');
var t = require('../../lib/process/traversal');
var TraversalStrategies = require('../../lib/process/traversal-strategy').TraversalStrategies;
var DriverRemoteConnection = require('../../lib/driver/driver-remote-connection');

var connection;

describe('Traversal', function () {
  before(function () {
    connection = new DriverRemoteConnection('ws://localhost:45950/gremlin');
    return connection.open();
  });
  after(function () {
    return connection.close();
  });
  describe('#toList()', function () {
    it('should submit the traversal and return a list', function () {
      var g = new Graph().traversal().withRemote(connection);
      return g.addV('user').toList().then(function (list) {
        assert.ok(list);
        assert.strictEqual(list.length, 1);
        assert.ok(list[0] instanceof Vertex);
      });
    });
  });
  describe('#next()', function () {
    it('should submit the traversal and return an iterator', function () {
      var g = new Graph().traversal().withRemote(connection);
      var traversal = g.V().count();
      return traversal.next()
        .then(function (item) {
          assert.ok(item);
          assert.strictEqual(item.done, false);
          assert.strictEqual(typeof item.value, 'number');
          return traversal.next();
        }).then(function (item) {
          assert.ok(item);
          assert.strictEqual(item.done, true);
          assert.strictEqual(item.value, null);
        });
    });
  });
});