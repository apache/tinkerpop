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

'use strict';

const assert = require('assert');
const Bytecode = require('../../lib/process/bytecode');
const graphModule = require('../../lib/structure/graph');
const helper = require('../helper');
const t = require('../../lib/process/traversal');

let client;

describe('Client', function () {
  before(function () {
    client = helper.getClient('gmodern');
    return client.open();
  });
  after(function () {
    return client.close();
  });
  describe('#submit()', function () {
    it('should send bytecode', function () {
      return client.submit(new Bytecode().addStep('V', []).addStep('tail', []))
        .then(function (result) {
          assert.ok(result);
          assert.strictEqual(result.length, 1);
          assert.ok(result.first().object instanceof graphModule.Vertex);
        });
    });
    it('should send and parse a script', function () {
      return client.submit('g.V().tail()')
        .then(function (result) {
          assert.ok(result);
          assert.strictEqual(result.length, 1);
          assert.ok(result.first() instanceof graphModule.Vertex);
        });
    });
    it('should send and parse a script with bindings', function () {
      return client.submit('x + x', { x: 3 })
        .then(function (result) {
          assert.ok(result);
          assert.strictEqual(result.first(), 6);
        });
    });
    it('should send and parse a script with non-native javascript bindings', function () {
      return client.submit('card.class.simpleName + ":" + card', { card: t.cardinality.set } )
        .then(function (result) {
          assert.ok(result);
          assert.strictEqual(result.first(), 'Cardinality:set');
        });
    });

    it('should retrieve the attributes', () => {
      return client.submit(new Bytecode().addStep('V', []).addStep('tail', []))
        .then(rs => {
          assert.ok(rs.attributes instanceof Map);
          assert.ok(rs.attributes.get('host'));
        });
    });
  });
});