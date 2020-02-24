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
 * @author : Liu Jianping
 */

'use strict';

const assert = require('assert');
const Bytecode = require('../../lib/process/bytecode');
const graphModule = require('../../lib/structure/graph');
const helper = require('../helper');

let client;

describe('Client', function () {
  before(function () {
    client = helper.getSessionClient('g');
    return client.open();
  });
  after(function () {
    return client.close();
  });

  describe('#submit()', function () {
    it('should send bytecode and response error', function () {
      return client.submit(new Bytecode().addStep('V', []).addStep('tail', []))
        .catch(function (err) {
          assert.ok(err);
          assert.ok(err.message.indexOf('session') > 0);
        });
    });

    it('should use golbal cache in session', function () {
      return client.submit("x = [0, 1, 2, 3, 4, 5]")
        .then(function (result) {
          assert.ok(result);
          assert.strictEqual(result.length, 6);
          //console.log("x : %s", JSON.stringify(result));
        }).then(function () {
          client.submit("x[2] + 4")
            .then(function(result) {
              assert.ok(result);
              assert.strictEqual(result.length, 1);
              assert.strictEqual(result.first(), 6);
              //console.log("x[2] + 4: %s", JSON.stringify(result));
            });
        });
    });

    it('should use bindings and golbal cache variable in session', function () {
      return client.submit('x[3] + y', { y: 3 })
        .then(function (result) {
          assert.ok(result);
          assert.strictEqual(result.length, 1);
          assert.strictEqual(result.first(), 6);
          //console.log("x[3] + y: %s", JSON.stringify(result));
        });
    });

  });
});
