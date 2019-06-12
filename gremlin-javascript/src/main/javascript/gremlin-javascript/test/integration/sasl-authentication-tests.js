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
const helper = require('../helper');

let connection;

describe('DriverRemoteConnection', function () {
  context('with PlainTextSaslAuthenticator', function () {
    this.timeout(20000);

    afterEach(function () {
      return connection.close();
    });

    describe('#submit()', function () {
      it('should send the request with valid credentials and parse the response', function () {
        connection = helper.getSecureConnectionWithPlainTextSaslAuthenticator(null, 'stephen', 'password');

        return connection.submit(new Bytecode().addStep('V', []).addStep('tail', []))
          .then(function (response) {
            assert.ok(response);
            assert.ok(response.traversers);
          });
      });

      it('should send the request with invalid credentials and parse the response error', function () {
        connection = helper.getSecureConnectionWithPlainTextSaslAuthenticator(null, 'Bob', 'password');

        return connection.submit(new Bytecode().addStep('V', []).addStep('tail', []))
          .catch(function (err) {
            assert.ok(err);
            assert.ok(err.message.indexOf('401') > 0);
          });
      });
    });
  });
});
