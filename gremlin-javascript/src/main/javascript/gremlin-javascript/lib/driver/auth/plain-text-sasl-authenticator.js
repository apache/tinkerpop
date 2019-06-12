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

const Authenticator = require('./authenticator');
const SaslMechanismPlain = require('./mechanisms/sasl-mechanism-plain');

class PlainTextSaslAuthenticator extends Authenticator {
  /**
   * Creates a new instance of PlainTextSaslAuthenticator.
   * @param {string} username Username to log into the server.
   * @param {string} password Password for the user.
   * @param {string} [authzid] Optional id
   * @constructor
   */
  constructor(username, password, authzid) {
    const options = {
      mechanism: new SaslMechanismPlain({
        'username': username,
        'password': password,
        'authzid': authzid
      })
    };

    super(options);
  }
  
  /**
   * Evaluates the challenge from the server and returns appropriate response.
   * @param {String} challenge Challenge string presented by the server.
   * @return {Object} A Promise that resolves to a valid sasl response object.
   */
  evaluateChallenge(challenge) {
    return this._options.mechanism.evaluateChallenge(challenge);
  }
}

module.exports = PlainTextSaslAuthenticator;
