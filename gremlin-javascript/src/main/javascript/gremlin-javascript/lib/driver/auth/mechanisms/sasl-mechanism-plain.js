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

const SaslMechanismBase = require('./sasl-mechanism-base');

class SaslMechanismPlain extends SaslMechanismBase {
  /**
   * Creates a new instance of SaslMechanismPlain.
   * @param {Object} [options] The mechanism options.
   * @param {String} [options.authzid] The identity of the client.
   * @param {String} [options.username] The identity of user with access to server.
   * @param {String} [options.password] The password of user with access to server.
   * @constructor
   */
  constructor(options) {
    super(options);

    if (this._options.username === undefined
      || this._options.username === null
      || this._options.username.length == 0
      || this._options.password === undefined
      || this._options.password === null
      || this._options.password.length == 0
    ) {
      throw new Error('Missing credentials for SASL PLAIN mechanism');
    }
  }

  /**
   * Returns the name of the mechanism
   */
  get name() {
    return 'PLAIN';
  }
  
  /**
   * Evaluates the challenge from the server and returns appropriate response.
   * @param {String} challenge Challenge string presented by the server.
   * @return {Object} A Promise that resolves to a valid sasl response object.
   */
  evaluateChallenge(challenge) {
    if (this._hasInitialResponse(challenge)) {
      return Promise.resolve({
        'saslMechanism': this.name,
        'sasl': this._saslArgument(this._options.authzid, this._options.username, this._options.password)
      });
    }
    
    return Promise.resolve({ 'sasl': this._saslArgument(this._options.authzid, this._options.username, this._options.password) });
  }

  /**
   * Generates a base64 encoded sasl argument based on the given parameters.
   * @param {String} authzid Identitiy of the client.
   * @param {String} username The identity of user with access to server.
   * @param {String} password The password of user with access to server.
   */
  _saslArgument(authzid, username, password) {
    if (authzid === undefined || authzid === null) authzid = '';
    if (username === undefined || username === null) username = '';
    if (password === undefined || password.length === null) password = '';

    return new Buffer(`${authzid}\0${username}\0${password}`).toString('base64');
  }

  /**
   * Checks challenge to see if we have the initial sasl response from the server.
   * @param {String} challenge The challenge string from the server.
   * @return {Boolean}
   */
  _hasInitialResponse(challenge) {
    if (challenge === undefined || challenge === null) {
      return false;
    }
    return true;
  }
}

module.exports = SaslMechanismPlain;
