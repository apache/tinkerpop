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

const DriverRemoteConnection = require('./driver-remote-connection');
const Bytecode = require('../process/bytecode');

class Client extends DriverRemoteConnection {
  /**
   * Creates a new instance of DriverRemoteConnection.
   * @param {String} url The resource uri.
   * @param {Object} [options] The connection options.
   * @param {Array} [options.ca] Trusted certificates.
   * @param {String|Array|Buffer} [options.cert] The certificate key.
   * @param {String} [options.mimeType] The mime type to use.
   * @param {String|Buffer} [options.pfx] The private key, certificate, and CA certs.
   * @param {GraphSONReader} [options.reader] The reader to use.
   * @param {Boolean} [options.rejectUnauthorized] Determines whether to verify or not the server certificate.
   * @param {String} [options.traversalSource] The traversal source. Defaults to: 'g'.
   * @param {GraphSONWriter} [options.writer] The writer to use.
   * @param {Authenticator} [options.authenticator] The authentication handler to use.
   * @constructor
   */
  constructor(url, options) {
    super(url, options);
  }

  /** override */
  submit(message, bindings) {
    if (typeof message === 'string' || message instanceof String) {
      const args = {
        'gremlin': message,
        'bindings': bindings,
        'language': 'gremlin-groovy',
        'accept': 'application/json',
        'aliases': { 'g': this.traversalSource }
      };

      return super.submit(null, 'eval', args, null, '');
    }

    if (message instanceof Bytecode) {
      return super.submit(message);
    }
  }
  
}

module.exports = Client;
