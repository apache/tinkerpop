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

const rcModule = require('./remote-connection');
const RemoteConnection = rcModule.RemoteConnection;
const RemoteTraversal = rcModule.RemoteTraversal;
const Client = require('./client');

/**
 * Represents the default {@link RemoteConnection} implementation.
 */
class DriverRemoteConnection extends RemoteConnection {

  /**
   * Creates a new instance of {@link DriverRemoteConnection}.
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
   * @param {Object} [options.headers] An associative array containing the additional header key/values for the initial request.
   * @constructor
   */
  constructor(url, options) {
    super(url);
    this._client = new Client(url, options);
  }

  /** @override */
  open() {
    return this._client.open();
  }

  /** @override */
  submit(bytecode) {
    return this._client.submit(bytecode).then(result => new RemoteTraversal(result.toArray()));
  }

  /** @override */
  close() {
    return this._client.close();
  }

  /** @override */
  addListener(...args) {
    return this._client.addListener(...args);
  }

  /** @override */
  removeListener(...args) {
    return this._client.removeListener(...args);
  }
}

module.exports = DriverRemoteConnection;
