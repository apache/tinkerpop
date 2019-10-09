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

const Connection = require('./connection');
const Bytecode = require('../process/bytecode');

/**
 * A {@link Client} contains methods to send messages to a Gremlin Server.
 */
class Client {

  /**
   * Creates a new instance of {@link Client}.
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
   * @param {String} [options.processor] The name of the opProcessor to use
   * @constructor
   */
  constructor(url, options) {
    this._options = options;
    this._connection = new Connection(url, options);
  }

  /**
   * Opens the underlying connection to the Gremlin Server, if it's not already opened.
   * @returns {Promise}
   */
  open() {
    return this._connection.open();
  }

  /**
   * Send a request to the Gremlin Server, can send a script or bytecode steps.
   * @param {Bytecode|string} message The bytecode or script to send
   * @param {Object} [bindings] The script bindings, if any.
   * @returns {Promise}
   */
  submit(message, bindings) {
    if (typeof message === 'string') {
      const args = {
        'gremlin': message,
        'bindings': bindings,
        'language': 'gremlin-groovy',
        'accept': this._connection.mimeType,
        'aliases': { 'g': this._options.traversalSource || 'g' }
      };

      return this._connection.submit(null, 'eval', args, null, this._options.processor || '');
    }

    if (message instanceof Bytecode) {
      return this._connection.submit(message);
    }
  }

  /**
   * Closes the underlying connection
   * @returns {Promise}
   */
  close() {
    return this._connection.close();
  }

  /**
   * Adds an event listener to the connection
   * @param {String} event The event name that you want to listen to.
   * @param {Function} handler The callback to be called when the event occurs.
   */
  addListener(event, handler) {
    this._connection.on(event, handler)
  }

  /**
   * Removes a previowsly added event listener to the connection
   * @param {String} event The event name that you want to listen to.
   * @param {Function} handler The event handler to be removed.
   */
  removeListener(event, handler) {
    this._connection.removeListener(event, handler)
  }
}

module.exports = Client;
